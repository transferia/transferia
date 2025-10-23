package container

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

type DockerWrapper struct {
	cli    DockerClient
	logger log.Logger
}

func NewDockerWrapper(logger log.Logger) (*DockerWrapper, error) {
	d := &DockerWrapper{
		cli:    nil,
		logger: logger,
	}

	if err := d.ensureDocker(os.Getenv("SUPERVISORD_PATH"), 30*time.Second); err != nil {
		return nil, err
	}

	return d, nil
}

func (d *DockerWrapper) isDockerReady() bool {
	if d.cli == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := d.cli.Ping(ctx)
	if err != nil {
		d.logger.Warnf("Docker is not ready: %v", err)
		return false
	}
	d.logger.Infof("Docker is ready")
	return true
}

func (d *DockerWrapper) Pull(ctx context.Context, image string, opts types.ImagePullOptions) error {
	_, _, err := d.cli.ImageInspectWithRaw(ctx, image)
	if client.IsErrNotFound(err) {
		reader, pullErr := d.cli.ImagePull(ctx, image, types.ImagePullOptions{})
		if pullErr != nil {
			return xerrors.Errorf("error pulling image %s: %w", image, pullErr)
		}
		_, err := io.Copy(os.Stdout, reader)
		if err != nil {
			return err
		}
		defer reader.Close()
	} else if err != nil {
		return xerrors.Errorf("error inspecting image %s: %w", image, err)
	}

	return nil
}

func (d *DockerWrapper) Run(ctx context.Context, opts ContainerOpts) (stdout io.ReadCloser, stderr io.ReadCloser, err error) {
	return d.RunContainer(ctx, opts.ToDockerOpts())
}

func (d *DockerWrapper) RunAndWait(ctx context.Context, opts ContainerOpts) (stdoutBuf *bytes.Buffer, stderrBuf *bytes.Buffer, err error) {
	// 1. Call Run to get the readers
	stdoutReader, stderrReader, err := d.Run(ctx, opts)
	if err != nil {
		return nil, nil, err
	}
	defer stdoutReader.Close()
	defer stderrReader.Close()

	// 2. Create buffers for output
	stdoutBuf = new(bytes.Buffer)
	stderrBuf = new(bytes.Buffer)

	// 3. Use WaitGroup to wait for both streams to be copied
	var wg sync.WaitGroup
	wg.Add(2)

	// 4. Channel for collecting errors
	errCh := make(chan error, 2)

	// Copy stdout
	go func() {
		defer wg.Done()
		_, err := io.Copy(stdoutBuf, stdoutReader)
		if err != nil && err != io.EOF {
			errCh <- xerrors.Errorf("error copying stdout: %w", err)
		}
	}()

	// Copy stderr
	go func() {
		defer wg.Done()
		_, err := io.Copy(stderrBuf, stderrReader)
		if err != nil && err != io.EOF {
			errCh <- xerrors.Errorf("error copying stderr: %w", err)
		}
	}()

	// 5. Wait for both copies to complete
	wg.Wait()
	close(errCh)

	// 6. Collect any errors
	var errs []error
	for e := range errCh {
		errs = append(errs, e)
	}

	// 7. Return combined error if any
	if len(errs) > 0 {
		var combinedErr error
		for _, e := range errs {
			if combinedErr == nil {
				combinedErr = e
			} else {
				combinedErr = xerrors.Errorf("%v; %w", combinedErr, e)
			}
		}
		return stdoutBuf, stderrBuf, combinedErr
	}

	return stdoutBuf, stderrBuf, nil
}

func (d *DockerWrapper) RunContainer(ctx context.Context, opts DockerOpts) (stdout io.ReadCloser, stderr io.ReadCloser, err error) {
	d.logger.Info("Run docker container")
	if d.cli == nil {
		return nil, nil, xerrors.Errorf("docker unavailable")
	}

	if err := d.Pull(ctx, opts.Image, types.ImagePullOptions{}); err != nil {
		return nil, nil, err
	}

	d.logger.Infof("Image %s pulled", opts.Image)

	containerConfig := &container.Config{
		Image:  opts.Image,
		Cmd:    opts.Command,
		Env:    opts.Env,
		Labels: opts.LogOptions,
		Tty:    false,
	}

	hostConfig := &container.HostConfig{
		Mounts:      opts.Mounts,
		AutoRemove:  opts.AutoRemove,
		LogConfig:   container.LogConfig{Type: opts.LogDriver, Config: opts.LogOptions},
		NetworkMode: "host",
	}

	if opts.RestartPolicy.Name != "" {
		hostConfig.RestartPolicy = opts.RestartPolicy
	}

	networkingConfig := &network.NetworkingConfig{}
	if opts.Network != "" {
		networkingConfig.EndpointsConfig = map[string]*network.EndpointSettings{
			opts.Network: {},
		}
	}

	resp, err := d.cli.ContainerCreate(ctx, containerConfig, hostConfig, networkingConfig, nil, opts.ContainerName)
	if err != nil {
		return nil, nil, xerrors.Errorf("error creating container: %w", err)
	}

	d.logger.Infof("container created : %v", resp)

	attachOptions := container.AttachOptions{
		Stream: true,
		Stdout: opts.AttachStdout,
		Stderr: opts.AttachStderr,
	}

	attachResp, err := d.cli.ContainerAttach(ctx, resp.ID, attachOptions)
	if err != nil {
		return nil, nil, xerrors.Errorf("error attaching to container: %w", err)
	}

	d.logger.Info("Success attaching to container")

	// Create pipe readers/writers for stdout and stderr
	stdoutReader, stdoutWriter := io.Pipe()
	stderrReader, stderrWriter := io.Pipe()

	// Start the container (non-blocking)
	if err := d.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		stdoutReader.Close()
		stderrReader.Close()
		stdoutWriter.Close()
		stderrWriter.Close()
		return nil, nil, xerrors.Errorf("error starting container: %w", err)
	}
	d.logger.Info("Docker container started")

	// Launch a goroutine to monitor container completion and manage streams
	go func() {
		defer stdoutWriter.Close()
		defer stderrWriter.Close()

		// Copy from Docker's multiplexed stream to our separate pipes
		copyErrCh := make(chan error, 1)
		go func() {
			_, err := stdcopy.StdCopy(stdoutWriter, stderrWriter, attachResp.Reader)
			copyErrCh <- err
			close(copyErrCh)
		}()

		// Wait for container to exit
		waitCh, errCh := d.cli.ContainerWait(ctx, resp.ID, container.WaitConditionNextExit)

		// Handle container completion or context cancellation
		select {
		case err := <-errCh:
			if err != nil {
				d.logger.Errorf("Error waiting for container: %v", err)
				stdoutWriter.CloseWithError(err)
				stderrWriter.CloseWithError(err)
			}
		case <-waitCh:
			// Container exited normally
			copyErr := <-copyErrCh
			if copyErr != nil {
				d.logger.Errorf("Error copying container output: %v", copyErr)
				stdoutWriter.CloseWithError(copyErr)
				stderrWriter.CloseWithError(copyErr)
			}
		case <-ctx.Done():
			// Context cancelled
			_ = d.cli.ContainerKill(context.Background(), resp.ID, "SIGKILL")
			stdoutWriter.CloseWithError(ctx.Err())
			stderrWriter.CloseWithError(ctx.Err())
		}

		// Clean up
		if attachResp.Conn != nil {
			attachResp.Close()
		}
	}()

	return stdoutReader, stderrReader, nil
}

func (d *DockerWrapper) ensureDocker(supervisorConfigPath string, timeout time.Duration) error {
	if supervisorConfigPath == "" {
		cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
		if err != nil {
			return xerrors.Errorf("unable to init docker cli: %w", err)
		}
		d.cli = cli
		// no supervisor, assume docker is already running.
		if !d.isDockerReady() {
			return xerrors.New("docker is not ready")
		}
		return nil
	}
	// Command to start supervisord
	st := time.Now()
	var stdoutBuf, stderrBuf bytes.Buffer

	// Ensure config path is valid to prevent command injection
	if _, err := os.Stat(supervisorConfigPath); os.IsNotExist(err) {
		return xerrors.Errorf("supervisord config file not found: %s", supervisorConfigPath)
	} else if err != nil {
		return xerrors.Errorf("error checking supervisord config file: %w", err)
	}

	supervisorCmd := exec.Command("supervisord", "-n", "-c", supervisorConfigPath)
	supervisorCmd.Stdout = &stdoutBuf
	supervisorCmd.Stderr = &stderrBuf

	// Start supervisord in a separate goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- supervisorCmd.Run()
		d.logger.Infof("supervisord: output: \n%s", stdoutBuf.String())
		if stderrBuf.Len() > 0 {
			d.logger.Warnf("supervidord: stderr: \n%s", stderrBuf.String())
		}
	}()

	// Wait for dockerd to be ready
	dockerReady := make(chan bool)
	go func() {
		for {
			if d.cli == nil {
				cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
				if err != nil {
					continue
				}
				d.cli = cli
			}

			if d.isDockerReady() {
				close(dockerReady)
				return
			}
			time.Sleep(1 * time.Second)
		}
	}()

	select {
	case <-dockerReady:
		d.logger.Infof("Docker is ready in %v!", time.Since(st))
		return nil
	case err := <-errCh:
		return xerrors.Errorf("supervisord exited unexpectedly: %w", err)
	case <-time.After(timeout):
		return xerrors.Errorf("timeout: %v waiting for Docker to be ready", timeout)
	}
}

// Type returns the container backend type
func (d *DockerWrapper) Type() ContainerBackend {
	return BackendDocker
}
