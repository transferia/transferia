package container

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
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

func (d *DockerWrapper) Run(ctx context.Context, opts ContainerOpts) (stdout io.Reader, stderr io.Reader, err error) {
	return d.RunContainer(ctx, opts.ToDockerOpts())
}

func (d *DockerWrapper) RunContainer(ctx context.Context, opts DockerOpts) (stdout io.Reader, stderr io.Reader, err error) {
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

	waitCh, errCh := d.cli.ContainerWait(ctx, resp.ID, container.WaitConditionNextExit)

	if err := d.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return nil, nil, xerrors.Errorf("error starting container: %w", err)
	}
	d.logger.Info("Docker container started")

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

	// TODO: don't hold stdoutBuf and stderrBuf in memory
	stdoutBuf := new(bytes.Buffer)
	stderrBuf := new(bytes.Buffer)

	copyCh := make(chan error)

	go func() {
		defer close(copyCh)
		if attachResp.Conn != nil {
			defer attachResp.Close()
		}
		if _, err := stdcopy.StdCopy(stdoutBuf, stderrBuf, attachResp.Reader); err != nil {
			copyCh <- err
		}
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return nil, nil, xerrors.Errorf("error waiting for container: %w", err)
		}
	case <-waitCh:
		select {
		case err := <-errCh:
			if err != nil {
				return nil, nil, xerrors.Errorf("error waiting for container: %w", err)
			}
		default:
		}
	case <-ctx.Done():
		_ = d.cli.ContainerKill(ctx, resp.ID, "SIGKILL")
		return nil, nil, xerrors.Errorf("error waiting for container: %w", ctx.Err())
	}

	if err := <-copyCh; err != nil {
		return nil, nil, xerrors.Errorf("error copying container output: %w", err)
	}

	return stdoutBuf, stderrBuf, nil
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
