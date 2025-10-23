package dbt

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/mount"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/container"
	"github.com/transferia/transferia/pkg/runtime/shared/pod"
	"go.ytsaurus.tech/library/go/core/log"
	"gopkg.in/yaml.v3"
	v1 "k8s.io/api/core/v1"
)

type runner struct {
	dst SupportedDestination
	cfg *Config

	transfer *model.Transfer

	cw container.ContainerImpl
}

func newRunner(dst SupportedDestination, cfg *Config, transfer *model.Transfer) (*runner, error) {
	containerImpl, err := container.NewContainerImpl(logger.Log)
	if err != nil {
		return nil, err
	}

	return &runner{
		dst: dst,
		cfg: cfg,

		transfer: transfer,

		cw: containerImpl,
	}, nil
}

func (r *runner) Run(ctx context.Context) error {
	r.cleanupConfiguration()
	if err := r.initializeDocker(ctx); err != nil {
		return xerrors.Errorf("failed to initialize docker for DBT: %w", err)
	}
	if err := r.initializeConfiguration(ctx); err != nil {
		return xerrors.Errorf("failed to initialize DBT configuration files: %w", err)
	}
	defer r.cleanupConfiguration()
	if err := r.run(ctx); err != nil {
		return xerrors.Errorf("failed to run DBT: %w", err)
	}
	return nil
}

func (r *runner) initializeDocker(ctx context.Context) error {
	if err := r.cw.Pull(ctx, r.fullImageID(), types.ImagePullOptions{}); err != nil {
		return xerrors.Errorf("docker initialization failed: %w", err)
	}
	return nil
}

func (r *runner) fullImageID() string {
	return fmt.Sprintf("%s:%s", dockerRegistryID(), r.dockerImageTag())
}

func dockerRegistryID() string {
	return fmt.Sprintf("%s/%s", os.Getenv("DBT_CONTAINER_REGISTRY"), `data-transfer-dbt`)
}

func (r *runner) dockerImageTag() string {
	// the tag can be made customizable in the DBT common configuration, r.g. to control the DBT version
	// the tag is currently produced by a manual push of the image with the use of the `datacloud/Makefile` `release-dbt-...` targets
	return os.Getenv("DBT_IMAGE_TAG")
}

func (r *runner) initializeConfiguration(ctx context.Context) error {
	if err := os.MkdirAll(dataDirectory(), (os.ModeDir | 0o700)); err != nil {
		return xerrors.Errorf("failed to create the DBT data directory %q: %w", dataDirectory(), err)
	}

	destinationConfiguration, err := r.dst.DBTConfiguration(ctx)
	if err != nil {
		return xerrors.Errorf("failed to compose a DBT configuration of the destination database: %w", err)
	}
	marshalledDestinationConfiguration, err := yaml.Marshal(
		map[string]any{
			r.cfg.ProfileName: map[string]any{
				"target": "dev",
				"outputs": map[string]any{
					"dev": destinationConfiguration,
				},
			},
		},
	)
	if err != nil {
		return xerrors.Errorf("failed to marshal the DBT configuration of the destination database into YAML: %w", err)
	}
	if err := os.WriteFile(pathProfiles(), marshalledDestinationConfiguration, 0o644); err != nil {
		return xerrors.Errorf("failed to write the profile file to '%s': %w", pathProfiles(), err)
	}

	outBuf := new(bytes.Buffer)
	opts := r.gitCloneCommands()
	opts.Progress = outBuf

	if _, err := git.PlainClone(pathProject(), false, opts); err != nil {
		return xerrors.Errorf("failed to clone a remote repository: %w", err)
	}
	logger.Log.Info(fmt.Sprintf("successfully executed `git clone %s %s`",
		r.cfg.GitRepositoryLink, pathProject()), log.String("stdout", outBuf.String()))

	return nil
}

func dataDirectory() string {
	return fmt.Sprintf("%s/%s", GlobalDataDirectory(), "dbt_data")
}

func GlobalDataDirectory() string {
	if baseDir, ok := os.LookupEnv("BASE_DIR"); ok {
		return baseDir
	}
	return pod.SharedDir
}

func pathProfiles() string {
	return fmt.Sprintf("%s/%s", dataDirectory(), "profiles.yml")
}

func pathProject() string {
	return fmt.Sprintf("%s/%s", dataDirectory(), "project")
}

func (r *runner) gitCloneCommands() *git.CloneOptions {
	opts := &git.CloneOptions{
		URL:   r.cfg.GitRepositoryLink,
		Depth: 1,
	}

	if branch := r.cfg.GitBranch; len(branch) > 0 {
		opts.ReferenceName = plumbing.NewBranchReferenceName(branch)
		opts.SingleBranch = true
	}

	return opts
}

func (r *runner) cleanupConfiguration() {
	if err := os.RemoveAll(pathProject()); err != nil {
		logger.Log.Warn("DBT project cleanup failed", log.Error(err))
	}
	if err := os.Remove(pathProfiles()); err != nil {
		logger.Log.Warn("DBT profiles cleanup failed", log.Error(err))
	}
}

func (r *runner) run(ctx context.Context) error {
	opts := container.ContainerOpts{
		Env: map[string]string{
			"AWS_EC2_METADATA_DISABLED": "true",
		},
		LogOptions: map[string]string{
			"max-size": "100m",
			"max-file": "3",
		},
		Namespace:     "",
		RestartPolicy: v1.RestartPolicyNever,
		PodName:       "dbt-runner",
		Image:         r.fullImageID(),
		LogDriver:     "local",
		Network:       "host",
		ContainerName: "runner",
		Volumes: []container.Volume{
			{
				Name:          "project",
				VolumeType:    string(mount.TypeBind),
				HostPath:      pathProject(),
				ContainerPath: "/usr/app",
			},
			{
				Name:          "profiles",
				VolumeType:    string(mount.TypeBind),
				HostPath:      pathProfiles(),
				ContainerPath: "/root/.dbt/profiles.yml",
			},
		},
		Command: nil,
		Args: []string{
			r.cfg.Operation,
		},
		// FIXME: make this configurable
		Timeout:      12 * time.Hour,
		AttachStdout: true,
		AttachStderr: true,
		AutoRemove:   true,
	}

	stdoutReader, stderrReader, err := r.cw.Run(ctx, opts)
	if err != nil {
		return xerrors.Errorf("docker run failed: %w", err)
	}
	defer stdoutReader.Close()
	if stderrReader != nil {
		defer stderrReader.Close()
	}

	// Use WaitGroup to read both streams concurrently until EOF
	var wg sync.WaitGroup

	// Channel for collecting errors
	errCh := make(chan error, 2)

	// Read stdout line by line
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutReader)
		for scanner.Scan() {
			// Log each line as it's read
			logger.Log.Info("stdout: " + scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			errCh <- xerrors.Errorf("error scanning stdout: %w", err)
		}
	}()

	if stderrReader != nil {
		wg.Add(1)
		// Read stderr line by line
		go func() {
			defer wg.Done()
			scanner := bufio.NewScanner(stderrReader)
			for scanner.Scan() {
				// Log each line as it's read
				logger.Log.Warn("stderr: " + scanner.Text())
			}
			if err := scanner.Err(); err != nil {
				errCh <- xerrors.Errorf("error scanning stderr: %w", err)
			}
		}()
	}

	// Wait for both streams to be fully read
	wg.Wait()
	close(errCh)

	// Collect any errors
	var errs []error
	for e := range errCh {
		errs = append(errs, e)
	}

	// Return combined error if any
	if len(errs) > 0 {
		var combinedErr error
		for _, e := range errs {
			if combinedErr == nil {
				combinedErr = e
			} else {
				combinedErr = xerrors.Errorf("%v; %w", combinedErr, e)
			}
		}
		return combinedErr
	}

	return nil
}
