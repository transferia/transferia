package oraclerecipe

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/docker/docker/api/types/container"
	testcontainers_go "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/tests/tcrecipes"
)

const (
	DefaultImage    = "gvenzl/oracle-free:latest"
	DefaultUser     = "system"
	DefaultPassword = "test_oracle_pass"
	DefaultService  = "FREEPDB1"
	oraclePort      = "1521/tcp"
)

// OracleContainer wraps the generic testcontainer.
type OracleContainer struct {
	testcontainers_go.Container
}

// PrepareContainer is idempotent: no-op if RECIPE_ORACLE_HOST already set (set by Arcadia
// recipe process or a previous call). When USE_TESTCONTAINERS=1 it starts the container.
func PrepareContainer(ctx context.Context) {
	if _, ok := os.LookupEnv("RECIPE_ORACLE_HOST"); ok {
		return
	}
	if tcrecipes.Enabled() {
		if _, err := Run(ctx, DefaultImage); err != nil {
			panic(err)
		}
	}
}

// Run starts an Oracle container and exports RECIPE_ORACLE_* env vars.
// It does NOT automatically execute any init SQL — mount/volume approaches are unreliable
// in the Arcadia test environment because the test binary's working directory is inside the
// build root, not the source tree. Use ExecSQL after calling PrepareContainer instead.
func Run(ctx context.Context, img string, opts ...testcontainers_go.ContainerCustomizer) (*OracleContainer, error) {
	req := testcontainers_go.GenericContainerRequest{
		ContainerRequest: testcontainers_go.ContainerRequest{
			Image:        img,
			ExposedPorts: []string{oraclePort},
			Env: map[string]string{
				"ORACLE_PASSWORD": DefaultPassword,
			},
			WaitingFor: wait.ForLog("DATABASE IS READY TO USE!").
				WithStartupTimeout(10 * time.Minute),
			ShmSize: 512 * 1024 * 1024, // Oracle SGA requires large /dev/shm; 512MB is sufficient for Free edition
			HostConfigModifier: func(hostConfig *container.HostConfig) {
				// VKTM raises its scheduling priority during startup. Without this
				// capability Oracle can abort before opening the database (ORA-00445).
				hostConfig.CapAdd = append(hostConfig.CapAdd, "SYS_NICE")
			},
		},
		Started: true,
	}

	for _, opt := range opts {
		if err := opt.Customize(&req); err != nil {
			return nil, xerrors.Errorf("customize container: %w", err)
		}
	}

	container, err := testcontainers_go.GenericContainer(ctx, req)
	if err != nil {
		// GenericContainer returns the created container even when startup fails.
		// Preserve its diagnostics before the recipe removes the Podman store.
		if container != nil {
			cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			containerState, stateError := container.State(cleanupContext)
			if stateError == nil {
				err = xerrors.Errorf("%w; Oracle container exit code=%d, OOMKilled=%t", err, containerState.ExitCode, containerState.OOMKilled)
			}
			logs, logsError := container.Logs(cleanupContext)
			if logsError == nil {
				output, readError := io.ReadAll(io.LimitReader(logs, 64*1024))
				_ = logs.Close()
				if readError == nil {
					err = xerrors.Errorf("%w; Oracle startup log: %s", err, output)
				}
			}
			alertLog, alertError := container.CopyFileFromContainer(cleanupContext, "/opt/oracle/diag/rdbms/free/FREE/trace/alert_FREE.log")
			if alertError == nil {
				output, readError := io.ReadAll(io.LimitReader(alertLog, 256*1024))
				_ = alertLog.Close()
				if readError == nil {
					err = xerrors.Errorf("%w; Oracle alert log: %s", err, output)
				}
			}
			// Diagnostics may use up their timeout. Give removal its own timeout.
			terminateContext, terminateCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer terminateCancel()
			if terminateError := container.Terminate(terminateContext); terminateError != nil {
				err = xerrors.Errorf("%w; terminate Oracle container: %w", err, terminateError)
			}
		}
		return nil, xerrors.Errorf("start oracle container: %w", err)
	}

	mapped, err := container.MappedPort(ctx, oraclePort)
	if err != nil {
		return nil, xerrors.Errorf("get oracle port: %w", err)
	}

	envVars := map[string]string{
		"RECIPE_ORACLE_HOST":     "localhost",
		"RECIPE_ORACLE_PORT":     mapped.Port(),
		"RECIPE_ORACLE_USER":     DefaultUser,
		"RECIPE_ORACLE_PASSWORD": DefaultPassword,
		"RECIPE_ORACLE_SERVICE":  DefaultService,
	}
	for k, v := range envVars {
		if err := os.Setenv(k, v); err != nil {
			return nil, xerrors.Errorf("setenv %s: %w", k, err)
		}
	}

	return &OracleContainer{Container: container}, nil
}
