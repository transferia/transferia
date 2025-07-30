package main

import (
	"context"
	"net/http"
	"os"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/transferia/transferia/cmd/trcli/activate"
	"github.com/transferia/transferia/cmd/trcli/check"
	"github.com/transferia/transferia/cmd/trcli/describe"
	"github.com/transferia/transferia/cmd/trcli/replicate"
	"github.com/transferia/transferia/cmd/trcli/upload"
	"github.com/transferia/transferia/cmd/trcli/validate"
	"github.com/transferia/transferia/internal/logger"
	internal_metrics "github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	coordinator "github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/cobraaux"
	"github.com/transferia/transferia/pkg/coordinator/etcdcoordinator"
	"github.com/transferia/transferia/pkg/coordinator/s3coordinator"
	_ "github.com/transferia/transferia/pkg/dataplane"
	"github.com/transferia/transferia/pkg/serverutil"
	zp "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

var (
	defaultLogLevel    = "debug"
	defaultLogConfig   = "console"
	defaultCoordinator = "memory"
)

func main() {
	var rt abstract.LocalRuntime
	var cp coordinator.Coordinator = coordinator.NewStatefulFakeClient()

	loggerConfig := newLoggerConfig()
	logger.Log = zap.Must(loggerConfig)
	hcPort := 0
	logLevel := defaultLogLevel
	logConfig := defaultLogConfig
	coordinatorTyp := defaultCoordinator
	coordinatorS3Bucket := ""
	coordinatorEtcdEndpoints := []string{}
	coordinatorEtcdUsername := ""
	coordinatorEtcdPassword := ""
	coordinatorEtcdCertFile := ""
	coordinatorEtcdKeyFile := ""
	coordinatorEtcdCAFile := ""
	runProfiler := false

	promRegistry, registry := internal_metrics.NewPrometheusRegistryWithNameProcessor()

	rootCommand := &cobra.Command{
		Use:          "trcli",
		Short:        "Transferia cli",
		Example:      "./trcli help",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			cmd.SetContext(ctx)

			if strings.Contains(cmd.CommandPath(), "describe") {
				return nil
			}
			if runProfiler {
				go serverutil.RunPprof()
			}

			go func() {
				rootMux := http.NewServeMux()
				rootMux.Handle("/metrics", promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{
					ErrorHandling: promhttp.PanicOnError,
				}))
				logger.Log.Infof("Prometheus is uprising on port %v", "9091")
				if err := http.ListenAndServe(":9091", rootMux); err != nil {
					logger.Log.Error("failed to serve metrics", log.Error(err))
				}
			}()

			switch strings.ToLower(logConfig) {
			case "json":
				loggerConfig = zp.NewProductionConfig()
			case "minimal":
				loggerConfig.EncoderConfig = zapcore.EncoderConfig{
					MessageKey: "message",
					LevelKey:   "level",
					// Disable the rest of the fields
					TimeKey:        "",
					NameKey:        "",
					CallerKey:      "",
					FunctionKey:    "",
					StacktraceKey:  "",
					LineEnding:     zapcore.DefaultLineEnding,
					EncodeLevel:    zapcore.CapitalColorLevelEncoder,
					EncodeName:     nil,
					EncodeDuration: nil,
				}
			}
			switch strings.ToLower(logLevel) {
			case "panic":
				loggerConfig.Level.SetLevel(zapcore.PanicLevel)
			case "fatal":
				loggerConfig.Level.SetLevel(zapcore.FatalLevel)
			case "error":
				loggerConfig.Level.SetLevel(zapcore.ErrorLevel)
			case "warning":
				loggerConfig.Level.SetLevel(zapcore.WarnLevel)
			case "info":
				loggerConfig.Level.SetLevel(zapcore.InfoLevel)
			case "debug":
				loggerConfig.Level.SetLevel(zapcore.DebugLevel)
			default:
				return xerrors.Errorf("unsupported value \"%s\" for --log-level", logLevel)
			}

			logger.Log = zap.Must(loggerConfig)

			switch coordinatorTyp {
			case defaultCoordinator:
				cp = coordinator.NewStatefulFakeClient()
				if rt.CurrentJob > 0 || rt.ShardingUpload.JobCount > 1 {
					return xerrors.Errorf("for sharding upload memory coordinator won't work")
				}
			case "etcd":
				var err error
				cp, err = etcdcoordinator.NewEtcdCoordinator(cmd.Context(), etcdcoordinator.EtcdConfig{
					Endpoints: coordinatorEtcdEndpoints,
					Username:  coordinatorEtcdUsername,
					Password:  coordinatorEtcdPassword,
					CertFile:  coordinatorEtcdCertFile,
					KeyFile:   coordinatorEtcdKeyFile,
					CAFile:    coordinatorEtcdCAFile,
				}, logger.Log)
				if err != nil {
					return xerrors.Errorf("unable to load etcd coordinator: %w", err)
				}
			case "s3":
				var err error
				cp, err = s3coordinator.NewS3(coordinatorS3Bucket, logger.Log)
				if err != nil {
					return xerrors.Errorf("unable to load s3 coordinator: %w", err)
				}
			}

			go serverutil.RunHealthCheckOnPort(hcPort)
			return nil
		},
	}

	cobraaux.RegisterCommand(rootCommand, activate.ActivateCommand(&cp, &rt, registry))
	cobraaux.RegisterCommand(rootCommand, check.CheckCommand())
	cobraaux.RegisterCommand(rootCommand, replicate.ReplicateCommand(&cp, &rt, registry))
	cobraaux.RegisterCommand(rootCommand, upload.UploadCommand(&cp, &rt, registry))
	cobraaux.RegisterCommand(rootCommand, validate.ValidateCommand())
	cobraaux.RegisterCommand(rootCommand, describe.DescribeCommand())

	rootCommand.PersistentFlags().StringVar(&logLevel, "log-level", defaultLogLevel, "Specifies logging level for output logs (\"panic\", \"fatal\", \"error\", \"warning\", \"info\", \"debug\")")
	rootCommand.PersistentFlags().StringVar(&logConfig, "log-config", defaultLogConfig, "Specifies logging config for output logs (\"console\", \"json\", \"minimal\")")

	rootCommand.PersistentFlags().StringVar(&coordinatorTyp, "coordinator", defaultCoordinator, "Specifies how to coordinate transfer nodes (\"memory\", \"s3\", \"etcd\")")
	rootCommand.PersistentFlags().StringVar(&coordinatorS3Bucket, "coordinator-s3-bucket", "", "Bucket for s3 coordinator")
	rootCommand.PersistentFlags().StringSliceVar(&coordinatorEtcdEndpoints, "coordinator-etcd-endpoints", []string{"http://localhost:2379"}, "Endpoints for etcd coordinator")
	rootCommand.PersistentFlags().StringVar(&coordinatorEtcdUsername, "coordinator-etcd-username", "", "Username for etcd coordinator")
	rootCommand.PersistentFlags().StringVar(&coordinatorEtcdPassword, "coordinator-etcd-password", "", "Password for etcd coordinator")
	rootCommand.PersistentFlags().StringVar(&coordinatorEtcdCertFile, "coordinator-etcd-cert-file", "", "Path to the etcd client certificate file")
	rootCommand.PersistentFlags().StringVar(&coordinatorEtcdKeyFile, "coordinator-etcd-key-file", "", "Path to the etcd client key file")
	rootCommand.PersistentFlags().StringVar(&coordinatorEtcdCAFile, "coordinator-etcd-ca-file", "", "Path to the etcd CA certificate file")

	rootCommand.PersistentFlags().BoolVar(&runProfiler, "run-profiler", true, "Run go pprof for performance profiles on 8080 port")
	rootCommand.PersistentFlags().IntVar(&rt.CurrentJob, "coordinator-job-index", 0, "Worker job index")
	rootCommand.PersistentFlags().IntVar(&rt.ShardingUpload.JobCount, "coordinator-job-count", 0, "Worker job count, if more then 1 - run consider as sharded, coordinator is required to be non memory")
	rootCommand.PersistentFlags().IntVar(&rt.ShardingUpload.ProcessCount, "coordinator-process-count", 1, "Worker process count, how many readers must be opened for each job")
	rootCommand.PersistentFlags().IntVar(&hcPort, "health-check-port", 3000, "Port to used as health-check API")

	err := rootCommand.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func newLoggerConfig() zp.Config {
	cfg := logger.DefaultLoggerConfig(zapcore.DebugLevel)
	cfg.OutputPaths = []string{"stdout"}
	cfg.ErrorOutputPaths = []string{"stderr"}
	return cfg
}
