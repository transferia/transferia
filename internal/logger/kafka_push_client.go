package logger

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"os"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/transferia/transferia/library/go/core/xerrors"
	zp "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

type KafkaConfig struct {
	Broker      string
	Topic       string
	User        string
	Password    string
	TLSFiles    []string
	TLSInsecure bool
}

type kafkaPusher struct {
	producer *kafka.Writer
}

func (p *kafkaPusher) Write(data []byte) (int, error) {
	msg := kafka.Message{
		Value: copySlice(data),
	}
	if err := p.producer.WriteMessages(context.Background(), msg); err != nil {
		return 0, err
	}
	return len(data), nil
}

func NewKafkaLogger(cfg *KafkaConfig) (*zap.Logger, io.Closer, error) {
	mechanism, err := scram.Mechanism(scram.SHA512, cfg.User, cfg.Password)
	if err != nil {
		return nil, nil, xerrors.Errorf("unable to init scram: %w", err)
	}
	if cfg.User == "" {
		mechanism = nil
	}
	var tlsCfg *tls.Config
	if len(cfg.TLSFiles) > 0 {
		cp, err := x509.SystemCertPool()
		if err != nil {
			return nil, nil, xerrors.Errorf("unable to init cert pool: %w", err)
		}
		for _, cert := range cfg.TLSFiles {
			if !cp.AppendCertsFromPEM([]byte(cert)) {
				return nil, nil, xerrors.Errorf("credentials: failed to append certificates")
			}
		}
		tlsCfg = &tls.Config{
			RootCAs: cp,
		}
	} else if cfg.TLSInsecure {
		tlsCfg = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	pr := &kafka.Writer{
		Addr:      kafka.TCP(cfg.Broker),
		Topic:     cfg.Topic,
		Async:     true,
		Balancer:  &kafka.LeastBytes{},
		Transport: &kafka.Transport{TLS: tlsCfg, SASL: mechanism},
	}

	f := kafkaPusher{
		producer: pr,
	}
	syncLb := zapcore.AddSync(&f)
	stdOut := zapcore.AddSync(os.Stdout)
	defaultPriority := zp.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.InfoLevel
	})
	lbEncoder := zapcore.NewJSONEncoder(zap.JSONConfig(log.InfoLevel).EncoderConfig)
	stdErrCfg := zap.CLIConfig(log.InfoLevel).EncoderConfig
	stdErrCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	stdErrEncoder := zapcore.NewConsoleEncoder(stdErrCfg)
	lbCore := zapcore.NewTee(
		zapcore.NewCore(stdErrEncoder, stdOut, defaultPriority),
		zapcore.NewCore(lbEncoder, zapcore.Lock(syncLb), defaultPriority),
	)
	Log.Info("WriterInit Kafka logger", log.Any("topic", cfg.Topic), log.Any("instance", cfg.Broker))
	return &zap.Logger{
		L: zp.New(
			lbCore,
			zp.AddCaller(),
			zp.AddCallerSkip(1),
			zp.AddStacktrace(zp.WarnLevel),
		),
	}, pr, nil
}
