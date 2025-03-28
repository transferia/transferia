package recipe

import (
	"context"
	"github.com/nats-io/nats.go"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/nats/source"
	tc_nats "github.com/transferia/transferia/tests/tcrecipes/nats"
	"os"
)

const (
	natsTestUrl = "NATS_TEST_URL"
)

type ContainerParams struct {
	streamConfigs []*nats.StreamConfig
}

type Option func(c *ContainerParams)

func WithStreams(streamConfigs []*nats.StreamConfig) Option {
	return func(c *ContainerParams) {
		c.streamConfigs = streamConfigs
	}
}

func ContainerNeeded() bool {
	return os.Getenv("USE_TESTCONTAINERS") == "1"
}

func SourceRecipe(opts ...Option) (*source.NatsSource, error) {
	src := new(source.NatsSource)
	src.WithDefaults()
	if ContainerNeeded() {
		if err := StartNatsContainer(opts...); err != nil {
			return nil, xerrors.Errorf("unable to setup nats container: %w", err)
		}
		src.Config.Connection.NatsConnectionOptions.Url = os.Getenv(natsTestUrl)
	}
	return src, nil
}

func MustSource(opts ...Option) *source.NatsSource {
	result, err := SourceRecipe(opts...)
	if err != nil {
		panic(err)
	}
	return result
}

func StartNatsContainer(opts ...Option) error {
	params := &ContainerParams{}
	for _, opt := range opts {
		opt(params)
	}
	_, err := tc_nats.RunContainer(context.Background(), tc_nats.WithStreams(params.streamConfigs))
	if err != nil {
		return xerrors.Errorf("unable to start nats container: %w", err)
	}
	return nil
}
