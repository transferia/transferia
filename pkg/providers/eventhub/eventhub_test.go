//go:build !disable_eventhub_provider

package eventhub_test

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/sas"
	eventhubs "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	eventhub2 "github.com/transferia/transferia/pkg/providers/eventhub"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

const (
	testCasesNum     = 30
	firstEventOffset = "-1"
	runTimeout       = 60 // seconds
)

var namespace, hub, consumerGroup string

func init() {
	namespace = os.Getenv("EVENTHUB_NAMESPACE")
	hub = os.Getenv("EVENTHUB_NAME")
	consumerGroup = os.Getenv("EVENTHUB_CONSUMER_GROUP")
}

type (
	stat struct {
		sent, received uint
	}
	eventhubSender struct {
		hub *eventhubs.Hub
	}
	mockSink struct {
		src map[string]*stat
	}
)

// Manual test based on Azure Eventhub.
// You should create eventhub instance in Azure using Terraform recipes in ./azure/ directory:
//  1. First, you need Azure login and az-cli and terraform
//  2. Login using az cli:
//     $ az login
//  3. Go to directoty with recipes and create eventhub using terraform recipe:
//     $ cd ./azure/
//     $ terraform init
//     $ terraform apply
//  4. Set environment variables for test:
//     $ export EVENTHUB_NAMESPACE=$(terraform output --raw eventhub_namespace)
//     $ export EVENTHUB_NAME=$(terraform output --raw eventhub_name)
//     $ export EVENTHUB_CONSUMER_GROUP=$(terraform output --raw eventhub_consumer_group)
//     $ export EVENTHUB_KEY_NAME=$(terraform output --raw eventhub_key_name)
//     $ export EVENTHUB_KEY_VALUE=$(terraform output --raw eventhub_key_value)
//  5. Go to directory with test and Run test:
//     $ cd ../
//     $ CGO_ENABLED=0 go test ./...
func TestNewSource(t *testing.T) {
	cases := make(map[string]*stat)
	for i := 0; i < testCasesNum; i++ {
		cases[randomString(20)] = &stat{}
	}
	if os.Getenv("EVENTHUB_KEY_NAME") == "" {
		t.Skip()
		return
	}

	transferID, err := uuid.NewUUID()
	require.NoError(t, err)

	cfg := &eventhub2.EventHubSource{
		ConsumerGroup:  consumerGroup,
		NamespaceName:  namespace,
		HubName:        hub,
		StartingOffset: firstEventOffset,
		Auth: &eventhub2.EventHubAuth{
			Method:   eventhub2.EventHubAuthSAS,
			KeyName:  os.Getenv("EVENTHUB_KEY_NAME"),
			KeyValue: model.SecretString(os.Getenv("EVENTHUB_KEY_VALUE")),
		},
	}

	ctx := context.Background()

	logger, err := zap.New(zap.CLIConfig(log.DebugLevel))
	require.NoError(t, err)

	src, err := eventhub2.NewSource(transferID.String(), cfg, logger, metrics.NewRegistry())
	require.NoError(t, err)
	logger.Info("eventhub source was initialized")

	sender, err := newEventhubSender(cfg)
	require.NoError(t, err)
	logger.Info("eventhub sender was initialized")

	t.Run("sender", func(t *testing.T) {
		for data, counters := range cases {
			err = sender.Send(ctx, []byte(data))
			require.NoError(t, err)
			counters.sent += 1
		}
	})
	logger.Infof("%d messages were sent", len(cases))

	timer := time.NewTimer(runTimeout * time.Second)
	go func() {
		for {
			select {
			case <-timer.C:
				logger.Warn("receiving was stopped by timeout")
				src.Stop()
				return
			default:
				continue
			}
		}
	}()

	err = src.Run(newSinker(cases))
	require.NoError(t, err)
	logger.Info("eventhub source was started")

	t.Run("receiver", func(t *testing.T) {
		for data, counter := range cases {
			if counter.sent != counter.received {
				t.Errorf("missed event: \"%s\", sent: %d, received: %d", data, counter.sent, counter.received)
			}
		}
	})
}

func newSinker(src map[string]*stat) *mockSink {
	return &mockSink{src}
}

func (sinker *mockSink) Close() error {
	return nil
}

func (sinker *mockSink) AsyncPush(input []abstract.ChangeItem) chan error {
	for _, in := range input {
		dataColumnIdx := -1
		for idx, columnName := range in.ColumnNames {
			if columnName == "data" {
				dataColumnIdx = idx
				break
			}
		}
		if dataColumnIdx < 0 {
			return util.MakeChanWithError(xerrors.Errorf("there is no \"data\" column in event"))
		}

		if len(in.ColumnValues) < dataColumnIdx {
			return util.MakeChanWithError(xerrors.Errorf("there is no %d'th column in ColumnValues", dataColumnIdx))
		}

		value, ok := in.ColumnValues[dataColumnIdx].(string)
		if !ok {
			return util.MakeChanWithError(xerrors.Errorf("wrong type of interface: %v", in.ColumnValues[dataColumnIdx]))
		}

		counters, ok := sinker.src[value]
		if !ok {
			return util.MakeChanWithError(xerrors.Errorf("unknown message: %s", value))
		}
		counters.received += 1
	}
	return util.MakeChanWithError(nil)
}

func newEventhubSender(cfg *eventhub2.EventHubSource) (*eventhubSender, error) {
	tokenProvider, err := sas.NewTokenProvider(sas.TokenProviderWithKey(cfg.Auth.KeyName, string(cfg.Auth.KeyValue)))
	if err != nil {
		return nil, err
	}

	h, err := eventhubs.NewHub(cfg.NamespaceName, cfg.HubName, tokenProvider)
	if err != nil {
		return nil, err
	}

	return &eventhubSender{h}, nil
}

func (sender *eventhubSender) Send(ctx context.Context, data []byte) error {
	return sender.hub.Send(ctx, eventhubs.NewEvent(data))
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
