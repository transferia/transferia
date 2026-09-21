package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	provider_kafka "github.com/transferia/transferia/pkg/providers/kafka"
	kafka_writer "github.com/transferia/transferia/pkg/providers/kafka/writer"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	serializer "github.com/transferia/transferia/pkg/serializer/queue"
	transformer_filter "github.com/transferia/transferia/pkg/transformer/registry/filter"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/transfer"
	"go.uber.org/mock/gomock"
)

var (
	Source = provider_postgres.PgSource{
		Hosts:    []string{"localhost"},
		User:     os.Getenv("PG_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database: os.Getenv("PG_LOCAL_DATABASE"),
		Port:     testenv.GetIntFromEnv("PG_LOCAL_PORT"),
	}
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

var key string

func callbackFunc(_, _, _ interface{}, msgs ...interface{}) error {
	for _, el := range msgs {
		switch v := el.(type) {
		case []serializer.SerializedMessage:
			for _, vv := range v {
				fmt.Println("QQQ::KEY", string(vv.Key))
				fmt.Println("QQQ::VAL", string(vv.Value))
				key = string(vv.Key)
			}
		}
	}
	return nil
}

//---------------------------------------------------------------------------------------------------------------------

func TestReplication(t *testing.T) {
	defer require.NoError(t, network.CheckConnections(
		network.LabeledPort{Label: "PG source", Port: Source.Port},
	))

	//------------------------------------------------------------------------------
	// start replication

	ctx := context.Background()

	dst := &provider_kafka.KafkaDestination{
		Connection: &provider_kafka.KafkaConnectionOptions{
			TLS:     model.DefaultTLS,
			Brokers: []string{"my_broker_0"},
		},
		Auth: &provider_kafka.KafkaAuth{
			Enabled:   true,
			Mechanism: "SHA-512",
			User:      "user1",
			Password:  "qwert12345",
		},
		TopicPrefix: "fullfillment",
		FormatSettings: model.SerializationFormat{
			Name: model.SerializationFormatDebezium,
			Settings: map[string]string{
				debezium_parameters.DatabaseDBName:   "pguser",
				debezium_parameters.AddOriginalTypes: "false",
				debezium_parameters.SourceType:       "pg",
			},
		},
		ParralelWriterCount: 10,
		// declare 'FormatSettings' explicitly, bcs here are not honest kafka-target,
		// and here are we forced to create transfer after creating sink - in real workflow,
		// FillDependentFields() called earlier than creating sink
		AddSystemTables: false,
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	currWriter := kafka_writer.NewMockAbstractWriter(ctrl)
	currWriter.EXPECT().WriteMessages(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Do(callbackFunc)
	currWriter.EXPECT().Close().AnyTimes()

	factory := kafka_writer.NewMockAbstractWriterFactory(ctrl)
	factory.EXPECT().BuildWriter([]string{"my_broker_0"}, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(currWriter)

	sink, err := provider_kafka.NewSinkImpl(
		dst,
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
		factory,
		false,
	)
	require.NoError(t, err)

	target := model.MockDestination{SinkerFactory: func() abstract.Sinker { return sink }}
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &target, abstract.TransferTypeIncrementOnly) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &target, abstract.TransferTypeIncrementOnly)

	transformer, err := transformer_filter.NewFilterColumnsTransformer(transformer_filter.FilterColumnsConfig{
		Columns: transformer_filter.Columns{
			ExcludeColumns: []string{
				"file_data",
				"processed_file_data",
			},
		},
	}, logger.Log)
	require.NoError(t, err)
	require.NoError(t, transfer.AddExtraTransformer(transformer))

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	//-----------------------------------------------------------------------------------------------------------------
	// execute SQL statements

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	query := `
		INSERT INTO certificate.certificate_template (
			certificate_event_id,
			certificate_type_id,
			file_content_type,
			file_data,
			polls_link,
			need_email_sending,
			created_at,
			updated_at,
			created_by_id,
			updated_by_id,
			created_by_user_name,
			updated_by_user_name,
			processed_file_content_type,
			processed_file_data,
			is_pre_made,
			is_additional_data_visible,
			is_polls_required
		) VALUES (
			'11111111-1111-1111-1111-111111111111', -- certificate_event_id
			'22222222-2222-2222-2222-222222222222', -- certificate_type_id
			'application/pdf',                      -- file_content_type
			'\x255044462d312e350a',                 -- file_data (пример hex для PDF)
			'https://example.com/poll',             -- polls_link
			TRUE,                                  -- need_email_sending
			NOW(),                                 -- created_at
			NOW(),                                 -- updated_at
			'33333333-3333-3333-3333-333333333333', -- created_by_id
			'44444444-4444-4444-4444-444444444444', -- updated_by_id
			'admin',                               -- created_by_user_name
			'admin',                               -- updated_by_user_name
			'application/pdf',                     -- processed_file_content_type
			'\x255044462d312e350a',                -- processed_file_data
			FALSE,                                 -- is_pre_made
			TRUE,                                  -- is_additional_data_visible
			TRUE                                   -- is_polls_required
		);
`
	_, err = srcConn.Exec(ctx, query)
	require.NoError(t, err)

	//-----------------------------------------------------------------------------------------------------------------

	for {
		if len(key) != 0 {
			break
		}
		time.Sleep(time.Second)
	}

	require.True(t, strings.HasPrefix(key, `{"payload":{"certificate_event_id":"11111111-1111-1111-1111-111111111111","certificate_type_id":"22222222-2222-2222-2222-222222222222"}`))
}
