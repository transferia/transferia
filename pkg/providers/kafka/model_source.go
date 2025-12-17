package kafka

import (
	"context"
	"net"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	kafkaConn "github.com/transferia/transferia/pkg/connection/kafka"
	"github.com/transferia/transferia/pkg/parsers"
	"go.uber.org/zap/zapcore"
)

const DefaultAuth = "admin"

type KafkaSource struct {
	Connection  *KafkaConnectionOptions     `log:"true"`
	Auth        *KafkaAuth                  `log:"true"`
	Topic       string                      `log:"true"`
	GroupTopics []string                    `log:"true"`
	Transformer *model.DataTransformOptions `log:"true"`

	// DialFunc can be used to intercept connections made by driver and replace hosts if needed,
	// for instance, in cloud-specific network topology
	DialFunc func(ctx context.Context, network string, address string) (net.Conn, error) `json:"-"`

	BufferSize model.BytesSize `log:"true"` // it's not some real buffer size - see comments to waitLimits() method in kafka-source

	SecurityGroupIDs []string `log:"true"`

	ParserConfig        map[string]interface{} `log:"true"`
	SynchronizeIsNeeded bool                   `log:"true"` // true, if we need to send synchronize events on releasing partitions

	OffsetPolicy          OffsetPolicy `log:"true"` // specify from what topic part start message consumption
	ParseQueueParallelism int          `log:"true"`
}

type OffsetPolicy string

const (
	NoOffsetPolicy      = OffsetPolicy("") // Not specified
	AtStartOffsetPolicy = OffsetPolicy("at_start")
	AtEndOffsetPolicy   = OffsetPolicy("at_end")
)

var _ model.Source = (*KafkaSource)(nil)
var _ model.WithConnectionID = (*KafkaSource)(nil)

func (s *KafkaSource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return logger.MarshalSanitizedObject(s, enc)
}

func (s *KafkaSource) MDBClusterID() string {
	if s.Connection != nil {
		return s.Connection.ClusterID
	}
	return ""
}

func (s *KafkaSource) ServiceAccountIDs() []string {
	if s.Transformer != nil && s.Transformer.ServiceAccountID != "" {
		return []string{s.Transformer.ServiceAccountID}
	}
	return nil
}

func (s *KafkaSource) GetConnectionID() string {
	return s.Connection.ConnectionID
}

func (s *KafkaSource) WithDefaults() {
	if s.Connection == nil {
		s.Connection = &KafkaConnectionOptions{
			ClusterID:      "",
			ConnectionID:   "",
			TLS:            "",
			TLSFile:        "",
			UserEnabledTls: nil,
			Brokers:        nil,
			SubNetworkID:   "",
		}
	}
	if s.Auth == nil {
		s.Auth = &KafkaAuth{
			Enabled:   true,
			Mechanism: kafkaConn.KafkaSaslSecurityMechanism_SCRAM_SHA512,
			User:      "",
			Password:  "",
		}
	}
	if s.Transformer != nil && s.Transformer.CloudFunction == "" {
		s.Transformer = nil
	}
	if s.BufferSize == 0 {
		s.BufferSize = 100 * 1024 * 1024
	}
}

func (KafkaSource) IsSource() {
}

func (s *KafkaSource) GetProviderType() abstract.ProviderType {
	return ProviderType
}

func (s *KafkaSource) Validate() error {
	if s.ParserConfig != nil {
		parserConfigStruct, err := parsers.ParserConfigMapToStruct(s.ParserConfig)
		if err != nil {
			return xerrors.Errorf("unable to create new parser config, err: %w", err)
		}
		return parserConfigStruct.Validate()
	}
	return nil
}

func (s *KafkaSource) IsAppendOnly() bool {
	if s.ParserConfig == nil {
		return false
	} else {
		parserConfigStruct, _ := parsers.ParserConfigMapToStruct(s.ParserConfig)
		if parserConfigStruct == nil {
			return false
		}
		return parserConfigStruct.IsAppendOnly()
	}
}

func (s *KafkaSource) YSRNamespaceID() string {
	if s.ParserConfig == nil {
		return ""
	} else {
		parserConfigStruct, _ := parsers.ParserConfigMapToStruct(s.ParserConfig)
		if parserConfigStruct == nil {
			return ""
		}
		if parserConfigStructYSRable, ok := parserConfigStruct.(parsers.YSRable); ok {
			return parserConfigStructYSRable.YSRNamespaceID()
		}
		return ""
	}
}

func (s *KafkaSource) IsDefaultMirror() bool {
	return s.ParserConfig == nil
}

func (s *KafkaSource) Parser() map[string]interface{} {
	return s.ParserConfig
}

var _ model.HostResolver = (*KafkaSource)(nil)

func (s *KafkaSource) HostsNames() ([]string, error) {
	if s.Connection != nil && s.Connection.ClusterID != "" {
		return nil, nil
	}
	return ResolveOnPremBrokers(s.Connection, s.Auth, s.DialFunc)
}

func (s *KafkaSource) WithConnectionID() error {
	if s.Connection == nil || s.Connection.ConnectionID == "" {
		return nil
	}

	kafkaConnection, err := resolveConnection(s.Connection.ConnectionID)
	if err != nil {
		return xerrors.Errorf("unable to resolve connection: %w", err)
	}
	s.Connection = ResolveConnectionOptions(s.Connection, kafkaConnection)
	s.Auth = ResolveKafkaAuth(s.Auth, kafkaConnection)

	return nil
}
