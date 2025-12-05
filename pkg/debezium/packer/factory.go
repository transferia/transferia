package packer

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewKeyPackerFromDebeziumParameters(connectorParameters map[string]string, logger log.Logger) (Packer, error) {
	if url := debeziumparameters.GetKeyConverterSchemaRegistryURL(connectorParameters); url != "" {
		caCert := debeziumparameters.GetKeyConverterSslCa(connectorParameters)
		srClient, err := confluent.NewSchemaRegistryClientWithTransport(url, caCert, logger)
		if err != nil {
			return nil, xerrors.Errorf("Unable to create schema registry client: %w", err)
		}
		authData := debeziumparameters.GetKeyConverterSchemaRegistryUserPassword(connectorParameters)
		if authData != "" {
			userAndPassword := strings.SplitN(authData, ":", 2)
			if len(userAndPassword) != 2 {
				return nil, xerrors.Errorf("invalid auth data format. Param %v must be in `user:password` format or empty",
					debeziumparameters.KeyConverterBasicAuthUserInfo)
			}
			srClient.SetCredentials(userAndPassword[0], userAndPassword[1])
		}
		return NewPackerCacheFinalSchema(NewPackerSchemaRegistry(
			srClient,
			debeziumparameters.GetKeySubjectNameStrategy(connectorParameters),
			true,
			debeziumparameters.UseWriteIntoOneFullTopicName(connectorParameters),
			debeziumparameters.GetTopicPrefix(connectorParameters),
			debeziumparameters.GetKeyConverterDTJSONGenerateClosedContentSchema(connectorParameters),
		)), nil
	}
	if debeziumparameters.IsKeySchemaDisabled(connectorParameters) {
		return NewPackerSkipSchema(), nil
	}
	return NewPackerCacheFinalSchema(NewPackerIncludeSchema()), nil
}

func NewValuePackerFromDebeziumParameters(connectorParameters map[string]string, logger log.Logger) (Packer, error) {
	if url := debeziumparameters.GetValueConverterSchemaRegistryURL(connectorParameters); url != "" {
		caCert := debeziumparameters.GetValueConverterSslCa(connectorParameters)
		srClient, err := confluent.NewSchemaRegistryClientWithTransport(url, caCert, logger)
		if err != nil {
			return nil, xerrors.Errorf("Unable to create schema registry client: %w", err)
		}
		authData := debeziumparameters.GetValueConverterSchemaRegistryUserPassword(connectorParameters)
		if authData != "" {
			userAndPassword := strings.SplitN(authData, ":", 2)
			if len(userAndPassword) != 2 {
				return nil, xerrors.Errorf("invalid auth data format. Param %v must be in `user:password` format or empty",
					debeziumparameters.ValueConverterBasicAuthUserInfo)
			}
			srClient.SetCredentials(userAndPassword[0], userAndPassword[1])
		}
		return NewPackerCacheFinalSchema(NewPackerSchemaRegistry(
			srClient,
			debeziumparameters.GetValueSubjectNameStrategy(connectorParameters),
			false,
			debeziumparameters.UseWriteIntoOneFullTopicName(connectorParameters),
			debeziumparameters.GetTopicPrefix(connectorParameters),
			debeziumparameters.GetValueConverterDTJSONGenerateClosedContentSchema(connectorParameters),
		)), nil
	}
	if namespaceID := debeziumparameters.GetYSRNamespaceID(connectorParameters); namespaceID != "" {
		srConnectionParameters, err := confluent.ResolveYSRNamespaceIDToConnectionParams(namespaceID)
		if err != nil {
			return nil, xerrors.Errorf("failed to resolve namespace id: %w", err)
		}
		caCert := debeziumparameters.GetValueConverterSslCa(connectorParameters) // TODO(@kry127)  is it needed?
		srClient, err := confluent.NewSchemaRegistryClientWithTransport(srConnectionParameters.URL, caCert, logger)
		if err != nil {
			return nil, xerrors.Errorf("Unable to create schema registry client: %w", err)
		}
		srClient.SetCredentials(srConnectionParameters.Username, srConnectionParameters.Password)
		return NewPackerCacheFinalSchema(NewPackerSchemaRegistry(
			srClient,
			debeziumparameters.GetValueSubjectNameStrategy(connectorParameters),
			false,
			debeziumparameters.UseWriteIntoOneFullTopicName(connectorParameters),
			debeziumparameters.GetTopicPrefix(connectorParameters),
			debeziumparameters.GetValueConverterDTJSONGenerateClosedContentSchema(connectorParameters),
		)), nil
	}

	if debeziumparameters.IsValueSchemaDisabled(connectorParameters) {
		return NewPackerSkipSchema(), nil
	}
	return NewPackerCacheFinalSchema(NewPackerIncludeSchema()), nil
}
