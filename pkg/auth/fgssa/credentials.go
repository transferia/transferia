package fgssa

import (
	"context"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

type AgentTokenIssuerClient interface {
	CreateAgentToken(resourceID, resourceType string) (string, error)
}

type Credentials interface {
	Token(context.Context) (string, error)
	ExpiresAt() *timestamp.Timestamp // NOTE: only for backward compatibility with yccreds.Credentials
}

type AgentTokenIssuerClientFactory func() (AgentTokenIssuerClient, error)

var (
	_ Credentials = (*agentTokenCredentials)(nil)

	tokenIssuerClientFactory AgentTokenIssuerClientFactory
)

type agentTokenCredentials struct {
	resourceID   string
	resourceType string
}

func NewAgentTokenCredentials(resourceID string, resourceType string) Credentials {
	return &agentTokenCredentials{
		resourceID:   resourceID,
		resourceType: resourceType,
	}
}

func SetAgentTokenIssuerClientFactory(factory AgentTokenIssuerClientFactory) {
	tokenIssuerClientFactory = factory
}

func (a *agentTokenCredentials) Token(ctx context.Context) (string, error) {
	client, err := tokenIssuerClientFactory()
	if err != nil {
		return "", xerrors.Errorf("cannot create control plane client: %w", err)
	}
	return client.CreateAgentToken(a.resourceID, a.resourceType)
}

func (a *agentTokenCredentials) ExpiresAt() *timestamp.Timestamp {
	return nil
}
