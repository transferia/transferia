package fgssa

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/operation"
	utiloperation "github.com/transferia/transferia/cloud/dataplatform/connman/pkg/util/grpc/operation"
	utilstatus "github.com/transferia/transferia/cloud/dataplatform/connman/pkg/util/grpc/status"
	"github.com/transferia/transferia/cloud/dataplatform/ycloud"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

type EnsureEnabler interface {
	EnsureEnabled(ctx context.Context, cloudIDs []string) error
}

var (
	_ EnsureEnabler = (*ensureEnabler)(nil)
	_ EnsureEnabler = (*FakeEnsureEnabler)(nil)
)

type FakeEnsureEnabler struct{}

type ensureEnabler struct {
	iamServiceControlSvcClient iam.ServiceControlServiceClient
	iamOperationSvcClient      iam.OperationServiceClient
	serviceID                  string
}

func NewEnsureEnabler(
	iamServiceControlSvcClient iam.ServiceControlServiceClient,
	iamOperationSvcClient iam.OperationServiceClient,
	serviceID string,
) EnsureEnabler {
	return &ensureEnabler{
		iamServiceControlSvcClient: iamServiceControlSvcClient,
		iamOperationSvcClient:      iamOperationSvcClient,
		serviceID:                  serviceID,
	}
}

func (m *FakeEnsureEnabler) EnsureEnabled(ctx context.Context, cloudIDs []string) error {
	return nil
}

func (m *ensureEnabler) EnsureEnabled(ctx context.Context, cloudIDs []string) error {
	return doWorkWithCloudParallel(ctx, func(ctx context.Context, cloudID string) error {
		return m.ensureEnabled(ctx, cloudID)
	}, cloudIDs)
}

func (m *ensureEnabler) ensureEnabled(ctx context.Context, cloudID string) error {
	var op *operation.Operation
	var err error
	if err := backoff.Retry(func() error {
		op, err = m.iamServiceControlSvcClient.EnsureEnabled(ctx, &iam.EnsureServicesEnabledRequest{
			ServiceIds: []string{m.serviceID},
			Resource: &iam.Resource{
				Id:   cloudID,
				Type: ycloud.ResourceCloud,
			},
		})
		if err != nil {
			if !utilstatus.IsRetriable(err) {
				return backoff.Permanent(xerrors.Errorf("cannot ensure enabled: %w", err))
			}
			return xerrors.Errorf("cannot ensure enabled: %w", err)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.WithContext(backoff.NewConstantBackOff(500*time.Millisecond), ctx), 20)); err != nil {
		return xerrors.Errorf("cannot create ensure enabled operation: %w", err)
	}
	return utiloperation.WaitOpDone(ctx, logger.Log, m.iamOperationSvcClient, &iam.GetOperationRequest{OperationId: op.GetId()})
}
