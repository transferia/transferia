package fgssa

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/operation"
	"github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/reference"
	utiloperation "github.com/transferia/transferia/cloud/dataplatform/connman/pkg/util/grpc/operation"
	utilstatus "github.com/transferia/transferia/cloud/dataplatform/connman/pkg/util/grpc/status"
	"github.com/transferia/transferia/cloud/dataplatform/ycloud"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServiceReferencesManager interface {
	AddServiceReferences(ctx context.Context, cloudIDs []string, resourceID, resourceType string) error
	DeleteServiceReferences(ctx context.Context, cloudIDs []string, resourceID, resourceType string) error
}

var (
	_ ServiceReferencesManager = (*serviceReferencesManager)(nil)
	_ ServiceReferencesManager = (*FakeServiceReferencesManager)(nil)
)

type FakeServiceReferencesManager struct{}

type serviceReferencesManager struct {
	iamServiceControlSvcClient iam.ServiceControlServiceClient
	iamOperationSvcClient      iam.OperationServiceClient
	serviceID                  string
}

func NewServiceReferencesManager(
	iamServiceControlSvcClient iam.ServiceControlServiceClient,
	iamOperationSvcClient iam.OperationServiceClient,
	serviceID string,
) ServiceReferencesManager {
	return &serviceReferencesManager{
		iamServiceControlSvcClient: iamServiceControlSvcClient,
		iamOperationSvcClient:      iamOperationSvcClient,
		serviceID:                  serviceID,
	}
}

func (m *FakeServiceReferencesManager) AddServiceReferences(ctx context.Context, cloudIDs []string, resourceID, resourceType string) error {
	return nil
}

func (m *FakeServiceReferencesManager) DeleteServiceReferences(ctx context.Context, cloudIDs []string, resourceID, resourceType string) error {
	return nil
}

func (m *serviceReferencesManager) AddServiceReferences(ctx context.Context, cloudIDs []string, resourceID, resourceType string) error {
	return doWorkWithCloudParallel(ctx, func(ctx context.Context, cloudID string) error {
		return m.addServiceReference(ctx, cloudID, resourceID, resourceType)
	}, cloudIDs)
}

func (m *serviceReferencesManager) DeleteServiceReferences(ctx context.Context, cloudIDs []string, resourceID, resourceType string) error {
	return doWorkWithCloudParallel(ctx, func(ctx context.Context, cloudID string) error {
		return m.deleteServiceReference(ctx, cloudID, resourceID, resourceType)
	}, cloudIDs)
}

func (m *serviceReferencesManager) addServiceReference(ctx context.Context, cloudID, resourceID, resourceType string) error {
	var op *operation.Operation
	var err error
	if err := backoff.Retry(func() error {
		op, err = m.iamServiceControlSvcClient.UpdateReferences(ctx, &iam.UpdateServiceReferencesRequest{
			ServiceId: m.serviceID,
			Resource: &iam.Resource{
				Id:   cloudID,
				Type: ycloud.ResourceCloud,
			},
			ReferenceAdditions: []*reference.Reference{
				{
					Referrer: &reference.Referrer{
						Type: resourceType,
						Id:   resourceID,
					},
					Type: reference.Reference_USED_BY,
				},
			},
			ReferenceDeletions: nil,
		})
		if err != nil {
			if status.Code(err) == codes.AlreadyExists {
				logger.Log.Info(
					"service reference already exist",
					log.String("resource id", resourceID),
					log.String("resource type", resourceType),
					log.String("cloud id", cloudID),
				)
				return nil
			}
			if !utilstatus.IsRetriable(err) {
				return backoff.Permanent(xerrors.Errorf("cannot add service reference: %w", err))
			}
			return xerrors.Errorf("cannot add service reference: %w", err)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.WithContext(backoff.NewConstantBackOff(500*time.Millisecond), ctx), 20)); err != nil {
		return xerrors.Errorf("cannot create add service reference operation: %w", err)
	}
	if op == nil { // already exist case
		return nil
	}
	return utiloperation.WaitOpDone(ctx, logger.Log, m.iamOperationSvcClient, &iam.GetOperationRequest{OperationId: op.GetId()})
}

func (m *serviceReferencesManager) deleteServiceReference(ctx context.Context, cloudID, resourceID, resourceType string) error {
	var op *operation.Operation
	var err error
	if err := backoff.Retry(func() error {
		op, err = m.iamServiceControlSvcClient.UpdateReferences(ctx, &iam.UpdateServiceReferencesRequest{
			ServiceId: m.serviceID,
			Resource: &iam.Resource{
				Id:   cloudID,
				Type: ycloud.ResourceCloud,
			},
			ReferenceAdditions: nil,
			ReferenceDeletions: []*reference.Reference{
				{
					Referrer: &reference.Referrer{
						Type: resourceType,
						Id:   resourceID,
					},
					Type: reference.Reference_USED_BY,
				},
			},
		})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				logger.Log.Info(
					"service reference not found",
					log.String("resource id", resourceID),
					log.String("resource type", resourceType),
					log.String("cloud id", cloudID),
				)
				return nil
			}
			if !utilstatus.IsRetriable(err) {
				return backoff.Permanent(xerrors.Errorf("cannot delete service reference: %w", err))
			}
			return xerrors.Errorf("cannot delete service reference: %w", err)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.WithContext(backoff.NewConstantBackOff(500*time.Millisecond), ctx), 20)); err != nil {
		return xerrors.Errorf("cannot create delete service reference operation: %w", err)
	}
	if op == nil { // not found case
		return nil
	}
	return utiloperation.WaitOpDone(ctx, logger.Log, m.iamOperationSvcClient, &iam.GetOperationRequest{OperationId: op.GetId()})
}
