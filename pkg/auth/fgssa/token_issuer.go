package fgssa

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/golang/groupcache/lru"
	"github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/auth/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var tokenIssuer *TokenIssuer

type TokenIssuer struct {
	mu                 sync.Mutex
	resourceResolver   ResourceCloudIDResolver
	tokenServiceClient iam.IamTokenServiceClient
	agentTokensCache   *lru.Cache
	serviceID          string
	microserviceID     string
}

type tokenCacheEntry struct {
	token   string
	renewAt time.Time
}

func InitTokenIssuer(
	resourceResolver ResourceCloudIDResolver,
	tokenServiceClient iam.IamTokenServiceClient,
	serviceID string,
	microserviceID string,
	tokenCacheSize int,
) {
	tokenIssuer = &TokenIssuer{
		mu:                 sync.Mutex{},
		resourceResolver:   resourceResolver,
		tokenServiceClient: tokenServiceClient,
		agentTokensCache:   lru.New(tokenCacheSize),
		serviceID:          serviceID,
		microserviceID:     microserviceID,
	}
}

func TokenIssuerInstance() (*TokenIssuer, error) {
	if tokenIssuer == nil {
		return nil, xerrors.New("fine grained ssa token issuer hasn't init")
	}
	return tokenIssuer, nil
}

func (i *TokenIssuer) Token(ctx context.Context, resource *iam.Resource) (string, error) {
	cloudID, err := i.resourceResolver.ResolveCloudID(ctx, resource)
	if err != nil {
		return "", status.Errorf(status.Code(err), "cannot resolve resource's cloud id, resource id: %s, resource type: %s: %v", resource.GetId(), resource.GetType(), err)
	}

	return i.createToken(ctx, cloudID)
}

func (i *TokenIssuer) createToken(ctx context.Context, cloudID string) (string, error) {
	i.mu.Lock()
	if tokenEntry, ok := i.agentTokensCache.Get(cloudID); ok && tokenEntry.(tokenCacheEntry).renewAt.After(time.Now()) {
		i.mu.Unlock()
		return tokenEntry.(tokenCacheEntry).token, nil
	}
	i.mu.Unlock()

	agentSAToken, err := backoff.RetryWithData(func() (*iam.CreateIamTokenResponse, error) {
		token, iamErr := i.tokenServiceClient.CreateForService(ctx, &iam.CreateIamTokenForServiceRequest{
			ResourceId:     cloudID,
			ServiceId:      i.serviceID,
			MicroserviceId: i.microserviceID,
			ResourceType:   resources.ResourceCloud,
		})
		if code := status.Code(iamErr); code == codes.PermissionDenied || code == codes.DeadlineExceeded || code == codes.Canceled {
			return nil, backoff.Permanent(xerrors.Errorf("cannot create token for agent SA: %w", iamErr))
		}
		if iamErr != nil {
			return nil, xerrors.Errorf("cannot create token for agent SA: %w", iamErr)
		}
		return token, nil
	}, backoff.WithMaxRetries(backoff.WithContext(backoff.NewExponentialBackOff(), ctx), 5))
	if err != nil {
		return "", status.Errorf(codes.Internal, "cannot create token for agent SA, cloud id: %s, service id: %s, microservice id: %s: %v", cloudID, i.serviceID, i.microserviceID, err.Error())
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	i.agentTokensCache.Add(cloudID, tokenCacheEntry{
		token:   agentSAToken.GetIamToken(),
		renewAt: agentSAToken.GetExpiresAt().AsTime().Add(-time.Hour),
	})

	return agentSAToken.GetIamToken(), nil
}
