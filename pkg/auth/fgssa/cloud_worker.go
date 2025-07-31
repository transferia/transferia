package fgssa

import (
	"context"
	"errors"
	"sync"

	"github.com/transferia/transferia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"golang.org/x/sync/semaphore"
)

type ResourceCloudIDResolver interface {
	ResolveCloudID(ctx context.Context, resource *iam.Resource) (string, error)
}

const (
	maxWorkers = 10
)

func doWorkWithCloudParallel(
	ctx context.Context,
	f func(ctx context.Context, cloudID string) error,
	cloudIDs []string,
) error {
	resChan := make(chan error, len(cloudIDs))
	sem := semaphore.NewWeighted(maxWorkers)
	var mu sync.Mutex
	processedCloudIDs := make(map[string]struct{})

	for _, cloudID := range cloudIDs {
		if err := sem.Acquire(ctx, 1); err != nil {
			resChan <- xerrors.Errorf("do work with cloud id: %s - unable to acquire semaphore: %w", cloudID, err)
			continue
		}
		go func(cloudID string) {
			defer sem.Release(1)

			mu.Lock()
			if _, ok := processedCloudIDs[cloudID]; ok {
				mu.Unlock()
				resChan <- nil
				return
			}
			processedCloudIDs[cloudID] = struct{}{}
			mu.Unlock()

			if err := f(ctx, cloudID); err != nil {
				resChan <- xerrors.Errorf("work failed cloud id: %s: %w", cloudID, err)
			} else {
				resChan <- nil
			}
		}(cloudID)
	}

	var nonNilErrors []error

	for i := 0; i < len(cloudIDs); i++ {
		res := <-resChan
		if res != nil {
			nonNilErrors = append(nonNilErrors, res)
		}
	}

	if len(nonNilErrors) > 0 {
		return errors.Join(nonNilErrors...)
	}

	return nil
}
