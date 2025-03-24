package source

import (
	"context"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

var _ ObjectFetcher = (*pollingSource)(nil)

type inflightObject struct {
	Object Object
	Done   bool
}

type pollingSource struct {
	ctx        context.Context
	client     s3iface.S3API
	src        *s3.S3Source
	reader     reader.Reader
	logger     log.Logger
	cp         coordinator.Coordinator
	transferID string
	lastObject *Object          // cronologically sorted last object in previously fetched slices
	inflight   []inflightObject // objects currently being processed
	mu         sync.Mutex
}

// FetchObjects fetches the whole list of objects from an S3 bucket and extracts the new objects in need of syncing from this list.
// The last synced object is used as reference to identify new objects. The newest object is added to the internal state for storing.
// All objects not matching the object type or pathPrefix are skipped.
func (s *pollingSource) FetchObjects() ([]Object, error) {
	if s.lastObject == nil {
		obj, err := s.loadTransferState()
		if err != nil {
			return nil, xerrors.Errorf("failed to load last read object: %w", err)
		}
		s.lastObject = obj
	}

	objectList, err := s.listFiles(s.lastObject)
	if err != nil {
		return nil, xerrors.Errorf("failed to get list of new objects: %w", err)
	}

	if len(objectList) > 0 {
		s.lastObject = &objectList[len(objectList)-1]
		s.addToInflight(objectList)
	}
	return objectList, nil
}

func (s *pollingSource) addToInflight(objects []Object) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, obj := range objects {
		s.inflight = append(s.inflight, inflightObject{
			Object: obj,
			Done:   false,
		})
	}
}

func (s *pollingSource) listFiles(lastObject *Object) ([]Object, error) {
	var objectList []Object
	var currentMarker *string
	endOfBucket := false
	for {
		if err := s.client.ListObjectsPagesWithContext(s.ctx, &aws_s3.ListObjectsInput{
			Bucket:  aws.String(s.src.Bucket),
			Prefix:  aws.String(s.src.PathPrefix),
			MaxKeys: aws.Int64(1000),
			Marker:  currentMarker,
		}, func(o *aws_s3.ListObjectsOutput, _ bool) bool {
			for _, file := range o.Contents {
				currentMarker = file.Key
				if reader.SkipObject(file, s.src.PathPattern, "|", s.reader.ObjectsFilter()) {
					s.logger.Infof("file did not pass type/path check, skipping: file %s, pathPattern: %s", *file.Key, s.src.PathPattern)
					continue
				}
				if lastObject == nil || (lastObject != nil && lastObject.LastModified.Before(*file.LastModified)) {
					s.logger.Infof("file %s: %s", *file.Key, *file.LastModified)
					objectList = append(objectList, Object{
						Name:         *file.Key,
						LastModified: *file.LastModified,
					})
				}

				if lastObject != nil && lastObject.LastModified.Equal(*file.LastModified) {
					// if different file with same timestamp, add
					if lastObject.Name != *file.Key && lastObject.Name < *file.Key {
						objectList = append(objectList, Object{
							Name:         *file.Key,
							LastModified: *file.LastModified,
						})
					}
				}
			}
			if len(o.Contents) < 1000 {
				endOfBucket = true
			}
			return true
		}); err != nil {
			return nil, err
		}

		if endOfBucket {
			break
		}
	}

	sort.SliceStable(objectList, func(i, j int) bool { // keep original ordering if equal
		return objectList[i].LastModified.Before(objectList[j].LastModified)
	})

	return objectList, nil
}

func (s *pollingSource) storeTransferState(object Object) error {
	if err := s.cp.SetTransferState(s.transferID, map[string]*coordinator.TransferStateData{
		ReadProgressKey: {Generic: object},
	}); err != nil {
		return xerrors.Errorf("unable to set transfer state: %w", err)
	}

	return nil
}

func (s *pollingSource) loadTransferState() (*Object, error) {
	stateMap, err := s.cp.GetTransferState(s.transferID)
	if err != nil {
		return nil, xerrors.Errorf("unable to get transfer state: %w", err)
	}

	state, ok := stateMap[ReadProgressKey]
	if ok && state.GetGeneric() != nil {
		var lastObject Object
		if err := util.MapFromJSON(state.Generic, &lastObject); err != nil {
			return nil, xerrors.Errorf("unable to unmarshal transfer state: %w", err)
		}
		return &lastObject, nil
	}

	return nil, nil
}

// Commit sets the matching object from the inflight to done.
// An object is persisted if the following occurs for example:
// 4 objects in inflight's sorted from newest to oldest by lastModified timestamp.
// If the first file is done it checks if the next file is also done.
// It does so until the first not-done file is found.
// Last of the done files is persisted to state (basically the file with the most recent lastModified timestamp).
// Inflight's list is cleared from persisted files to ensure a small size.
func (s *pollingSource) Commit(object Object) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var toPersist Object
	continuous := true
	dropPrefixed := 0
	for index, inflightObject := range s.inflight {
		if inflightObject.Object.Name == object.Name {
			s.inflight[index].Done = true // mark object as processed
		}

		if s.inflight[index].Done && continuous { // check if current object is done, if not then nothing to persist yet
			toPersist = s.inflight[index].Object
			dropPrefixed = index
		} else {
			continuous = false
		}
	}

	if dropPrefixed != 0 || len(s.inflight) == 1 {
		// there is something to persist and inflight needs resizing
		if err := s.storeTransferState(toPersist); err != nil {
			return xerrors.Errorf("failed to store latest read state: %w", err)
		}
		s.inflight = s.inflight[dropPrefixed+1:]
	}

	return nil
}

func NewPollingSource(ctx context.Context, client s3iface.S3API, logger log.Logger,
	cp coordinator.Coordinator, transferID string, reader reader.Reader, sourceConfig *s3.S3Source,
) (*pollingSource, error) {
	return &pollingSource{
		ctx:        ctx,
		client:     client,
		logger:     logger,
		src:        sourceConfig,
		cp:         cp,
		reader:     reader,
		transferID: transferID,
		lastObject: nil,
		inflight:   []inflightObject{},
		mu:         sync.Mutex{},
	}, nil
}
