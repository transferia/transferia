package list

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher/file"
	"go.ytsaurus.tech/library/go/core/log"
)

const listSize = 1000

// ListNewMyFiles - saves matched && new files into dispatcher
func ListNewMyFiles(
	ctx context.Context,
	logger log.Logger,
	srcModel *s3.S3Source,
	inReader reader.Reader,
	s3client s3iface.S3API,
	currDispatcher *dispatcher.Dispatcher,
) error {
	var fatalError error = nil
	listStat := newStat()
	var currentMarker *string
	endOfBucket := false
	for {
		callback := func(o *aws_s3.ListObjectsOutput, _ bool) bool {
			if fatalError != nil {
				return false
			}
			for _, currentFile := range o.Contents {
				currentMarker = currentFile.Key

				if reader.SkipObject(currentFile, srcModel.PathPattern, "|", inReader.ObjectsFilter()) {
					logger.Debugf("ListNewMyFiles - file did not pass type/path check, skipping: file %s, pathPattern: %s", *currentFile.Key, srcModel.PathPattern) // TODO - MAKE HERE SPECIAL LOGGER!!!
					listStat.skippedBcsNotMatched++
					continue
				}

				currFileObject := file.NewFile(*currentFile.Key, *currentFile.Size, *currentFile.LastModified)

				if !currDispatcher.IsMyFileName(currFileObject.FileName) {
					listStat.skippedBcsNotMine++
					continue // skip it, bcs NOT MY FILE
				}

				// here we are, if file is MY, the only question - new or not
				isNew, err := currDispatcher.AddIfNew(currFileObject)
				if err != nil {
					fatalError = abstract.NewFatalError(xerrors.Errorf("dispatcher.AddIfNew returned error, err: %w", err))
					return false
				}
				if !isNew {
					listStat.skippedBcsMineButKnown++
				} else {
					logger.Debugf("new file found: file %s, pathPattern: %s, fileSize: %d, lastModified: %s, syntheticPartitionNum:%d", currFileObject.FileName, srcModel.PathPattern, currFileObject.FileSize, currFileObject.LastModified, currDispatcher.DetermineSyntheticPartitionNum(currFileObject.FileName)) // TODO - MAKE HERE SPECIAL LOGGER!!!
					listStat.notSkipped++
				}
			}
			if len(o.Contents) < listSize {
				endOfBucket = true
			}
			return true
		}

		err := s3client.ListObjectsPagesWithContext(
			ctx,
			&aws_s3.ListObjectsInput{
				Bucket:  aws.String(srcModel.Bucket),
				Prefix:  aws.String(srcModel.PathPrefix),
				MaxKeys: aws.Int64(listSize),
				Marker:  currentMarker,
			},
			callback,
		)
		if err != nil {
			return xerrors.Errorf("unable to list objects pages, err: %w", err)
		}

		if endOfBucket || fatalError != nil {
			break
		}
	}
	listStat.log(logger)
	return fatalError
}
