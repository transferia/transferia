package list

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/logging/batching_logger"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/s3util"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/dispatcher"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/lr_window/r_window"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/s3sess"
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

			currLogger := batching_logger.NewBatchingLogger(func(in string) { logger.Debug(in) }, "ListNewMyFiles", "|", true)
			defer currLogger.Close()

			for _, currentFile := range o.Contents {
				currentMarker = currentFile.Key
				currFileObject := file.NewFile(*currentFile.Key, *currentFile.Size, *currentFile.LastModified)

				if s3util.SkipObject(currentFile, srcModel.PathPattern, "|", inReader.ObjectsFilter()) {
					listStat.skippedBcsNotMatched++
					currLogger.Log(currFileObject.String() + ":skippedBcsNotMatched")
					continue
				}

				if !currDispatcher.IsMyFileName(currFileObject.FileName) {
					listStat.skippedBcsNotMine++
					currLogger.Log(currFileObject.String() + ":skippedBcsNotMine")
					continue // skip it, bcs NOT MY FILE
				}

				if currDispatcher.IsOutOfRWindow(currFileObject) {
					listStat.skippedBcsMineButOutOfRWindow++
					currLogger.Log(currFileObject.String() + ":skippedBcsMineButOutOfRWindow")
					continue // skip it, bcs OUT-OF-R-WINDOW
				}

				// here we are, if file is MY, the only question - new or not
				isNew, err := currDispatcher.AddIfNew(currFileObject)
				if err != nil {
					fatalError = abstract.NewFatalError(xerrors.Errorf("dispatcher.AddIfNew returned error, err: %w", err))
					return false
				}
				if !isNew {
					listStat.skippedBcsMineButKnown++
					currLogger.Log(currFileObject.String() + ":skippedBcsMineButKnown")
				} else {
					listStat.notSkipped++
					currLogger.Log(currFileObject.String() + ":Mine")
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

func ListAllReturnDispatcher(
	ctx context.Context,
	logger log.Logger,
	registry metrics.Registry,
	srcModel *s3.S3Source,
) (*dispatcher.Dispatcher, error) {
	_, s3client, currReader, _, err := s3sess.NewSessClientReaderMetrics(logger, srcModel, registry)
	if err != nil {
		return nil, xerrors.Errorf("failed to create s3session/s3client/reader, err: %w", err)
	}

	currDispatcher := dispatcher.NewDispatcher(
		r_window.NewRWindowEmpty(srcModel.OverlapDuration),
		srcModel.SyntheticPartitionsNum,
		effective_worker_num.NewEffectiveWorkerNumSingleWorker(),
		srcModel.OverlapDuration,
	)
	err = currDispatcher.BeforeListing()
	if err != nil {
		return nil, xerrors.Errorf("contract is broken, err: %w", err)
	}

	err = ListNewMyFiles(
		ctx,
		logger,
		srcModel,
		currReader,
		s3client,
		currDispatcher,
	)
	if err != nil {
		return nil, xerrors.Errorf("unable to list objects, err: %w", err)
	}

	currDispatcher.AfterListing()

	return currDispatcher, nil
}

func ListAll(
	ctx context.Context,
	logger log.Logger,
	registry metrics.Registry,
	srcModel *s3.S3Source,
) ([]file.File, error) {
	result, err := ListAllReturnDispatcher(ctx, logger, registry, srcModel)
	if err != nil {
		return nil, xerrors.Errorf("unable to list objects, err: %w", err)
	}
	return result.ExtractSortedFileEntries(), nil
}
