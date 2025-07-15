package sink

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/format"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
)

var (
	FatalAWSCodes = set.New("InvalidAccessKeyId")
)

type replicationUploader struct {
	cfg      *s3_provider.S3Destination
	logger   log.Logger
	uploader *s3manager.Uploader
}

func (u *replicationUploader) Upload(name string, lsns []uint64, data []byte) error {
	st := time.Now()
	buf := &bytes.Buffer{}
	fileName := fmt.Sprintf("%v.%v", name, strings.ToLower(string(u.cfg.OutputFormat)))
	if len(lsns) > 0 && lsns[len(lsns)-1] != 0 {
		fileName = fmt.Sprintf("%v-%v_%v.%v", name, lsns[0], lsns[len(lsns)-1], strings.ToLower(string(u.cfg.OutputFormat)))
	}
	if u.cfg.OutputEncoding == s3_provider.GzipEncoding {
		fileName = fileName + ".gz"
		gzWriter := gzip.NewWriter(buf)
		if _, err := gzWriter.Write(data); err != nil {
			return err
		}
		if err := gzWriter.Close(); err != nil {
			return xerrors.Errorf("unable to close gzip writer: %w", err)
		}
	} else {
		_, err := buf.Write(data)
		if err != nil {
			return xerrors.Errorf("unable to write: %w", err)
		}
	}
	res, err := u.uploader.Upload(&s3manager.UploadInput{
		Body:                    bytes.NewReader(buf.Bytes()),
		Bucket:                  aws.String(u.cfg.Bucket),
		Key:                     aws.String(fileName),
		Metadata:                nil,
		WebsiteRedirectLocation: nil,
	})
	if err != nil {
		u.logger.Error("upload: "+fileName, log.Any("res", res), log.Error(err))
		var awsErr awserr.Error
		if errors.As(err, &awsErr) {
			if FatalAWSCodes.Contains(awsErr.Code()) {
				return abstract.NewFatalError(xerrors.Errorf("upload fatal error: %w", err))
			}
			return xerrors.Errorf("aws error: code: %s, %s", awsErr.Code(), awsErr.Error())
		}
		return xerrors.Errorf("upload failed: %w", err)
	} else {
		u.logger.Infof("upload done: %v %v in %v", fileName, format.SizeInt(buf.Len()), time.Since(st))
	}
	return nil
}
