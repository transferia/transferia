//go:build !disable_delta_provider

package store

import (
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/util/iter"
)

var (
	_ Store       = (*S3)(nil)
	_ StoreConfig = (*S3Config)(nil)
)

type S3Config struct {
	Endpoint         string
	TablePath        string
	Region           string
	AccessKey        string
	S3ForcePathStyle bool
	Secret           string
	Bucket           string
	UseSSL           bool
	VerifySSL        bool
}

func (s S3Config) isStoreConfig() {}

type S3 struct {
	config *S3Config
	client *s3.S3
}

func (s S3) Root() string {
	return s.config.TablePath
}

func (s S3) Read(path string) (iter.Iter[string], error) {
	data, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(path),
	})
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case s3.ErrCodeNoSuchKey:
			return nil, ErrFileNotFound
		}
	}
	if err != nil {
		return nil, xerrors.Errorf("unable to read object: %s: %w", path, err)
	}
	return iter.FromReadCloser(data.Body), nil
}

func (s S3) ListFrom(path string) (iter.Iter[*FileMeta], error) {
	ls, err := s.client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(filepath.Dir(path)),
	})
	if err != nil {
		return nil, xerrors.Errorf("unable to list objects: %s: %w", path, err)
	}
	contents := yslices.Filter(ls.Contents, func(object *s3.Object) bool {
		return *object.Key > path
	})
	return iter.FromSlice(yslices.Map(contents, func(t *s3.Object) *FileMeta {
		return &FileMeta{
			path:         *t.Key,
			timeModified: *t.LastModified,
			size:         uint64(*t.Size),
		}
	})...), nil
}

func NewStoreS3(config *S3Config) (*S3, error) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(config.Endpoint),
		Region:           aws.String(config.Region),
		S3ForcePathStyle: aws.Bool(config.S3ForcePathStyle),
		Credentials: credentials.NewStaticCredentials(
			config.AccessKey, config.Secret, "",
		),
	})
	if err != nil {
		return nil, xerrors.Errorf("unable to init aws session: %w", err)
	}
	return &S3{
		config: config,
		client: s3.New(sess),
	}, nil
}
