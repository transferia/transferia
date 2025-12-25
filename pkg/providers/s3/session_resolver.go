package s3

import (
	"crypto/tls"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	creds "github.com/transferia/transferia/pkg/credentials"
	"go.ytsaurus.tech/library/go/core/log"
)

func findRegion(bucket, region string, s3ForcePathStyle bool) (string, error) {
	if region != "" {
		return region, nil
	}

	// No region, assuming public bucket.
	tmpSession, err := session.NewSession(&aws.Config{
		Region:           aws.String("aws-global"),
		S3ForcePathStyle: aws.Bool(s3ForcePathStyle),
		Credentials:      credentials.AnonymousCredentials,
	})
	if err != nil {
		return "", xerrors.Errorf("unable to init aws session: %w", err)
	}

	client := aws_s3.New(tmpSession)
	req, _ := client.ListObjectsRequest(&aws_s3.ListObjectsInput{
		Bucket: &bucket,
	})

	if err := req.Send(); err != nil {
		// expected request to fail, extract region form header
		if region := req.HTTPResponse.Header.Get("x-amz-bucket-region"); len(region) != 0 {
			return region, nil
		}
		return "", xerrors.Errorf("cannot get header from response with error: %w", err)
	}
	return "", xerrors.NewSentinel("unknown region")
}

func NewAWSSession(lgr log.Logger, bucket string, cfg ConnectionConfig) (*session.Session, error) {
	region, err := findRegion(bucket, cfg.Region, cfg.S3ForcePathStyle)
	if err != nil {
		return nil, xerrors.Errorf("unable to find region: %w", err)
	}
	cfg.Region = region

	if cfg.ServiceAccountID != "" {
		currCreds, err := creds.NewServiceAccountCreds(lgr, cfg.ServiceAccountID)
		if err != nil {
			return nil, xerrors.Errorf("unable to get service account credentials: %w", err)
		}
		sess, err := session.NewSession(&aws.Config{
			Endpoint:         aws.String(cfg.Endpoint),
			Region:           aws.String(cfg.Region),
			S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
			Credentials:      credentials.AnonymousCredentials,
			HTTPClient:       &http.Client{Transport: newCredentialsRoundTripper(currCreds, http.DefaultTransport)},
		})
		if err != nil {
			return nil, xerrors.Errorf("unable to create session: %w", err)
		}
		return sess, nil
	}

	cred := credentials.AnonymousCredentials
	if cfg.AccessKey != "" {
		cred = credentials.NewStaticCredentials(cfg.AccessKey, string(cfg.SecretKey), "")
	}
	sess, err := session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
		Credentials:      cred,
		DisableSSL:       aws.Bool(!cfg.UseSSL),
		HTTPClient:       &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: !cfg.VerifySSL}}},
	})
	if err != nil {
		return nil, xerrors.Errorf("unable to create session (without SA credentials): %w", err)
	}
	return sess, nil
}
