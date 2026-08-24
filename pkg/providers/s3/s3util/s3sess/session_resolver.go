package s3sess

import (
	"crypto/tls"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/credentials"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"go.ytsaurus.tech/library/go/core/log"
)

func newHTTPClient(transport http.RoundTripper) *http.Client {
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func findRegion(bucket, region string, s3ForcePathStyle bool) (string, error) {
	if region != "" {
		return region, nil
	}

	// No region, assuming public bucket.
	tmpSession, err := aws_session.NewSession(&aws.Config{
		Region:           aws.String("aws-global"),
		S3ForcePathStyle: aws.Bool(s3ForcePathStyle),
		Credentials:      aws_credentials.AnonymousCredentials,
		HTTPClient:       newHTTPClient(http.DefaultTransport),
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

func NewAWSSession(lgr log.Logger, bucket string, cfg s3_model.ConnectionConfig) (*aws_session.Session, error) {
	region, err := findRegion(bucket, cfg.Region, cfg.S3ForcePathStyle)
	if err != nil {
		return nil, xerrors.Errorf("unable to find region: %w", err)
	}
	cfg.Region = region

	if cfg.ServiceAccountID != "" {
		currCreds, err := credentials.NewServiceAccountCreds(lgr, cfg.ServiceAccountID)
		if err != nil {
			return nil, xerrors.Errorf("unable to get service account credentials: %w", err)
		}
		sess, err := aws_session.NewSession(&aws.Config{
			Endpoint:         aws.String(cfg.Endpoint),
			Region:           aws.String(cfg.Region),
			S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
			Credentials:      aws_credentials.AnonymousCredentials,
			HTTPClient:       newHTTPClient(NewCredentialsRoundTripper(currCreds, http.DefaultTransport)),
		})
		if err != nil {
			return nil, xerrors.Errorf("unable to create session: %w", err)
		}
		return sess, nil
	}

	cred := aws_credentials.AnonymousCredentials
	if cfg.AccessKey != "" {
		cred = aws_credentials.NewStaticCredentials(cfg.AccessKey, string(cfg.SecretKey), "")
	}
	sess, err := aws_session.NewSession(&aws.Config{
		Endpoint:         aws.String(cfg.Endpoint),
		Region:           aws.String(cfg.Region),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
		Credentials:      cred,
		DisableSSL:       aws.Bool(!cfg.UseSSL),
		HTTPClient:       newHTTPClient(&http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: !cfg.VerifySSL}}),
	})
	if err != nil {
		return nil, xerrors.Errorf("unable to create session (without SA credentials): %w", err)
	}
	return sess, nil
}
