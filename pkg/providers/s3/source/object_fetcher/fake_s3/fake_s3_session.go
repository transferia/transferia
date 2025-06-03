package fake_s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
)

type myResolverT struct{}

func (t *myResolverT) EndpointFor(service, region string, opts ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
	return endpoints.ResolvedEndpoint{}, nil
}

func NewSess() *session.Session {
	return &session.Session{
		Config: &aws.Config{
			EndpointResolver: &myResolverT{},
		},
	}
}
