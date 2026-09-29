package yds

import (
	"context"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/internal/logger"
	yds_cpclient "github.com/transferia/transferia/kikimr/public/sdk/go/persqueue/controlplane"
	Ydb_PersQueue_V1pb "github.com/transferia/transferia/kikimr/public/sdk/go/persqueue/genproto/Ydb_PersQueue_V1"
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	persqueue_session "github.com/transferia/transferia/kikimr/public/sdk/go/persqueue/session"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
	error_codes "github.com/transferia/transferia/pkg/errors/codes"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	yds_source "github.com/transferia/transferia/pkg/providers/yds/source"
	backoffutil "github.com/transferia/transferia/pkg/util/backoff"
	"github.com/transferia/transferia/pkg/xtls"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

func convertCodecs(src []yds_source.YdsCompressionCodec) (dst []Ydb_PersQueue_V1pb.Codec) {
	dst = make([]Ydb_PersQueue_V1pb.Codec, 0, len(src))
	for _, srcItem := range src {
		dst = append(dst, Ydb_PersQueue_V1pb.Codec(srcItem))
	}
	return dst
}

func newPQOptions(src *yds_source.YDSSource) (*persqueue_session.Options, error) {
	opts := &persqueue_session.Options{
		Endpoint: src.Endpoint,
		Port:     src.Port,
		Logger:   corelogadapter.New(logger.Log),
		Database: src.Database,
	}
	if src.TLSCACertificate != "" {
		tls, err := xtls.FromContent(src.TLSCACertificate)
		if err != nil {
			return nil, xerrors.Errorf("could not create TLS config from certificate content: %w", err)
		}
		opts.TLSConfig = tls
	} else if src.TLSEnalbed {
		tls, err := xtls.FromPath(src.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("error getting TLS config: %w", err)
		}
		opts.TLSConfig = tls
	}
	opts.Credentials = src.Credentials
	if opts.Credentials == nil {
		var err error
		opts.Credentials, err = provider_ydb.ResolveCredentials(
			src.UserdataAuth,
			string(src.Token),
			provider_ydb.JWTAuthParams{
				KeyContent:      src.SAKeyContent,
				TokenServiceURL: src.TokenServiceURL,
			},
			src.ServiceAccountID,
			nil,
			logger.Log,
		)
		if err != nil {
			return nil, xerrors.Errorf("Cannot create YDB credentials: %w", err)
		}
	}
	return opts, nil
}

func CreateReadRule(src *yds_source.YDSSource, defaultConsumer string) error {
	consumer := src.Consumer
	if consumer == "" {
		consumer = defaultConsumer
	}
	return backoff.RetryNotify(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		opts, err := newPQOptions(src)
		if err != nil {
			return xerrors.Errorf("unable to init pq options: %w", err)
		}
		lbControlPlane, err := yds_cpclient.NewControlPlaneClient(ctx, *opts)
		if err != nil {
			return xerrors.Errorf("unable to init pq cp client: %w", err)
		}
		defer lbControlPlane.Close()
		dRes, err := lbControlPlane.DescribeTopic(ctx, src.Stream)
		if err != nil {
			return xerrors.Errorf("unable to describe topic: %s, err: %w", src.Stream, err)
		}
		logger.Log.Infof("topic info: %v", dRes)
		if readRuleExists(dRes, consumer) {
			return nil
		}
		if err = lbControlPlane.AddReadRule(ctx, &Ydb_PersQueue_V1pb.AddReadRuleRequest{
			Path: src.Stream,
			ReadRule: &Ydb_PersQueue_V1pb.TopicSettings_ReadRule{
				ConsumerName:    consumer,
				Important:       false,
				SupportedFormat: Ydb_PersQueue_V1pb.TopicSettings_FORMAT_BASE,
				SupportedCodecs: convertCodecs(src.GetSupportedCodecs()),
				Version:         1,
				// ServiceType:     "data-transfer",
			},
		}); err != nil {
			if strings.Contains(err.Error(), "ALREADY_EXISTS") {
				return nil
			}
			if st, ok := grpc_status.FromError(err); ok {
				switch st.Code() {
				case grpc_codes.AlreadyExists:
					return nil
				case grpc_codes.ResourceExhausted:
					return coded.Errorf(error_codes.YDBOverloaded, "add read-rule failed: %w", err)
				}
			}
			if strings.Contains(err.Error(), "OVERLOADED") {
				return coded.Errorf(error_codes.YDBOverloaded, "add read-rule failed: %w", err)
			}
			return err
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3), backoffutil.Log(context.Background(), "add read-rule"))
}

func readRuleExists(topicDescription *Ydb_PersQueue_V1pb.DescribeTopicResult, consumer string) bool {
	for _, rule := range topicDescription.Settings.ReadRules {
		if rule.ConsumerName == consumer {
			logger.Log.Infof("Read rule for consumer %s already exists: important = %v, starting message timestamp(ms) = %v", consumer, rule.Important, rule.StartingMessageTimestampMs)
			return true
		}
	}
	return false
}

func DropReadRule(src *yds_source.YDSSource, defaultConsumer string) error {
	if err := backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		opts, err := newPQOptions(src)
		if err != nil {
			return xerrors.Errorf("unable to get new persqueue options from YDS source: %w", err)
		}
		lbControlPlane, err := yds_cpclient.NewControlPlaneClient(ctx, *opts)
		if err != nil {
			return xerrors.Errorf("unable to get new persqueue controlplane client: %w", err)
		}
		defer lbControlPlane.Close()
		dRes, err := lbControlPlane.DescribeTopic(ctx, src.Stream)
		if err != nil {
			return xerrors.Errorf("unable to get describe topic result from persqueue controlplane: %w", err)
		}
		logger.Log.Infof("topic info: %v", dRes)
		consumer := src.Consumer
		if consumer == "" {
			consumer = defaultConsumer
		}
		if err := lbControlPlane.RemoveReadRule(ctx, &Ydb_PersQueue_V1pb.RemoveReadRuleRequest{
			Path:         src.Stream,
			ConsumerName: consumer,
		}); err != nil {
			if errStatus, ok := grpc_status.FromError(err); ok && errStatus.Code() == grpc_codes.NotFound {
				logger.Log.Infof("unable to remove read rule (path=%s, consumer=%s): %v", src.Stream, consumer, err)
			} else {
				return xerrors.Errorf("unable to remove read rule from persqueue controlplane: %w", err)
			}
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 10)); err != nil {
		return err
	}
	return nil
}
