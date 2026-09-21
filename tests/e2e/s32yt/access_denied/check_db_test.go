package access_denied

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	_ "github.com/transferia/transferia/pkg/dataplane"
	"github.com/transferia/transferia/pkg/dataplane/provideradapter"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/worker/tasks"
)

func TestS3AccessDeniedFailsActivation(t *testing.T) {
	var requestCount atomic.Int64
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`))
	}))
	defer s3Server.Close()

	src := &s3_model.S3Source{
		Bucket: "test-s3-certbank",
		ConnectionConfig: s3_model.ConnectionConfig{
			AccessKey:        "access-key",
			SecretKey:        model.SecretString("secret-key"),
			S3ForcePathStyle: true,
			Endpoint:         s3Server.URL,
			Region:           "ru-central1",
		},
		PathPrefix:     "certificates/latest/",
		TableName:      "latest",
		TableNamespace: "certbank",
		InputFormat:    model.ParsingFormatCSV,
		IsInferSchema:  true,
		PathPattern:    "certificates_bank_latest.csv",
		UnparsedPolicy: s3_model.UnparsedPolicyFail,
	}
	src.WithDefaults()
	src.Format.CSVSetting.QuoteChar = `"`

	// Source schema resolution must fail before activation needs a YT connection.
	dst := provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Cluster:                  "localhost:1",
		Path:                     "//home/cdc/test/s32yt/access_denied",
		Static:                   true,
		UseStaticTableOnSnapshot: false,
	})
	dst.WithDefaults()
	transfer := &model.Transfer{
		ID:   "dtsupport-7671",
		Type: abstract.TransferTypeSnapshotOnly,
		Src:  src,
		Dst:  dst,
		Runtime: &abstract.LocalRuntime{
			Host:       "localhost",
			CurrentJob: 0,
			ShardingUpload: abstract.ShardUploadParams{
				JobCount:     1,
				ProcessCount: 1,
			},
		},
	}
	transfer.FillDependentFields()
	require.NoError(t, provideradapter.ApplyForTransfer(transfer))

	// The test runner owns the timeout: storage schema resolution uses its own
	// background context, so a test goroutine cannot cancel a stuck activation.
	err := tasks.ActivateDelivery(
		context.Background(),
		nil,
		coordinator.NewFakeClient(),
		*transfer,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
	)
	var failure awserr.RequestFailure
	require.ErrorAs(t, err, &failure)
	require.Equal(t, "AccessDenied", failure.Code())
	require.Equal(t, http.StatusForbidden, failure.StatusCode())
	require.EqualValues(t, 1, requestCount.Load(), "permanent S3 errors must not be retried")
}
