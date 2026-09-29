package reader_error

import (
	"context"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
)

// unknownReaderErr implements ReaderError but is not one of the classified concrete types.
type unknownReaderErr struct{ msg string }

func (e unknownReaderErr) Error() string { return e.msg }
func (unknownReaderErr) isReaderError()  {}

func TestContractorRetryDecisionFor_AllPoliciesTransportRetry(t *testing.T) {
	tr := NewReaderErrorTransport("op", "f", xerrors.New("t"))
	for _, pol := range []s3_model.UnparsedPolicy{
		s3_model.UnparsedPolicyFail,
		s3_model.UnparsedPolicyContinue,
		s3_model.UnparsedPolicyRetry,
	} {
		t.Run(string(pol), func(t *testing.T) {
			require.Equal(t, ContractorDecisionRetry, ContractorRetryDecisionFor(pol, ContractorPhaseResolveSchema, tr))
			require.Equal(t, ContractorDecisionRetry, ContractorRetryDecisionFor(pol, ContractorPhaseRead, tr))
		})
	}
}

func TestContractorRetryDecisionFor_ImpossibleSinkDuringResolve(t *testing.T) {
	sk := NewReaderErrorSink("sink", "f", xerrors.New("s"))
	for _, pol := range []s3_model.UnparsedPolicy{
		s3_model.UnparsedPolicyFail,
		s3_model.UnparsedPolicyContinue,
		s3_model.UnparsedPolicyRetry,
	} {
		require.Equal(t, ContractorDecisionUpgradeFatal, ContractorRetryDecisionFor(pol, ContractorPhaseResolveSchema, sk))
		require.Equal(t, ContractorDecisionRetry, ContractorRetryDecisionFor(pol, ContractorPhaseRead, sk))
	}
}

func TestContractorRetryDecisionFor_ImpossibleNoFilesDuringRead(t *testing.T) {
	nf := NewReaderErrorNoFiles("op", "pfx")
	for _, pol := range []s3_model.UnparsedPolicy{
		s3_model.UnparsedPolicyFail,
		s3_model.UnparsedPolicyContinue,
		s3_model.UnparsedPolicyRetry,
	} {
		require.Equal(t, ContractorDecisionReturn, ContractorRetryDecisionFor(pol, ContractorPhaseResolveSchema, nf))
		require.Equal(t, ContractorDecisionUpgradeFatal, ContractorRetryDecisionFor(pol, ContractorPhaseRead, nf))
	}
}

func TestRunWithContractorRetry_TransportThenOK(t *testing.T) {
	ctx := context.Background()
	bo := backoff.NewConstantBackOff(0)
	var n int
	err := RunWithContractorRetry(
		ctx,
		s3_model.UnparsedPolicyContinue,
		ContractorPhaseResolveSchema,
		bo,
		func() ReaderError {
			n++
			if n < 3 {
				return NewReaderErrorTransport("op", "k", xerrors.New("transient"))
			}
			return nil
		},
	)
	require.Nil(t, err)
	require.Equal(t, 3, n)
}

func TestRunWithContractorRetry_PermanentS3RequestFailure(t *testing.T) {
	var attempts int
	accessDenied := awserr.NewRequestFailure(
		awserr.New("AccessDenied", "Access Denied", nil),
		http.StatusForbidden,
		"request-id",
	)
	err := RunWithContractorRetry(
		context.Background(),
		s3_model.UnparsedPolicyFail,
		ContractorPhaseResolveSchema,
		backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 1),
		func() ReaderError {
			attempts++
			return NewReaderErrorTransport("list", "prefix", accessDenied)
		},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, accessDenied)
	require.Equal(t, 1, attempts)
}

func TestContractorRetryDecisionFor_S3RequestFailures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		code       string
		statusCode int
		want       ContractorRetryDecision
	}{
		{name: "access denied", code: "AccessDenied", statusCode: http.StatusForbidden, want: ContractorDecisionReturn},
		{name: "bad request", code: "InvalidArgument", statusCode: http.StatusBadRequest, want: ContractorDecisionReturn},
		{name: "missing bucket", code: "NoSuchBucket", statusCode: http.StatusNotFound, want: ContractorDecisionReturn},
		{name: "HTTP timeout", code: "UnknownError", statusCode: http.StatusRequestTimeout, want: ContractorDecisionRetry},
		{name: "HTTP throttling", code: "UnknownError", statusCode: http.StatusTooManyRequests, want: ContractorDecisionRetry},
		{name: "SDK timeout", code: "RequestTimeout", statusCode: http.StatusBadRequest, want: ContractorDecisionRetry},
		{name: "SDK throttling", code: "Throttling", statusCode: http.StatusBadRequest, want: ContractorDecisionRetry},
		{name: "expired credentials", code: "ExpiredToken", statusCode: http.StatusForbidden, want: ContractorDecisionRetry},
		{name: "server failure", code: "InternalError", statusCode: http.StatusInternalServerError, want: ContractorDecisionRetry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := awserr.NewRequestFailure(awserr.New(tc.code, "S3 request failed", nil), tc.statusCode, "request-id")
			err := WrapContractorReadStep("contractor", NewReaderErrorTransport("list", "prefix", xerrors.Errorf("request failed: %w", failure)))
			for _, policy := range []s3_model.UnparsedPolicy{s3_model.UnparsedPolicyFail, s3_model.UnparsedPolicyContinue, s3_model.UnparsedPolicyRetry} {
				for _, phase := range []ContractorPhase{ContractorPhaseResolveSchema, ContractorPhaseRead} {
					require.Equal(t, tc.want, ContractorRetryDecisionFor(policy, phase, err), "policy %s, phase %v", policy, phase)
				}
			}
		})
	}
}

func TestRunWithContractorRetry_DataNoLoop(t *testing.T) {
	ctx := context.Background()
	bo := backoff.NewConstantBackOff(0)
	var n int
	dataErr := NewReaderErrorDataRecord("op", "f.csv", 1, xerrors.New("bad"))
	err := RunWithContractorRetry(
		ctx,
		s3_model.UnparsedPolicyContinue,
		ContractorPhaseRead,
		bo,
		func() ReaderError {
			n++
			return dataErr
		},
	)
	require.Equal(t, 1, n)
	require.Equal(t, dataErr, err)
}

func TestRunWithContractorRetry_UnknownKindFatal(t *testing.T) {
	ctx := context.Background()
	bo := backoff.NewConstantBackOff(0)
	err := RunWithContractorRetry(
		ctx,
		s3_model.UnparsedPolicyContinue,
		ContractorPhaseRead,
		bo,
		func() ReaderError {
			return unknownReaderErr{"opaque"}
		},
	)
	require.Error(t, err)
	_, ok := err.(ReaderErrorFatal)
	require.True(t, ok, "expected ReaderErrorFatal for unknown ReaderError kind")
}
