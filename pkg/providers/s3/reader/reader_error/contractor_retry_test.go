package reader_error

import (
	"context"
	"testing"

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
