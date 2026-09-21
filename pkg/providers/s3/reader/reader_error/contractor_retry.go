package reader_error

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	aws_request "github.com/aws/aws-sdk-go/aws/request"
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	backoffutil "github.com/transferia/transferia/pkg/util/backoff"
)

// ContractorPhase distinguishes schema resolution from row reading when deciding retries and invariants.
type ContractorPhase int

const (
	// ContractorPhaseResolveSchema corresponds to ReaderContractor.ResolveSchema / resolveSchemaCached.
	ContractorPhaseResolveSchema ContractorPhase = iota
	// ContractorPhaseRead corresponds to S3Reader.Read and the completion-chunk push.
	ContractorPhaseRead
)

// ContractorRetryDecision is what RunWithContractorRetry should do after an error from the inner callback.
type ContractorRetryDecision int

const (
	// ContractorDecisionReturn returns err to the caller (no retry).
	ContractorDecisionReturn ContractorRetryDecision = iota
	// ContractorDecisionRetry applies backoff and runs the callback again (retryable transport / sink).
	ContractorDecisionRetry
	// ContractorDecisionUpgradeFatal wraps the error as ReaderErrorFatal — invalid combination of phase and kind.
	ContractorDecisionUpgradeFatal
)

// ContractorRetryDecisionFor maps (policy is reserved for future rules that tie UnparsedPolicy to schema walks)
// phase + concrete ReaderError kind to retry / return / upgrade-to-fatal.
//
// Invariants that cannot occur in a correct reader stack (e.g. sink failure during schema-only resolution)
// are upgraded to fatal so callers fail loudly instead of retrying forever or mis-classifying.
func ContractorRetryDecisionFor(_ s3_model.UnparsedPolicy, phase ContractorPhase, err ReaderError) ContractorRetryDecision {
	if err == nil {
		return ContractorDecisionReturn
	}
	k := ClassifyContractorError(err)
	switch phase {
	case ContractorPhaseResolveSchema:
		switch k {
		case ContractorKindSink:
			return ContractorDecisionUpgradeFatal
		case ContractorKindNoFiles:
			return ContractorDecisionReturn
		}
	case ContractorPhaseRead:
		if k == ContractorKindNoFiles {
			return ContractorDecisionUpgradeFatal
		}
		if k == ContractorKindNoSuchFile {
			return ContractorDecisionReturn
		}
	}

	switch k {
	case ContractorKindTransport:
		if isPermanentS3RequestFailure(err) {
			return ContractorDecisionReturn
		}
		return ContractorDecisionRetry
	case ContractorKindSink:
		return ContractorDecisionRetry
	case ContractorKindFatal, ContractorKindConfig, ContractorKindNoFiles, ContractorKindNoSuchFile:
		return ContractorDecisionReturn
	case ContractorKindData:
		// UnparsedPolicy is applied inside format readers; contractor boundary does not retry data errors.
		return ContractorDecisionReturn
	case ContractorKindUnknown:
		return ContractorDecisionUpgradeFatal
	default:
		return ContractorDecisionUpgradeFatal
	}
}

func isPermanentS3RequestFailure(err error) bool {
	var requestFailure awserr.RequestFailure
	if !xerrors.As(err, &requestFailure) {
		return false
	}
	if aws_request.IsErrorRetryable(requestFailure) || aws_request.IsErrorThrottle(requestFailure) {
		return false
	}
	statusCode := requestFailure.StatusCode()
	return statusCode >= http.StatusBadRequest &&
		statusCode < http.StatusInternalServerError &&
		statusCode != http.StatusRequestTimeout &&
		statusCode != http.StatusTooManyRequests
}

// RunWithContractorRetry runs fn until it returns nil, a non-retryable ReaderError, or ctx done.
// Sink errors and retryable transport errors are retried (see ContractorRetryDecisionFor); permanent S3 4xx
// request failures are returned immediately. Backoff uses bo.NextBackOff; when bo returns backoff.Stop, the last
// error is returned.
func RunWithContractorRetry(
	ctx context.Context,
	policy s3_model.UnparsedPolicy,
	phase ContractorPhase,
	bo backoff.BackOff,
	fn func() ReaderError,
) ReaderError {
	if bo != nil {
		bo.Reset()
	}
	var last ReaderError
	for {
		select {
		case <-ctx.Done():
			return NewReaderErrorFatal("RunWithContractorRetry.ctx", ctx.Err())
		default:
		}

		last = fn()
		if last == nil {
			return nil
		}

		switch ContractorRetryDecisionFor(policy, phase, last) {
		case ContractorDecisionUpgradeFatal:
			return NewReaderErrorFatal(
				"RunWithContractorRetry.invariant",
				xerrors.Errorf("%w", last),
			)
		case ContractorDecisionReturn:
			return last
		case ContractorDecisionRetry:
			if bo == nil {
				return last
			}
			d := bo.NextBackOff()
			if d == backoff.Stop {
				return last
			}
			backoffutil.Log(ctx, "S3 reader operation")(last, d)
			select {
			case <-ctx.Done():
				return NewReaderErrorFatal("RunWithContractorRetry.ctx", ctx.Err())
			case <-time.After(d):
			}
		}
	}
}
