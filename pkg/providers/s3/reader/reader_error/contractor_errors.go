package reader_error

// ContractorErrorKind classifies ReaderError values for ReaderContractor.Read / ResolveSchema.
type ContractorErrorKind int

const (
	ContractorKindTransport ContractorErrorKind = iota
	ContractorKindData
	ContractorKindConfig
	ContractorKindSink
	ContractorKindFatal
	ContractorKindNoFiles
	ContractorKindNoSuchFile
	ContractorKindUnknown
)

// ClassifyContractorError maps concrete ReaderError types to ContractorErrorKind.
// Callers use this for metrics, retry policy, or propagation (fatal vs retriable transport).
func ClassifyContractorError(err ReaderError) ContractorErrorKind {
	if err == nil {
		return ContractorKindUnknown
	}
	switch err.(type) {
	case ReaderErrorTransport:
		return ContractorKindTransport
	case ReaderErrorData:
		return ContractorKindData
	case ReaderErrorConfig:
		return ContractorKindConfig
	case ReaderErrorSink:
		return ContractorKindSink
	case ReaderErrorFatal:
		return ContractorKindFatal
	case ReaderErrorNoFiles:
		return ContractorKindNoFiles
	case ReaderErrorNoSuchFile:
		return ContractorKindNoSuchFile
	default:
		return ContractorKindUnknown
	}
}

// ContractorTransportOrSinkMayRetry reports whether the error is typically retried by an outer worker
// (transport I/O or sink push failure). Fatal/config/data-layer errors are not covered here.
func contractorTransportOrSinkMayRetry(err ReaderError) bool {
	switch ClassifyContractorError(err) {
	case ContractorKindTransport, ContractorKindSink:
		return true
	default:
		return false
	}
}

// ContractorShouldPropagateFatal reports whether the error must not be swallowed (fatal reader layer).
func contractorShouldPropagateFatal(err ReaderError) bool {
	return ClassifyContractorError(err) == ContractorKindFatal
}
