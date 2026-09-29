package coordinator

// Coordinator is an interface encapsulating (remote) procedure calls of the Coordinator of the dataplane nodes.
// This is an interface of an internal transport facility. Coordinator implementation MUST be thread-safe.
type Coordinator interface {
	Editor
	TransferStatus
	StatusMessageProvider
	TransferState
	TestReporter
	CheckEndpointReporter
	OperationStatus
	OperationState
	Sharding
}
