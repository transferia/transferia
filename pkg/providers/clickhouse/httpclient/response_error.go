package httpclient

// ResponseError wraps an unsuccessful HTTP response from QueryStream, preserving
// its status code and the error parsed from its body. Unwrap keeps the underlying
// ClickHouse exception available to error classification.
// The exactly-once snapshot sink uses it in snapshot.isRetryableReadError
// (snapshot/sink_funcs.go) to recognize HTTP 502/503/504 proxy failures without
// parsing error text, for safe-query retries and replica availability checks.
// These statuses do not make INSERT or other data-changing commands safe to retry.
type ResponseError struct {
	StatusCode int
	cause      error
}

func newResponseError(statusCode int, cause error) *ResponseError {
	return &ResponseError{StatusCode: statusCode, cause: cause}
}

func (e *ResponseError) Error() string {
	return e.cause.Error()
}

func (e *ResponseError) Unwrap() error {
	return e.cause
}
