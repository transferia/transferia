package grpcutil

const (
	RequestIDHeaderKey      = "x-request-id"
	ForwardedForHeaderKey   = "x-forwarded-for"
	ForwardedAgentHeaderKey = "x-forwarded-agent"
	IdempotencyKeyHeaderKey = "idempotency-key" // https://cloud.yandex.com/en-ru/docs/api-design-guide/concepts/idempotency
)
