package connection

import "github.com/nats-io/nats.go"

// ConnectionConfig holds the details required to connect to a NATS server.
type ConnectionConfig struct {
	NatsConnectionOptions *nats.Options
}
