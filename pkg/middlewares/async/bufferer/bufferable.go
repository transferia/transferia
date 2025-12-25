package bufferer

// Bufferable is an interface for destination configurations.
//
// When implemented, it signals that the destination supports Bufferer as a middleware.
type Bufferable interface {
	// BuffererConfig returns a configuration for the bufferer middleware
	BuffererConfig() *BuffererConfig
}
