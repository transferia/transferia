package abstract

// Movable is a sinker which supports replace cleanup policy
type Replacable interface {
	Sinker
	// Replace temporary tables
	Replace() error
}
