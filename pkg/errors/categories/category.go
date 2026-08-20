package categories

// Category provides information about the high-level component of the Transfer service which is the cause of an error which occurred with a transfer.
// Any error can have only one assigned category.
type Category string

func (c Category) ID() string {
	return string(c)
}

type CategorizedError interface {
	error

	Category() Category
}

const (
	// Source is for errors originating from the source endpoint (its configuration, runtime properties, etc.)
	Source Category = "source"
	// Target is for errors originating from the target endpoint (its configuration, runtime properties, etc.)
	Target Category = "target"
	// Internal is for errors caused by the Transfer service's infrastructure and unfixable by the user
	Internal Category = "internal"
)
