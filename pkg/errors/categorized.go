package errors

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/categories"
)

// Categorized is an error with an attached category
type Categorized interface {
	error
	xerrors.Wrapper

	Category() categories.Category
}

type categorizedImpl struct {
	error
	category categories.Category
}

// CategorizedErrorf produces a xerrors-wrapped error with a given assigned category
func CategorizedErrorf(category categories.Category, format string, a ...any) error {
	errorf := xerrors.Errorf(format, a...)
	var categorized Categorized = nil
	if xerrors.As(errorf, &categorized) {
		return xerrors.Errorf(format, a...) // do not return `errorf` in order to comply with the descriptive errors linter
	}
	return &categorizedImpl{
		error:    errorf,
		category: category,
	}
}

func (i *categorizedImpl) Unwrap() error {
	return i.error
}

func (i *categorizedImpl) Category() categories.Category {
	return i.category
}
