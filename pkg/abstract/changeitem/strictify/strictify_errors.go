package strictify

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"go.ytsaurus.tech/yt/go/schema"
)

// StrictifyError is a general strictification error. It contains the column name and the target type name.
type StrictifyError struct{ error }

func NewStrictifyError(sch *changeitem.ColSchema, strictType schema.Type, err error) *StrictifyError {
	return &StrictifyError{
		error: xerrors.Errorf("unable to strictify the value of column %s (original type %q) to type %s: %w", sch.ColumnName, sch.OriginalType, strictType.String(), err),
	}
}

func (e StrictifyError) Error() string {
	return e.error.Error()
}

func (e StrictifyError) Is(err error) bool {
	_, ok := err.(*StrictifyError)
	return ok
}

func (e StrictifyError) Unwrap() error {
	return e.error
}

var StrictifyRangeError error = xerrors.NewSentinel("value is outside of the allowed range for the strict type")
