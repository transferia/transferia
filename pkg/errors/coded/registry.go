package coded

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util/set"
)

// Code define provider defined stable code. Each provider has own code-registry, but we have global registry to dedup them
// in case we have duplicate we will panic at start
type Code string

func (c Code) ID() string {
	return string(c)
}

func (c Code) Contains(err error) bool {
	var codedErr CodedError
	unwrappedErr := err
	for xerrors.As(unwrappedErr, &codedErr) {
		if codedErr.Code() == c {
			return true
		}
		unwrappedErr = xerrors.Unwrap(codedErr)
	}
	return false
}

var knownCodes = set.New[Code]()

func Register(parts ...string) Code {
	code := Code(strings.Join(parts, "."))
	if knownCodes.Contains(code) {
		panic(fmt.Sprintf("code: %s already registered", code))
	}
	knownCodes.Add(code)
	return code
}

func All() []Code {
	return knownCodes.SortedSliceFunc(func(a, b Code) bool {
		return a > b
	})
}
