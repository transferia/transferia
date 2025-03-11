package benchxerrors

import (
	"fmt"
	"testing"

	"github.com/transferria/transferria/library/go/core/xerrors/internal/modes"
)

func RunPerMode(b *testing.B, bench func(b *testing.B)) {
	for _, mode := range modes.KnownStackTraceModes() {
		b.Run(fmt.Sprintf("Mode%s", mode), func(b *testing.B) {
			modes.SetStackTraceMode(mode)
			bench(b)
		})
	}
}
