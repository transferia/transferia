package reader_error

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// TestWrapContractorReadStep_PreservesKind ensures step wrapping does not change ClassifyContractorError.
func TestWrapContractorReadStep_PreservesKind(t *testing.T) {
	cases := []struct {
		name string
		err  ReaderError
		kind ContractorErrorKind
	}{
		{"transport", NewReaderErrorTransport("op", "f", xerrors.New("e")), ContractorKindTransport},
		{"data", NewReaderErrorDataRecord("op", "f", 1, xerrors.New("e")), ContractorKindData},
		{"config", NewReaderErrorConfig("op", xerrors.New("e")), ContractorKindConfig},
		{"sink", NewReaderErrorSink("op", "f", xerrors.New("e")), ContractorKindSink},
		{"fatal", NewReaderErrorFatal("op", xerrors.New("e")), ContractorKindFatal},
		{"nofiles", NewReaderErrorNoFiles("op", "pfx"), ContractorKindNoFiles},
		{"nosuchfile", NewReaderErrorNoSuchFile("op", "f"), ContractorKindNoSuchFile},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w1 := WrapContractorReadStep("contractor.ResolveSchema", tc.err)
			require.Equal(t, tc.kind, ClassifyContractorError(w1))
			w2 := WrapContractorReadStep("contractor.Read", tc.err)
			require.Equal(t, tc.kind, ClassifyContractorError(w2))
		})
	}
}

func TestAsReaderErrorNoFiles_XerrorsWrap(t *testing.T) {
	nf := NewReaderErrorNoFiles("op", "pfx")
	err := xerrors.Errorf("outer: %w", WrapContractorReadStep("contractor.ResolveSchema", nf))
	out, ok := AsReaderErrorNoFiles(err)
	require.True(t, ok)
	require.Equal(t, "pfx", out.Prefix())
}

func TestReaderErrorFromPush_FatalClassifiesAsFatal(t *testing.T) {
	err := abstract.NewFatalError(xerrors.New("boom"))
	re := ReaderErrorFromPush("pusher.Push", "f", err)
	require.Equal(t, ContractorKindFatal, ClassifyContractorError(re))
}
