package coordinator

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"
)

func TestIsProlongableWith(t *testing.T) {
	var statusMessage1, statusMessage2 StatusMessage
	_ = gofakeit.Struct(&statusMessage1)
	_ = gofakeit.Struct(&statusMessage2)

	require.False(t, statusMessage1.IsProlongableWith(&statusMessage2), "gofakeit should produce different status messages")

	// copy fields known to be significant for prolongability
	statusMessage1.Heading = statusMessage2.Heading
	statusMessage1.Message = statusMessage2.Message
	statusMessage1.Categories = statusMessage2.Categories
	slices.SortFunc(statusMessage1.Categories, func(_, _ string) int { return rand.Intn(3) - 1 })
	statusMessage1.Code = statusMessage2.Code
	statusMessage1.Type = statusMessage2.Type

	require.True(t, statusMessage1.IsProlongableWith(&statusMessage2), "setting key fields that identifies prolongability should make messaages prolongable")
}
