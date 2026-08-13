package source

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultYDSSourceModel(t *testing.T) {
	model := new(YDSSource)
	model.WithDefaults()
	require.NoError(t, model.Validate())
}
