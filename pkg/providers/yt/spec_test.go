//go:build !disable_yt_provider

package yt

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCast(t *testing.T) {
	canonValue := make(map[string]interface{})
	canonValue["root_long"] = int64(1000)
	canonValue["root_float"] = 100.5
	canonValue["label"] = "root"
	canonValue["nested"] = map[string]interface{}{"long": int64(1000), "float": 100.5, "label": "nested"}

	canonYtSpec := YTSpec{canonValue}

	encoded, _ := json.Marshal(canonYtSpec)

	var decoded YTSpec
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	assert.Equal(t, canonValue, decoded.GetConfig())
}
