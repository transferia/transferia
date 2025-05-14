package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectionHosts(t *testing.T) {
	// DTSUPPORT-5356
	srcModel := ChSource{
		MdbClusterID: "",
		ShardsList:   nil,
	}
	storageParams := srcModel.ToStorageParams()
	result, err := ConnectionHosts(storageParams, "")
	require.Error(t, err)
	require.Equal(t, len(result), 0)
}
