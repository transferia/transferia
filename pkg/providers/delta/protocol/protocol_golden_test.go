//go:build !disable_delta_provider

package protocol

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	store "github.com/transferia/transferia/pkg/providers/delta/store"
	"github.com/transferia/transferia/pkg/util/set"
)

// badGoldenTest it's a set of cases that doomed to fail
// that was designed that way
var badGoldenTest = set.New(
	"versions-not-contiguous",
	"deltalog-state-reconstruction-without-protocol",
	"deltalog-state-reconstruction-without-metadata",
	"data-reader-absolute-paths-escaped-chars",
)

func TestGoldenDataSet(t *testing.T) {
	golden, err := os.ReadDir("golden")
	require.NoError(t, err)
	for _, entry := range golden {
		if badGoldenTest.Contains(entry.Name()) {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			path, err := filepath.Abs("golden/" + entry.Name())
			require.NoError(t, err)
			readDir(t, path)
		})
	}
}

func readDir(t *testing.T, path string) {
	subF, err := os.ReadDir(path)
	require.NoError(t, err)
	isDelatLog := false
	for _, f := range subF {
		if f.IsDir() && f.Name() == "_delta_log" {
			isDelatLog = true
		}
	}
	if !isDelatLog {
		for _, f := range subF {
			if f.IsDir() {
				if badGoldenTest.Contains(f.Name()) {
					continue
				}
				t.Run(f.Name(), func(t *testing.T) {
					readDir(t, path+"/"+f.Name())
				})
			}
		}
		return
	}

	st, err := store.New(&store.LocalConfig{Path: path})
	require.NoError(t, err)
	table, err := NewTableLog(path, st)
	require.NoError(t, err)

	snapshot, err := table.Snapshot()
	require.NoError(t, err)

	version := snapshot.Version()
	logger.Log.Infof("versions: %v", version)
	meta, err := snapshot.Metadata()
	require.NoError(t, err)
	logger.Log.Infof("format %v", meta.Format.Provider)
	schema, err := meta.DataSchema()
	if err != nil {
		require.NoError(t, err)
	}

	for _, f := range schema.GetFields() {
		logger.Log.Infof("	%s (%s) %v", f.Name, f.DataType.Name(), f.Nullable)
	}

	files, err := snapshot.AllFiles()
	require.NoError(t, err)
	for _, f := range files {
		logger.Log.Info(f.Path)
	}
}
