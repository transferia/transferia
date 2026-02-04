package copy

import (
	"context"
	"os"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	client2 "github.com/transferia/transferia/pkg/abstract/coordinator"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/copy/target"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	SrcYT        = os.Getenv("YT_PROXY_SRC")
	DstYT        = os.Getenv("YT_PROXY_DST")
	Source       = yt2.YtSource{
		Cluster: "src",
		YtProxy: SrcYT,
		Paths: []string{
			"//a",
			"//nested/test/b",
			"//test_dir",
			"//nested/test/dir",
		},
		YtToken: "",
	}
	Target = yt2.YtCopyDestination{
		Cluster:            DstYT,
		YtToken:            "",
		Prefix:             "//dst_pref",
		Parallelism:        2,
		UsePushTransaction: true,
		Pool:               "default",
	}
)

type row struct {
	Key   int    `yson:"key"`
	Value string `yson:"value"`
}

type ytTbl struct {
	InPath  string
	OutPath string
	Data    []row
}

func initSrcData(srcEnv *yttest.Env, data []ytTbl) error {
	for _, tbl := range data {
		p, err := ypath.Parse(tbl.InPath)
		if err != nil {
			return xerrors.Errorf("error in test input data: error parsing path %s: %w", tbl.InPath, err)

		}

		pref, _, err := ypath.Split(p.YPath())
		if err != nil {
			return xerrors.Errorf("error splitting path %s: %w", tbl.InPath, err)
		}
		if _, err = srcEnv.YT.CreateNode(context.Background(), pref, yt.NodeMap, &yt.CreateNodeOptions{
			Recursive:      true,
			IgnoreExisting: true,
		}); err != nil {
			return xerrors.Errorf("error creating directory for %s: %w", tbl.InPath, err)
		}

		if err := srcEnv.UploadSlice(p, tbl.Data); err != nil {
			return xerrors.Errorf("error uploading test data for table %s: %w", tbl.InPath, err)
		}
	}
	return nil
}

func checkDstData(dstEnv *yttest.Env, data []ytTbl) error {
	for _, tbl := range data {
		p, err := ypath.Parse(tbl.OutPath)
		if err != nil {
			return xerrors.Errorf("error in test input data: error parsing path %s: %w", tbl.OutPath, err)

		}

		inLen := len(tbl.Data)
		if err := dstEnv.DownloadSlice(p, &tbl.Data); err != nil {
			return xerrors.Errorf("error downloading test data for table %s: %w", tbl.OutPath, err)
		}
		outLen := len(tbl.Data)

		if inLen*2 != outLen {
			return xerrors.Errorf("tbl %s: expected %d rows of has been copied, got %d", tbl.OutPath, inLen, outLen-inLen)
		}

		for i := 0; i < inLen; i++ {
			if !reflect.DeepEqual(tbl.Data[i], tbl.Data[i+inLen]) {
				return xerrors.Errorf("tbl %s: expected input row %d (%v) equal to output %d (%v)",
					tbl.OutPath, i, tbl.Data[i], i+inLen, tbl.Data[i+inLen])
			}
		}
	}
	return nil
}

func TestYTHomoProvider(t *testing.T) {
	Source.WithDefaults()
	Target.WithDefaults()
	srcYT := os.Getenv("YT_PROXY_SRC")
	dstYT := os.Getenv("YT_PROXY_DST")
	srcYTEnv := yttest.New(t, yttest.WithConfig(yt.Config{Proxy: srcYT}), yttest.WithLogger(logger.Log.Structured()))
	dstYTEnv := yttest.New(t, yttest.WithConfig(yt.Config{Proxy: dstYT}), yttest.WithLogger(logger.Log.Structured()))

	testData := []ytTbl{
		{
			InPath:  "//a",
			OutPath: "//dst_pref/a",
			Data: []row{
				{1, "A1"},
				{2, "A2"},
			},
		},
		{
			InPath:  "//nested/test/b",
			OutPath: "//dst_pref/b",
			Data: []row{
				{1, "B1"},
				{2, "B2"},
			},
		},
		{
			InPath:  "//test_dir/c",
			OutPath: "//dst_pref/c",
			Data: []row{
				{1, "C1"},
				{2, "C2"},
			},
		},
		{
			InPath:  "//test_dir/nested/d",
			OutPath: "//dst_pref/nested/d",
			Data: []row{
				{1, "D1"},
				{2, "D2"},
			},
		},
		{
			InPath:  "//test_dir/nested/deep/e",
			OutPath: "//dst_pref/nested/deep/e",
			Data: []row{
				{1, "E1"},
				{2, "E2"},
			},
		},
		{
			InPath:  "//nested/test/dir/f",
			OutPath: "//dst_pref/f",
			Data: []row{
				{1, "F1"},
				{2, "F2"},
			},
		},
		{
			InPath:  "//nested/test/dir/deep/g",
			OutPath: "//dst_pref/deep/g",
			Data: []row{
				{1, "G1"},
				{2, "G2"},
			},
		},
	}

	err := initSrcData(srcYTEnv, testData)
	require.NoError(t, err, "Error initializing data in source YT")

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	snapshotLoader := tasks.NewSnapshotLoader(client2.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	require.NoError(t, snapshotLoader.UploadV2(context.Background(), nil, nil))

	err = checkDstData(dstYTEnv, testData)
	require.NoError(t, err, "Error checking destination data")
}

func uploadSlice(t *testing.T, ytEnv *yttest.Env, path string, data []row) {
	ytpath, err := ypath.Parse(path)
	require.NoError(t, err)
	require.NoError(t, ytEnv.UploadSlice(ytpath, data))
}

// checkPathsHaveContentRevisionAttr fails if at least one of provided paths does not have ContentRevisionAttr.
func checkPathsHaveContentRevisionAttr(t *testing.T, cl yt.Client, paths []string) {
	for _, path := range paths {
		ytpath, err := ypath.Parse(path)
		require.NoError(t, err)
		attrPath := ytpath.Child("@" + target.ContentRevisionAttr)
		var rev int64
		require.NoError(t, cl.GetNode(t.Context(), attrPath, &rev, nil))
		require.Greater(t, rev, int64(0))
	}
}

// getTablesUpdatedAt returns modification_time attribute for each path.
func getTablesUpdatedAt(t *testing.T, cl yt.Client, paths []string) []any {
	times := make([]any, len(paths))
	for i, path := range paths {
		ytpath, err := ypath.Parse(path)
		require.NoError(t, err)
		attrPath := ytpath.Child("@modification_time")
		require.NoError(t, cl.GetNode(t.Context(), attrPath, &times[i], nil))
	}
	return times
}

func TestYTCopySkipUnchangedTables(t *testing.T) {
	Source.WithDefaults()
	Target.WithDefaults()
	Target.SkipUnchangedTables = true
	srcYT := os.Getenv("YT_PROXY_SRC")
	dstYT := os.Getenv("YT_PROXY_DST")
	require.NotEmpty(t, srcYT)
	require.NotEmpty(t, dstYT)
	srcYTEnv := yttest.New(t, yttest.WithConfig(yt.Config{Proxy: srcYT}), yttest.WithLogger(logger.Log.Structured()))
	dstYTEnv := yttest.New(t, yttest.WithConfig(yt.Config{Proxy: dstYT}), yttest.WithLogger(logger.Log.Structured()))

	// Four tables: a, b — unchanged between runs; c, d — we edit on source before second run.
	testData := []ytTbl{
		{InPath: "//a", OutPath: "//dst_pref/a", Data: []row{{1, "A1"}, {2, "A2"}}},
		{InPath: "//nested/test/b", OutPath: "//dst_pref/b", Data: []row{{1, "B1"}, {2, "B2"}}},
		{InPath: "//test_dir/c", OutPath: "//dst_pref/c", Data: []row{{1, "C1"}, {2, "C2"}}},
		{InPath: "//test_dir/d", OutPath: "//dst_pref/d", Data: []row{{1, "D1"}, {2, "D2"}}},
	}
	afterUpdateDataC := []row{{1, "C1_updated"}, {2, "C2_updated"}}
	afterUpdateDataD := []row{{1, "D1_updated"}, {2, "D2_updated"}}

	unchangedIndices := []int{0, 1} // a, b — not edited
	changedIndices := []int{2, 3}   // c, d — edit on source before second run
	outPaths := make([]string, 0, len(testData))
	for _, tbl := range testData {
		outPaths = append(outPaths, tbl.OutPath)
	}

	require.NoError(t, initSrcData(srcYTEnv, testData))
	initialRowCounts := make([]int, len(testData))
	for i := range testData {
		initialRowCounts[i] = len(testData[i].Data)
	}
	transfer := helpers.MakeTransfer(helpers.TransferID+"-skip-unchanged", &Source, &Target, TransferType)
	snapshotLoader := tasks.NewSnapshotLoader(client2.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())

	// First run: copy all tables.
	require.NoError(t, snapshotLoader.UploadV2(t.Context(), nil, nil))
	require.NoError(t, checkDstData(dstYTEnv, testData))
	checkPathsHaveContentRevisionAttr(t, dstYTEnv.YT, outPaths)
	firstUpdatedAt := getTablesUpdatedAt(t, dstYTEnv.YT, outPaths)

	// Edit source tables that should be re-copied (c and d).
	uploadSlice(t, srcYTEnv, testData[2].InPath, afterUpdateDataC)
	uploadSlice(t, srcYTEnv, testData[3].InPath, afterUpdateDataD)

	// Second run: unchanged (a, b) must be skipped; changed (c, d) must be re-copied.
	require.NoError(t, snapshotLoader.UploadV2(t.Context(), nil, nil))
	secondUpdatedAt := getTablesUpdatedAt(t, dstYTEnv.YT, outPaths)
	for _, i := range unchangedIndices {
		require.Equal(t, firstUpdatedAt[i], secondUpdatedAt[i], outPaths[i])
	}
	for _, i := range changedIndices {
		require.NotEqual(t, firstUpdatedAt[i], secondUpdatedAt[i], outPaths[i])
	}

	// Expected on dst after second run: a and b unchanged, c and d with new data.
	expectedAfterSecond := slices.Clone(testData)
	expectedAfterSecond[2].Data = afterUpdateDataC
	expectedAfterSecond[3].Data = afterUpdateDataD
	require.NoError(t, checkDstData(dstYTEnv, expectedAfterSecond))
}
