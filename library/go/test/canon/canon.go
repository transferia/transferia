package canon

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/transferria/transferria/library/go/test/yatest"
)

var isRunningUnderGotest bool

type canonObject struct {
	TestName string      `json:"test_name"`
	Data     interface{} `json:"data"`
}

type fileObject struct {
	Local    *bool    `json:"local,omitempty"`
	URI      string   `json:"uri"`
	DiffTool []string `json:"diff_tool,omitempty"`
}

type Option interface {
	isOption()
}

type diffToolOption struct {
	diffTool []string
}

func (diffToolOption) isOption() {}

func WithDiffTool(diffTool ...string) Option {
	return diffToolOption{diffTool: diffTool}
}

type isLocalOption struct {
	isLocal bool
}

func (isLocalOption) isOption() {}

func WithLocal(isLocal bool) Option {
	return isLocalOption{isLocal: isLocal}
}

func saveObject(t *testing.T, item interface{}) {
	var canonData canonObject
	canonData.TestName = t.Name()
	canonData.Data = item

	testName := strings.Replace(t.Name(), "/", ".", -1)
	canonDir := yatest.WorkPath("canon")
	pathToCanonFile := filepath.Join(canonDir, testName)

	if _, err := os.Stat(pathToCanonFile); err == nil || os.IsExist(err) {
		t.Fatalf("canon file already exists. Probably you are trying to canonize data several times in a test")
	}

	jsonData, err := json.Marshal(canonData)
	if err != nil {
		t.Fatalf("invalid json: %v", err)
	}

	if err := os.Mkdir(canonDir, 0777); err != nil && !os.IsExist(err) {
		t.Fatalf("failed to create canon directory: %v", err)
	}

	if err = os.WriteFile(pathToCanonFile, jsonData, 0666); err != nil {
		t.Fatalf("failed to write canon data to file: %v", err)
	}
}

func SaveJSON(t *testing.T, item interface{}) {
	if !isRunningUnderGotest {
		saveObject(t, item)
	}
}

func getCanonDestination(t *testing.T, canonFilename string) string {
	if _, err := os.Stat(canonFilename); os.IsNotExist(err) {
		t.Fatalf("file: %s does not exists", canonFilename)
	}

	tmpDir, err := os.MkdirTemp(yatest.BuildPath(""), "canon_tmp")
	if err != nil {
		t.Fatalf("failed to create tmp directory: %v", err)
	}

	dstFilename := filepath.Join(tmpDir, filepath.Base(canonFilename))
	return dstFilename
}

func SaveFile(t *testing.T, canonFilename string, opts ...Option) {
	if isRunningUnderGotest {
		return
	}

	dstFilename := getCanonDestination(t, canonFilename)
	dst, err := os.Create(dstFilename)
	if err != nil {
		t.Fatalf("failed to create destination file %s: %v", dstFilename, err)
	}
	defer dst.Close()

	src, err := os.Open(canonFilename)
	if err != nil {
		t.Fatalf("failed to open %s: %v", canonFilename, err)
	}
	defer src.Close()

	_, err = io.Copy(dst, src)
	if err != nil {
		t.Fatalf("failed to copy file: %v", err)
	}

	uri := "file://" + dstFilename
	canonDescription := fileObject{URI: uri}
	for _, opt := range opts {
		switch v := opt.(type) {
		case diffToolOption:
			if canonDescription.DiffTool == nil {
				canonDescription.DiffTool = v.diffTool
			} else {
				t.Fatalf("option %#v already set", opt)
			}
		case isLocalOption:
			if canonDescription.Local == nil {
				canonDescription.Local = new(bool)
				*canonDescription.Local = v.isLocal
			} else {
				t.Fatalf("option %#v already set", opt)
			}
		default:
			t.Fatalf("unexpected option %#v", opt)
		}
	}
	saveObject(t, canonDescription)
}
