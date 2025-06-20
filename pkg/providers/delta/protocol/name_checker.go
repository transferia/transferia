//go:build !disable_delta_provider

package protocol

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

var (
	checkpointFilePattern = regexp.MustCompile(`\d+\.checkpoint(\.\d+\.\d+)?\.parquet`)
	deltaFilePattern      = regexp.MustCompile(`\d+\.json`)
)

func DeltaFile(path string, version int64) string {
	return path + fmt.Sprintf("%020d.json", version)
}

func CheckpointVersion(path string) (int64, error) {
	path = filepath.Base(path)
	return strconv.ParseInt(strings.Split(path, ".")[0], 10, 64)
}

func IsCheckpointFile(path string) bool {
	path = filepath.Base(path)
	return checkpointFilePattern.MatchString(path)
}

func LogVersion(path string) (int64, error) {
	path = filepath.Base(path)
	return strconv.ParseInt(strings.TrimSuffix(path, ".json"), 10, 64)
}

func IsDeltaFile(path string) bool {
	path = filepath.Base(path)
	ret := deltaFilePattern.MatchString(path)
	return ret
}

func GetFileVersion(path string) (int64, error) {
	if IsCheckpointFile(path) {
		v, err := CheckpointVersion(path)
		if err != nil {
			return 0, xerrors.Errorf("unable to parse checkpoint version: %s: %w", path, err)
		}
		return v, nil
	} else if IsDeltaFile(path) {
		v, err := LogVersion(path)
		if err != nil {
			return 0, xerrors.Errorf("unable to parse log version: %s: %w", path, err)
		}
		return v, nil
	} else {
		return -1, xerrors.Errorf("unexpected file type: %s", path)
	}
}

func NumCheckpointParts(path string) (int, error) {
	path = filepath.Base(path)
	segments := strings.Split(path, ".")
	if len(segments) != 5 {
		return 0, nil
	}

	// name should contain {VERSION}.checkpoint.{INDEX}.{NUM_PARTS}.format
	// so we need to parse 4th part of name (NUM_PARTS)
	n, err := strconv.ParseInt(segments[3], 10, 32)
	return int(n), err
}

func CheckpointPrefix(path string, version int64) string {
	return path + fmt.Sprintf("%020d.checkpoint", version)
}

func CheckpointFileSingular(dir string, version int64) string {
	return dir + fmt.Sprintf("%020d.checkpoint.parquet", version)
}

func CheckpointFileWithParts(dir string, version int64, numParts int) []string {
	res := make([]string, numParts)
	for i := 1; i < numParts+1; i++ {
		res[i-1] = dir + fmt.Sprintf("%020d.checkpoint.%010d.%010d.parquet", version, i, numParts)
	}
	return res
}
