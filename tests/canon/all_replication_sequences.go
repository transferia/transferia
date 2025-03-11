package canon

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

// SequenceTestCases contains a list of all sequence test cases. Each new case must be added here
var SequenceTestCases = []string{
	"insert_update_delete",
	"updatepk",
	"insert_update_insert",
}

type CanonizedSequenceCase struct {
	Name   string
	Tables abstract.TableMap
	Items  []abstract.ChangeItem
}

func AllReplicationSequences() []CanonizedSequenceCase {
	result := make([]CanonizedSequenceCase, 0)
	for _, name := range SequenceTestCases {
		sCase := decodeSequenceTestCase(name)
		if sCase == nil {
			panic(fmt.Sprintf("failed to obtain sequence %s", name))
		}
		result = append(result, *sCase)
	}
	return result
}

//go:embed sequences/canondata/*/extracted
var sequencesCanonized embed.FS

// decodeSequenceTestCase returns a properly initialized canonized sequence test case. The name must not be an empty string.
//
// In case of an error, nil is returned.
func decodeSequenceTestCase(name string) *CanonizedSequenceCase {
	result := &CanonizedSequenceCase{
		Name:   name,
		Tables: make(abstract.TableMap),
		Items:  nil,
	}

	if err := fs.WalkDir(sequencesCanonized, "sequences", func(path string, d fs.DirEntry, err error) error {
		logger.Log.Warnf("Walking entry %s", path)
		if err != nil {
			return xerrors.Errorf("failed to walk directory %s: %w", path, err)
		}
		if d == nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}

		if filepath.Base(path) != "extracted" {
			return nil
		}
		thisCaseName := extractCanonizedCaseNameFromPath(path)
		if name != thisCaseName {
			return nil
		}

		data, err := fs.ReadFile(sequencesCanonized, path)
		if err != nil {
			return xerrors.Errorf("failed to read file %s: %w", path, err)
		}

		var typedRows []abstract.TypedChangeItem
		if err := json.Unmarshal(data, &typedRows); err != nil {
			logger.Log.Errorf("unable to parse test case %s: %v", path, err)
		}
		var rows []abstract.ChangeItem
		for _, typedRow := range typedRows {
			rows = append(rows, abstract.ChangeItem(typedRow))
		}
		result.Items = append(result.Items, rows...)
		return nil
	}); err != nil {
		logger.Log.Fatalf("unable to walk all extracted stuff: %v", err)
	}

	if len(result.Items) == 0 {
		return nil
	}

	for i := range result.Items {
		item := &result.Items[i]
		if _, ok := result.Tables[item.TableID()]; !ok {
			result.Tables[item.TableID()] = abstract.TableInfo{
				EtaRow: 1,
				IsView: false,
				Schema: item.TableSchema,
			}
		}
	}

	return result
}

var canonRe *regexp.Regexp = regexp.MustCompile(`TestCanonizeSequences_(.*)_canon`)

func extractCanonizedCaseNameFromPath(path string) string {
	canonDirName := filepath.Base(filepath.Dir(path))
	if !canonRe.MatchString(canonDirName) {
		return ""
	}
	sm := canonRe.FindStringSubmatch(canonDirName)
	if len(sm) != 2 {
		return ""
	}
	return sm[1]
}

// AllSubsequences produces all possible subsequences from the given sequence which all have the common beginning and start from the first item in the given sequence.
// Non-row items do not form sequences which begin from them or end with them. Instead, they are "attached" to the row items that follow them.
func AllSubsequences(sequence []abstract.ChangeItem) [][]abstract.ChangeItem {
	result := make([][]abstract.ChangeItem, 0)
	for i := range sequence {
		if !sequence[i].IsRowEvent() {
			continue
		}
		subseqLength := i + 1
		result = append(result, make([]abstract.ChangeItem, subseqLength))
		copy(result[i], sequence[:subseqLength])
	}
	return result
}
