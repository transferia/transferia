package diff

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/util"
	"golang.org/x/exp/constraints"
)

// TODO can be removed after go1.21
func min[T constraints.Ordered](x T, y ...T) T {
	result := x
	for _, y := range y {
		if y < result {
			result = y
		}
	}
	return result
}

// LineDiff takes two strings and calculates diff in terms of lines that came from a or from b.
// It calculates DiffItems, which is array of []DiffItem -- an array of minimal length that
// describes what lines came from a, what lines came from b, and what lines are common.
// Further DiffItems can be converted to string with AsString method.
func LineDiff(a, b string) (DiffItems, error) {
	return Diff(strings.Split(a, "\n"), strings.Split(b, "\n"))
}

// DiffItems is array of diff items, that has useful `AsString` method which can give you a
// readable format. You can use default String method if you want default settings
type DiffItems []DiffItem

// String converts DiffItems to string with AsString method with default string conversion options
func (d DiffItems) String() string {
	return d.AsString(nil)
}

// AsString converts array of DiffItem into single string. You can control the
// result with parameter `options *DiffItemsAsStringOptions`, or set it to null
// for default configuration
func (d DiffItems) AsString(options *diffItemsAsStringOptions) string {
	opts := util.Coalesce(options, makeDiffAsStringOptions())
	items := yslices.Filter(d, func(item DiffItem) bool {
		for _, diffSourceExcluded := range opts.DiffSourceExcludeFilter {
			if item.From == diffSourceExcluded {
				return false
			}
		}
		return true
	})
	itemsStrings := yslices.Map(items, func(item DiffItem) string {
		switch item.From {
		case DiffSourceBoth:
			return opts.CommonPrefix + item.Line
		case DiffSourceLeft:
			return opts.FromLeftPrefix + item.Line
		case DiffSourceRight:
			return opts.FromRightPrefix + item.Line
		default:
			return item.Line
		}
	})
	return strings.Join(itemsStrings, opts.JoinWith)
}

type diffItemsAsStringOptions struct {
	// DiffSourceExcludeFilter represent slice of changes that should persist in final string
	DiffSourceExcludeFilter []DiffSource
	// CommonPrefix describes prefix appended to strings common from left and right part
	CommonPrefix string
	// FromLeftPrefix describes prefix appended to strings that came from left part
	FromLeftPrefix string
	// FromRightPrefix describes prefix appended to strings that came from right part
	FromRightPrefix string
	// JoinWith represents string that used to join diff items together, most commonly, new line symbol \n
	JoinWith string
}

// DiffOnlySources is used when you want to get only diff lines
func (l *diffItemsAsStringOptions) DiffOnlySources() *diffItemsAsStringOptions {
	l.DiffSourceExcludeFilter = []DiffSource{DiffSourceBoth}
	return l
}

// CommonOnlySources is used when you want to get only common lines
func (l *diffItemsAsStringOptions) CommonOnlySources() *diffItemsAsStringOptions {
	l.DiffSourceExcludeFilter = []DiffSource{DiffSourceLeft, DiffSourceRight}
	return l
}

// AllSources is used when you want to get all lines: diff and common
func (l *diffItemsAsStringOptions) AllSources() *diffItemsAsStringOptions {
	l.DiffSourceExcludeFilter = nil
	return l
}

func makeDiffAsStringOptions() diffItemsAsStringOptions {
	return diffItemsAsStringOptions{
		DiffSourceExcludeFilter: nil,
		CommonPrefix:            " ",
		FromLeftPrefix:          "-",
		FromRightPrefix:         "+",
		JoinWith:                "\n",
	}
}

func DiffAsStringOptions() *diffItemsAsStringOptions {
	res := makeDiffAsStringOptions()
	return &res
}

type DiffSource string

const (
	DiffSourceBoth  DiffSource = "both"
	DiffSourceLeft  DiffSource = "left"
	DiffSourceRight DiffSource = "right"
)

type DiffItem struct {
	Line string
	From DiffSource
}

// Diff takes two array of strings of arbitrary size and returns array of DiffItem.
// Each diff item repesents a Line and it's purpose -- added from left string array,
// added from right string, or equal in both left and right string arrays.
func Diff(a, b []string) (diffItems DiffItems, err error) {
	// no panic policy: convert panic (if here would be any panic) to returnable error.
	defer func() {
		if r := recover(); r != nil {
			err = xerrors.Errorf("error occurred while calculating diff: %v", r)
		}
	}()
	// calculating levenshtein distance in add-delete case
	D := make([][]int, len(a))
	for i := range D {
		D[i] = make([]int, len(b))
	}
	getD := func(i, j int) int {
		if i >= 0 && j >= 0 {
			return D[i][j]
		}
		return i + j + 2
	}
	for i := range D {
		for j := range D[i] {
			D[i][j] = min(getD(i-1, j)+1, getD(i, j-1)+1)
			if a[i] == b[j] {
				D[i][j] = min(D[i][j], getD(i-1, j-1)+1)
			}
		}
	}

	diffSize := getD(len(a)-1, len(b)-1)
	i := len(a) - 1
	j := len(b) - 1
	result := make([]DiffItem, diffSize)
	for d := diffSize - 1; d >= 0; d-- {
		if j >= 0 && (i >= 0 && getD(i, j-1)+1 == D[i][j] || i < 0) {
			result[d] = DiffItem{
				Line: b[j],
				From: DiffSourceRight,
			}
			j--
			continue
		}
		if i >= 0 && (j >= 0 && getD(i-1, j)+1 == D[i][j] || j < 0) {
			result[d] = DiffItem{
				Line: a[i],
				From: DiffSourceLeft,
			}
			i--
			continue
		}
		if i >= 0 && j >= 0 && getD(i-1, j-1)+1 == D[i][j] {
			result[d] = DiffItem{
				Line: a[i],
				From: DiffSourceBoth,
			}
			i--
			j--
			continue
		}
	}
	return result, nil
}
