package filter

import (
	"regexp"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type Tables struct {
	IncludeTables []string `json:"includeTables" yaml:"include_tables"`
	ExcludeTables []string `json:"excludeTables" yaml:"exclude_tables"`
}

type Columns struct {
	IncludeColumns []string `json:"includeColumns" yaml:"include_columns"`
	ExcludeColumns []string `json:"excludeColumns" yaml:"exclude_columns"`
}

type Filter struct {
	IncludeRegexp []string `json:"includeRegexp" yaml:"include_regexp"`
	ExcludeRegexp []string `json:"excludeRegexp" yaml:"exclude_regexp"`

	compiledInclude []*regexp.Regexp
	compiledExclude []*regexp.Regexp
}

func (f *Filter) Match(value string) bool {
	for _, excludeRe := range f.compiledExclude {
		if matches := excludeRe.MatchString(value); matches {
			return false
		}
	}

	if len(f.IncludeRegexp) == 0 {
		return true
	}

	for _, includeRe := range f.compiledInclude {
		if matches := includeRe.MatchString(value); matches {
			return true
		}
	}
	return false
}

func (f *Filter) Empty() bool {
	return len(f.IncludeRegexp) == 0 && len(f.ExcludeRegexp) == 0
}

func NewFilter(IncludeRegexp, ExcludeRegexp []string) (Filter, error) {
	res := &Filter{
		IncludeRegexp:   IncludeRegexp,
		compiledInclude: nil,
		ExcludeRegexp:   ExcludeRegexp,
		compiledExclude: nil,
	}
	for _, reg := range IncludeRegexp {
		creg, err := regexp.Compile(reg)
		if err != nil {
			return *res, xerrors.Errorf("unable to compile include regexp: %s: %w", reg, err)
		}
		res.compiledInclude = append(res.compiledInclude, creg)
	}
	for _, reg := range ExcludeRegexp {
		creg, err := regexp.Compile(reg)
		if err != nil {
			return *res, xerrors.Errorf("unable to compile exclude regexp: %s: %w", reg, err)
		}
		res.compiledExclude = append(res.compiledExclude, creg)
	}
	return *res, nil
}
