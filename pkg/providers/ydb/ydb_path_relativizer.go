//go:build !disable_ydb_provider

package ydb

import (
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
)

const (
	YDBRelativePathTransformerType = "ydb-path-relativizer-transformer"
)

type YDBPathRelativizerTransformer struct {
	Paths []string
}

func (r *YDBPathRelativizerTransformer) Type() abstract.TransformerType {
	return YDBRelativePathTransformerType
}

func makePathsTrailingSlashVariants(path string) (withTrailingSlash, withoutTrailingSlash string) {
	withoutTrailingSlash = strings.TrimRight(path, "/")
	withTrailingSlash = withoutTrailingSlash + "/"
	return withTrailingSlash, withoutTrailingSlash
}

func (r *YDBPathRelativizerTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)
	for _, changeItem := range input {
		changeItem.Table = MakeYDBRelPath(false, r.Paths, changeItem.Table)
		transformed = append(transformed, changeItem)
	}

	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      errors,
	}
}

func (r *YDBPathRelativizerTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return true
}

func (r *YDBPathRelativizerTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (r *YDBPathRelativizerTransformer) Description() string {
	return "YDB relative path transformer"
}

func NewYDBRelativePathTransformer(paths []string) *YDBPathRelativizerTransformer {
	return &YDBPathRelativizerTransformer{
		Paths: paths,
	}
}
