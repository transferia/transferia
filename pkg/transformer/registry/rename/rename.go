package rename

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/transformer"
	"go.ytsaurus.tech/library/go/core/log"
)

const RenameTablesTransformerType = abstract.TransformerType("rename_tables")

func init() {
	transformer.Register[Config](RenameTablesTransformerType, func(cfg Config, lgr log.Logger, runtime abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		return NewRenameTableTransformer(cfg), nil
	})
}

type Config struct {
	RenameTables []RenameTable `json:"renameTables"`
}

type Table struct {
	Name      string `json:"name"`
	Namespace string `json:"nameSpace"`
}

func (t Table) ID() abstract.TableID {
	return abstract.TableID{Namespace: t.Namespace, Name: t.Name}
}

type RenameTable struct {
	OriginalName Table `json:"originalName"`
	NewName      Table `json:"newName"`
}

type RenameTableTransformer struct {
	AltNames map[abstract.TableID]abstract.TableID
}

func (r *RenameTableTransformer) Type() abstract.TransformerType {
	return RenameTablesTransformerType
}

func (r *RenameTableTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	transformed := make([]abstract.ChangeItem, 0)
	errors := make([]abstract.TransformerError, 0)
	for _, chI := range input {
		if r.Suitable(chI.TableID(), chI.TableSchema) {
			altName := r.AltNames[chI.TableID()]
			chI.Schema = altName.Namespace
			chI.Table = altName.Name
		}
		transformed = append(transformed, chI)
	}

	return abstract.TransformerResult{
		Transformed: transformed,
		Errors:      errors,
	}
}

func (r *RenameTableTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	_, hasAltName := r.AltNames[table]
	return hasAltName
}

func (r *RenameTableTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (r *RenameTableTransformer) Description() string {
	renames := make([]string, len(r.AltNames))
	i := 0
	for from, to := range r.AltNames {
		renames[i] = fmt.Sprintf("%s->%s", from.Fqtn(), to.Fqtn())
		i++
	}
	return fmt.Sprintf("Rename tables: %s", strings.Join(renames, ", "))
}

func NewRenameTableTransformer(config Config) *RenameTableTransformer {
	altNames := make(map[abstract.TableID]abstract.TableID)
	for _, rename := range config.RenameTables {
		altNames[rename.OriginalName.ID()] = rename.NewName.ID()
	}
	return &RenameTableTransformer{
		AltNames: altNames,
	}
}
