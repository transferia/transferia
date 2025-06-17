//go:build !disable_clickhouse_provider

package clickhouse

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/providers/clickhouse"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpuploader"
	"github.com/transferia/transferia/pkg/transformer"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type columnMeta struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type resultStat struct {
	Elapsed   float64 `json:"elapsed"`
	RowsRead  int     `json:"rows_read"`
	BytesRead int     `json:"bytes_read"`
}

type jsonCompactResult struct {
	Meta       []columnMeta    `json:"meta"`
	Data       [][]interface{} `json:"data"`
	Rows       int             `json:"rows"`
	Statistics resultStat      `json:"statistics"`
}

const Type = abstract.TransformerType("sql")

func init() {
	transformer.Register[Config](Type, func(protoConfig Config, lgr log.Logger, _ abstract.TransformationRuntimeOpts) (abstract.Transformer, error) {
		return New(protoConfig, lgr)
	})
}

var (
	//go:embed README.md
	readme []byte
	_      model.Describable = (*Config)(nil)
)

type Config struct {
	Tables filter.Tables `json:"tables" yaml:"tables"`
	Query  string        `json:"query" yaml:"query,flow"`
}

func (c Config) Describe() model.Doc {
	return model.Doc{
		Usage: string(readme),
		Example: `
tables:                                                                       
	include_tables:                                                             
	- '"public"."included_data"'                                                
	exclude_tables:                                                             
	- '"public"."excluded_data"'                                                
query: |
	select
		* 
	from table
`,
	}
}

type ClickhouseTransformer struct {
	filter         filter.Filter
	query          string
	logger         log.Logger
	outputSchemas  map[abstract.TableID]*abstract.TableSchema
	engineMutex    sync.Mutex
	clickhousePath string
}

func (s *ClickhouseTransformer) Type() abstract.TransformerType {
	return Type
}

func (s *ClickhouseTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	s.engineMutex.Lock()
	defer s.engineMutex.Unlock()
	marshallingRules := httpuploader.NewRules(
		input[0].ColumnNames,
		input[0].TableSchema.Columns(),
		abstract.MakeMapColNameToIndex(input[0].TableSchema.Columns()),
		map[string]*columntypes.TypeDescription{},
		true,
	)
	var totalRes abstract.TransformerResult
	for _, subinput := range abstract.SplitUpdatedPKeys(input) {
		input = abstract.Collapse(subinput)
		buffers, err := s.prepareInput(input, marshallingRules)
		if err != nil {
			s.logger.Error("unable to prepare request", log.Error(err))
			var res abstract.TransformerResult
			res.Errors = allWithError(input, err)
			return res
		}
		var resChs []chan abstract.TransformerResult
		for range buffers {
			resChs = append(resChs, make(chan abstract.TransformerResult, 1))
		}
		for i, buffer := range buffers {
			go func(i int, buffer bytes.Buffer) {
				output, err := s.clickhouseExec(buffer, marshallingRules)
				if err != nil {
					s.logger.Error("unable to exec query\n"+util.TailSample(string(output), 256), log.Error(err))
					var res abstract.TransformerResult
					res.Errors = allWithError(input, err)
					resChs[i] <- res
					return
				}
				st := time.Now()
				resChs[i] <- s.parseOutput(input, output)
				s.logger.Infof("done parse in %s total data: %s", time.Since(st), format.SizeInt(len(output)))
			}(i, buffer)
		}
		for _, resCh := range resChs {
			res := <-resCh
			totalRes.Transformed = append(totalRes.Transformed, res.Transformed...)
			totalRes.Errors = append(totalRes.Errors, res.Errors...)
		}
	}
	return totalRes
}

func (s *ClickhouseTransformer) prepareInput(input []abstract.ChangeItem, marshallingRules *httpuploader.MarshallingRules) ([]bytes.Buffer, error) {
	rules := typesystem.RuleFor(clickhouse.ProviderType)
	var res []bytes.Buffer
	for _, col := range marshallingRules.ColSchema {
		chType, ok := rules.Target[ytschema.Type(col.DataType)]
		if !ok {
			s.logger.Errorf("col %s not found type %s in typesystem", col.ColumnName, col.DataType)
			return nil, xerrors.Errorf("col %s not found type %s in typesystem", col.ColumnName, col.DataType)
		}
		marshallingRules.SetColType(col.ColumnName, columntypes.NewTypeDescription(chType))
	}
	buffer := bytes.Buffer{}
	for _, row := range input {
		if !row.IsRowEvent() {
			continue
		}
		if row.IsToasted() {
			return nil, xerrors.New("toasted rows not supported")
		}
		if err := httpuploader.MarshalCItoJSON(row, marshallingRules, &buffer); err != nil {
			return nil, xerrors.Errorf("unalbe to marshal: %w", err)
		}
		if buffer.Len() > 1024*1024*5 {
			res = append(res, buffer)
			buffer = bytes.Buffer{}
		}
	}
	if buffer.Len() > 0 {
		res = append(res, buffer)
	}
	return res, nil
}

func (s *ClickhouseTransformer) clickhouseExec(buffer bytes.Buffer, marshallingRules *httpuploader.MarshallingRules) ([]byte, error) {
	rules := typesystem.RuleFor(clickhouse.ProviderType)
	var inputCols []string
	for _, col := range marshallingRules.ColSchema {
		chType, ok := rules.Target[ytschema.Type(col.DataType)]
		if !ok {
			return nil, xerrors.Errorf("col %s not found type %s in typesystem", col.ColumnName, col.DataType)
		}
		inputCols = append(inputCols, fmt.Sprintf("%s %s", col.ColumnName, chType))
	}
	if len(inputCols) == 0 {
		return nil, xerrors.New("empty table (zero columnt)")
	}
	inputStructure := strings.Join(inputCols, ",")
	st := time.Now()
	cmd := exec.Command(s.clickhousePath, "local", "--input-format", "JSONEachRow", "--output-format", "JSONCompact", "--structure", inputStructure, "--query", s.query, "--no-system-tables")
	cmd.Env = append(cmd.Env, "TZ=UTC")
	s.logger.Infof("exec: %s \n%s", format.SizeInt(buffer.Len()), strings.Join(cmd.Args, " "))
	cmd.Stdin = &buffer
	output, err := cmd.CombinedOutput()
	s.logger.Infof("done exec in %s total data: %s", time.Since(st), format.SizeInt(len(output)))
	return output, err
}

func (s *ClickhouseTransformer) parseOutput(input []abstract.ChangeItem, output []byte) abstract.TransformerResult {
	var res abstract.TransformerResult
	table := input[0].TableID()
	resSchema := s.outputSchemas[table]
	if len(resSchema.Columns()) == 0 {
		s.logger.Errorf("sql output for %s has no columns", table.String())
		res.Errors = allWithError(input, xerrors.Errorf("sql output for %s has no columns", table.String()))
		return res
	}
	if !resSchema.Columns().HasPrimaryKey() {
		s.logger.Errorf("table fit by table ID: %s, but has no PKey", table.String())
		res.Errors = allWithError(input, xerrors.Errorf("table fit by table ID: %s, but has no PKey", table.String()))
		return res
	}
	keyIdxs := s.extractKeyIndexes(input)
	var queryResult jsonCompactResult
	if err := json.Unmarshal(output, &queryResult); err != nil {
		s.logger.Error("unable to unmarshal result", log.Error(err))
		res.Errors = allWithError(input, err)
		return res
	}
	colNames := s.outputSchemas[input[0].TableID()].Columns().ColumnNames()
	for i, row := range queryResult.Data {
		var vals []interface{}
		var keyVals []string
		for j, rowCell := range row {
			col := s.outputSchemas[input[0].TableID()].Columns()[j]
			val := columntypes.Restore(col, rowCell)
			vals = append(vals, val)
			if col.IsKey() {
				keyVals = append(keyVals, fmt.Sprintf("%v", val))
			}
		}
		baseChangeItem := abstract.ChangeItem{
			ID:           0,
			LSN:          input[0].LSN,
			CommitTime:   input[0].CommitTime,
			Counter:      i,
			Kind:         abstract.InsertKind,
			Schema:       input[0].Schema,
			Table:        input[0].Table,
			PartID:       input[0].PartID,
			ColumnNames:  colNames,
			ColumnValues: vals,
			TableSchema:  s.outputSchemas[input[0].TableID()],
			OldKeys:      *new(abstract.OldKeysType),
			TxID:         "",
			Query:        "",
			Size:         abstract.EventSize{Read: 0, Values: 0},
		}
		k := strings.Join(keyVals, ",")
		origIdx, ok := keyIdxs[k]
		if !ok {
			res.Errors = append(res.Errors, abstract.TransformerError{
				Input: baseChangeItem,
				Error: xerrors.Errorf("unable to find original change for %v keys", keyVals),
			})
			continue
		}
		baseChangeItem.ID = input[origIdx].ID
		baseChangeItem.LSN = input[origIdx].LSN
		baseChangeItem.CommitTime = input[origIdx].CommitTime
		baseChangeItem.Counter = input[origIdx].Counter
		baseChangeItem.Kind = input[origIdx].Kind
		baseChangeItem.PartID = input[origIdx].PartID
		baseChangeItem.OldKeys = input[origIdx].OldKeys
		baseChangeItem.TxID = input[origIdx].TxID
		baseChangeItem.Query = input[origIdx].Query
		baseChangeItem.Size = input[origIdx].Size
		switch baseChangeItem.Kind {
		case abstract.DeleteKind, abstract.UpdateKind:
			baseChangeItem.OldKeys.KeyValues = baseChangeItem.ColumnValues
			baseChangeItem.OldKeys.KeyNames = baseChangeItem.ColumnNames
		}
		if baseChangeItem.Kind == abstract.DeleteKind {
			baseChangeItem.ColumnValues = nil
			baseChangeItem.ColumnNames = nil
		}
		res.Transformed = append(res.Transformed, baseChangeItem)
	}
	return res
}

func (s *ClickhouseTransformer) extractKeyIndexes(input []abstract.ChangeItem) map[string]int {
	res := map[string]int{}
	for i, row := range input {
		if !row.IsRowEvent() {
			continue
		}
		if row.Kind == abstract.DeleteKind {
			fc := row.TableSchema.FastColumns()
			// for delete kind we extract keys from old-values
			var keyVals []string
			for colIdx, colName := range row.OldKeys.KeyNames {
				if fc[abstract.ColumnName(colName)].PrimaryKey {
					keyVals = append(keyVals, fmt.Sprintf("%v", row.OldKeys.KeyValues[colIdx]))
				}
			}
			res[strings.Join(keyVals, ",")] = i
		} else {
			res[strings.Join(row.KeyVals(), ",")] = i
		}
	}
	return res
}

func allWithError(input []abstract.ChangeItem, err error) []abstract.TransformerError {
	var res []abstract.TransformerError
	for _, row := range input {
		res = append(res, abstract.TransformerError{
			Input: row,
			Error: err,
		})
	}
	return res
}

func (s *ClickhouseTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	ok := filter.MatchAnyTableNameVariant(s.filter, table)
	if !ok {
		s.logger.Info("table not fit by table ID, so skipped", log.String("table", table.Fqtn()))
		return false
	}
	resSchema, _ := s.ResultSchema(schema)
	if len(resSchema.Columns()) == 0 {
		s.logger.Warn("table fit by table ID, but has no columns", log.String("table", table.Fqtn()))
	}
	if !resSchema.Columns().HasPrimaryKey() {
		s.logger.Warn("table fit by table ID, but has no PKey", log.String("table", table.Fqtn()))
	}
	s.outputSchemas[table] = resSchema
	return true
}

func (s *ClickhouseTransformer) ResultSchema(schema *abstract.TableSchema) (*abstract.TableSchema, error) {
	s.engineMutex.Lock()
	defer s.engineMutex.Unlock()
	var inputCols []string
	rules := typesystem.RuleFor(clickhouse.ProviderType)
	for _, col := range schema.Columns() {
		chType, ok := rules.Target[ytschema.Type(col.DataType)]
		if !ok {
			return nil, xerrors.Errorf("col %s not found type %s in typesystem", col.ColumnName, col.DataType)
		}
		inputCols = append(inputCols, fmt.Sprintf("%s %s", col.ColumnName, chType))
	}
	if len(inputCols) == 0 {
		return nil, nil
	}
	inputStructure := strings.Join(inputCols, ",")
	cmd := exec.Command(s.clickhousePath, "local", "--input-format", "JSONEachRow", "--output-format", "JSONCompact", "--structure", inputStructure, "--query", s.query, "--no-system-tables")
	buffer := bytes.Buffer{}
	buffer.Write([]byte(""))
	cmd.Stdin = &buffer
	output, err := cmd.CombinedOutput()
	s.logger.Infof("exec: \n%s", strings.Join(cmd.Args, " "))
	if err != nil {
		s.logger.Warnf("exec: \n%s\nerror:%s", strings.Join(cmd.Args, " "), util.DefaultSample(string(output)))
		return nil, xerrors.Errorf("unable to exec query: %w\n:%s", err, util.DefaultSample(string(output)))
	}
	s.logger.Infof("input schema:\n%s\noutput:\n%s", inputStructure, string(output))
	var res jsonCompactResult
	if err := json.Unmarshal(output, &res); err != nil {
		return nil, xerrors.Errorf("unable to unmarshal query plan result: %w", err)
	}
	keys := map[string]bool{}
	for _, col := range schema.Columns() {
		if col.IsKey() {
			keys[col.ColumnName] = true
		}
	}
	var resSchema abstract.TableColumns
	for _, col := range res.Meta {
		dtType, ok := rules.Source[columntypes.BaseType(col.Type)]
		if !ok {
			dtType = ytschema.TypeAny
			s.logger.Infof("col %s not found type %s in typesystem, fallback as `any`", col.Name, col.Type)
		}
		resSchema = append(resSchema, abstract.ColSchema{
			TableSchema:  "",
			TableName:    "",
			Path:         "",
			ColumnName:   col.Name,
			DataType:     dtType.String(),
			PrimaryKey:   keys[col.Name],
			FakeKey:      false,
			Required:     false,
			Expression:   "",
			OriginalType: "ch:" + col.Type,
			Properties:   nil,
		})
	}
	resTableSchema := abstract.NewTableSchema(resSchema)
	if !resTableSchema.Columns().HasPrimaryKey() {
		return nil, xerrors.New("result table has no primary key")
	}
	return abstract.NewTableSchema(resSchema), nil
}

func (s *ClickhouseTransformer) Description() string {
	return "SQL transfer"
}

func New(config Config, lgr log.Logger) (*ClickhouseTransformer, error) {
	chPath := "clickhouse-local"
	if os.Getenv("CH_LOCAL_PATH") != "" {
		chPath = os.Getenv("CH_LOCAL_PATH")
	}
	tableFilter, err := filter.NewFilter(config.Tables.IncludeTables, config.Tables.ExcludeTables)
	if err != nil {
		return nil, xerrors.Errorf("unable to init table filter: %w", err)
	}
	return &ClickhouseTransformer{
		filter:         tableFilter,
		query:          config.Query,
		logger:         lgr,
		outputSchemas:  map[abstract.TableID]*abstract.TableSchema{},
		engineMutex:    sync.Mutex{},
		clickhousePath: chPath,
	}, nil
}
