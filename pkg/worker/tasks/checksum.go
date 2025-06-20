package tasks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors"
	"github.com/transferia/transferia/pkg/errors/categories"
	"github.com/transferia/transferia/pkg/providers"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	defaultTableSizeThreshold = 1024 * 1024 * 20 // 20mb
	compareRetryThreshold     = 3
	maxErrorSamplesPerKind    = 3

	genericError        = "generic"
	schemaMismatchError = "table schema mismatch"
	missedKeyError      = "missed key"

	roundingConst = 12
)

var defaultPgConnInfo = pgtype.NewConnInfo()

type ChecksumComparator func(lVal interface{}, lSchema abstract.ColSchema, rVal interface{}, rSchema abstract.ColSchema, intoArray bool) (comparable bool, result bool, err error)

func columnMismatchError(columnName string) string {
	return fmt.Sprintf("column '%v' mismatch", columnName)
}

type errorEntry struct {
	count                   int
	errorDescriptionSamples []string
}

type (
	errorKindMap map[ /* error kind */ string]*errorEntry
	errorMap     struct {
		lgr        log.Logger
		errorKinds map[ /* table name */ string]errorKindMap
	}
)

func newErrorMap(lgr log.Logger) errorMap {
	return errorMap{
		lgr:        lgr,
		errorKinds: map[string]errorKindMap{},
	}
}

func (em errorMap) addError(fqtn string, errorKind string, errorDescription string) {
	errors := em.emplace(fqtn).emplace(errorKind)
	errors.count++
	if len(errors.errorDescriptionSamples) < maxErrorSamplesPerKind {
		errors.errorDescriptionSamples = append(errors.errorDescriptionSamples, errorDescription)
	}
	em.lgr.Debugf("table %v, %v error: %v", fqtn, errorKind, errorDescription)
}

func (em errorMap) emplace(fqtn string) (result errorKindMap) {
	if result = em.errorKinds[fqtn]; result == nil {
		result = errorKindMap{}
		em.errorKinds[fqtn] = result
	}
	return result
}

func (em errorMap) clearTableErrors(table abstract.TableDescription) {
	em.errorKinds[table.Name] = errorKindMap{}
	em.errorKinds[table.Fqtn()] = errorKindMap{}
}

func (em errorMap) summary() (sampleErrorMessages []string, badTables []string, totalErrors int) {
	for fqtn, errorKinds := range em.errorKinds {
		if len(errorKinds) > 0 {
			badTables = append(badTables, fqtn)
		}
		for kind, entry := range errorKinds {
			for i, sampleDescription := range entry.errorDescriptionSamples {
				report := fmt.Sprintf("table %v, %v error (%v of %v): %v", fqtn, kind, i+1, entry.count, sampleDescription)
				sampleErrorMessages = append(sampleErrorMessages, report)
			}
			totalErrors += entry.count
		}
	}
	return sampleErrorMessages, badTables, totalErrors
}

func (ekm errorKindMap) emplace(errorKind string) (result *errorEntry) {
	if result = ekm[errorKind]; result == nil {
		result = new(errorEntry)
		ekm[errorKind] = result
	}
	return result
}

type ChecksumParameters struct {
	TableSizeThreshold  int64
	Tables              []abstract.TableDescription
	PriorityComparators []ChecksumComparator
}

func (p *ChecksumParameters) GetTableSizeThreshold() uint64 {
	if p == nil || p.TableSizeThreshold == 0 {
		return defaultTableSizeThreshold
	}
	return uint64(p.TableSizeThreshold)
}

func (p *ChecksumParameters) GetPriorityComparators() []ChecksumComparator {
	if p == nil {
		return nil
	}
	return p.PriorityComparators
}

func Checksum(transfer model.Transfer, lgr log.Logger, registry metrics.Registry, params *ChecksumParameters) error {
	var err error
	var srcStorage, dstStorage abstract.SampleableStorage
	var tables []abstract.TableDescription
	srcF, ok := providers.Source[providers.Sampleable](lgr, registry, coordinator.NewFakeClient(), &transfer)
	if !ok {
		return fmt.Errorf("unsupported source type for checksum: %T", transfer.Src)
	}
	srcStorage, tables, err = srcF.SourceSampleableStorage()
	if err != nil {
		return xerrors.Errorf("unabel to init source: %w", err)
	}
	defer srcStorage.Close()

	if len(params.Tables) > 0 {
		tables = params.Tables
	}
	dstF, ok := providers.Destination[providers.Sampleable](lgr, registry, coordinator.NewFakeClient(), &transfer)
	if !ok {
		return fmt.Errorf("unsupported source type for checksum: %T", transfer.Src)
	}
	dstStorage, err = dstF.DestinationSampleableStorage()
	if err != nil {
		return xerrors.Errorf("unable to init dst storage: %w", err)
	}
	defer dstStorage.Close()

	if err = CompareChecksum(srcStorage, dstStorage, tables, lgr, registry, func(l, r string) bool { return l == r }, params); err != nil {
		lgr.Warnf("Unable to compare checksum\n%v", err)
		return xerrors.Errorf(`unable to compare checksum: %w`, err)
	}
	return nil
}

type primaryKeys map[abstract.TableID][]string /* column name */

func loadSchema(storage abstract.SampleableStorage) (abstract.DBSchema, primaryKeys, error) {
	var schema abstract.DBSchema
	var err error
	switch s := storage.(type) {
	case abstract.SchemaStorage:
		schema, err = s.LoadSchema()
	default:
		return nil, nil, fmt.Errorf("cannot load primary keys for unsupported storage type: %T", s)
	}
	if err != nil {
		return nil, nil, err
	}

	pkeys := primaryKeys{}
	for tID, columns := range schema {
		for _, col := range columns.Columns() {
			if col.PrimaryKey {
				pkeys[tID] = append(pkeys[tID], col.ColumnName)
			}
		}
	}

	return schema, pkeys, nil
}

type SingleStorageSchema interface {
	DatabaseSchema() string
}

func CompareChecksum(
	src abstract.SampleableStorage,
	dst abstract.SampleableStorage,
	tables []abstract.TableDescription,
	lgr log.Logger,
	registry metrics.Registry,
	equalDataTypes func(lDataType, rDataType string) bool,
	params *ChecksumParameters,
) error {
	var matchedTables []string
	tableErrors := newErrorMap(lgr)

	lDBSchema, lPrimaryKeys, err := loadSchema(src)
	if err != nil {
		return fmt.Errorf("unable to load schema for source DB: %v", err)
	}
	rDBSchema, rPrimaryKeys, err := loadSchema(dst)
	if err != nil {
		return fmt.Errorf("unable to load schema for target DB: %v", err)
	}

TBLS:
	for _, srcTable := range tables {
		if !compareSchema(srcTable, lDBSchema, rDBSchema, tableErrors, equalDataTypes) {
			continue
		}
		if !comparePrimaryKeys(srcTable, lPrimaryKeys, rPrimaryKeys, tableErrors) {
			continue
		}
		if lightCompare(srcTable, src, dst) {
			logger.Log.Infof("light compare completed for table: %v", srcTable)
		}
		fullLoad := false
		matched := false
		for i := 0; i <= compareRetryThreshold; i++ {
			var lData map[string]abstract.ChangeItem
			var rData map[string]abstract.ChangeItem
			var lErr error
			var rErr error
			wg := sync.WaitGroup{}
			wg.Add(2)
			tableSize, err := src.TableSizeInBytes(srcTable.ID())
			if err == nil && tableSize < params.GetTableSizeThreshold() {
				fullLoad = true
			}
			go func() {
				data, err := loadTopBottomKeyset(src, srcTable, log.With(lgr, log.Any("kind", "source")), fullLoad)
				if err != nil {
					lErr = err
				}
				lData = data
				wg.Done()
			}()
			go func() {
				dstTable := srcTable
				switch t := dst.(type) {
				case SingleStorageSchema:
					dstTable.Schema = t.DatabaseSchema()
				}
				data, err := loadTopBottomKeyset(dst, dstTable, log.With(lgr, log.Any("kind", "target")), fullLoad)
				if err != nil {
					rErr = err
				}
				rData = data
				wg.Done()
			}()
			wg.Wait()
			if lErr != nil {
				tableErrors.addError(srcTable.Fqtn(), genericError, fmt.Sprintf("unable to load top/bottom keyset from source DB: %v", lErr))
				continue TBLS
			}
			if rErr != nil {
				tableErrors.addError(srcTable.Fqtn(), genericError, fmt.Sprintf("unable to load top/bottom keyset from target DB: %v", rErr))
				continue TBLS
			}
			matched = compareKeysets(lData, rData, srcTable, tableErrors, params.GetPriorityComparators())
			if matched {
				tableErrors.clearTableErrors(srcTable)
				matchedTables = append(matchedTables, srcTable.Fqtn())
				continue TBLS
			}
			lgr.Warnf("Top-bottom/full sample for %v comparing failed, retrying", srcTable.Name)
			time.Sleep(time.Duration(i) * time.Second)
		}
		if !matched {
			lgr.Errorf("Retrying top-bottom/full sample failed %v times. Continuing.", compareRetryThreshold)
			continue TBLS
		} else {
			tableErrors.clearTableErrors(srcTable)
			lgr.Infof("Table %v full/top-bottom sample matched successfully!", srcTable.Name)
		}

		if !fullLoad {
			left, keyRange, err := loadRandomKeyset(src, srcTable)
			if err != nil {
				tableErrors.addError(srcTable.Fqtn(), genericError, fmt.Sprintf("unable to load random keyset from source DB: %v", err))
				continue TBLS
			}
			right, err := loadExactKeyset(dst, srcTable, keyRange)
			if err != nil {
				tableErrors.addError(srcTable.Fqtn(), genericError, fmt.Sprintf("unable to load exact keyset from target DB: %v", err))
				continue TBLS
			}
			matched := compareKeysets(left, right, srcTable, tableErrors, params.GetPriorityComparators())
			mismatchCount := 0
			if !matched {
				for _, key := range keyRange {
					left, err = loadExactKeyset(src, srcTable, []map[string]interface{}{key})
					if err != nil {
						tableErrors.addError(srcTable.Fqtn(), genericError, fmt.Sprintf("unable to load exact keyset from source DB: %v", err))
						continue TBLS
					}
					right, err = loadExactKeyset(dst, srcTable, keyRange)
					if err != nil {
						tableErrors.addError(srcTable.Fqtn(), genericError, fmt.Sprintf("unable to load exact keyset from target DB: %v", err))
						continue TBLS
					}
					matched = compareKeysets(left, right, srcTable, tableErrors, params.GetPriorityComparators())
					if !matched {
						mismatchCount++
					}
				}
			}

			if mismatchCount > 0 {
				lgr.Errorf("Retrying random sample comparing failed %v times. Continuing.", compareRetryThreshold)
			} else {
				lgr.Infof("Table %v random sample matched successfully!", srcTable.Name)
				tableErrors.clearTableErrors(srcTable)
				matchedTables = append(matchedTables, srcTable.Fqtn())
			}
		}
	}

	sampleErrorMessages, badTables, totalErrors := tableErrors.summary()
	if totalErrors > 0 {
		sort.Strings(sampleErrorMessages)
		return xerrors.New(
			fmt.Sprintf(
				"Total Errors: %v\nTotal unmatched: %v\n%v\nErrors:\n%v\nTotal Matched: %v\n%v\n",
				totalErrors,
				len(badTables),
				strings.Join(badTables, "\n"),
				strings.Join(sampleErrorMessages, "\n"),
				len(matchedTables),
				strings.Join(matchedTables, "\n"),
			),
		)
	}
	return nil
}

func lightCompare(table abstract.TableDescription, src abstract.SampleableStorage, dst abstract.SampleableStorage) bool {
	result := map[string]abstract.ChangeItem{}
	var keys []map[string]interface{}
	if err := src.LoadRandomSample(table, func(input []abstract.ChangeItem) error {
		for _, row := range input {
			if row.CommitTime == 0 {
				break
			}
			keyVals := row.KeyVals()
			if len(keyVals) == 0 {
				return xerrors.Errorf("No key columns found for table %s", table.Fqtn())
			}
			result[strings.Join(keyVals, "-")] = row
			keys = append(keys, row.KeysAsMap())
		}
		return nil
	}); err != nil {
		logger.Log.Warnf("unable to read from src %v", err)
		return false
	}
	logger.Log.Infof("Prepare to compare %v rows", len(keys))
	if err := dst.LoadSampleBySet(table, keys, func(input []abstract.ChangeItem) error {
		if len(input) != len(keys) {
			logger.Log.Warnf("Mismatch size %v from %v", len(input), len(keys))
		}
		dstR := map[string]abstract.ChangeItem{}
		for _, row := range input {
			if row.CommitTime == 0 {
				break
			}
			keyVals := row.KeyVals()
			if len(keyVals) == 0 {
				return xerrors.Errorf("No key columns found for table %s", table.Fqtn())
			}
			dstR[strings.Join(keyVals, "-")] = row
		}
		missK := 0
		for k := range result {
			if _, ok := dstR[k]; !ok {
				logger.Log.Warnf("unable to find key: %v", k)
				missK++
			}
		}
		if missK > 0 {
			return xerrors.Errorf("Unable to find %v keys", missK)
		}
		return nil
	}); err != nil {
		logger.Log.Warnf("unable to compare with dst %v", err)
		return false
	}
	return true
}

type columnMap map[ /* column name */ string][]*abstract.ColSchema

const (
	sourceIndex = 0
	targetIndex = 1
)

func (cm columnMap) emplace(columnName string) []*abstract.ColSchema {
	if value, ok := cm[columnName]; ok {
		return value
	}
	slice := make([]*abstract.ColSchema, 2)
	cm[columnName] = slice
	return slice
}

func (cm columnMap) fill(dbSchema abstract.DBSchema, table abstract.TableDescription, valueIndex int) bool {
	tableSchema, ok := dbSchema[table.ID()]
	if !ok {
		var schemaPrefix string
		if table.Schema != "public" {
			schemaPrefix = table.Schema + "_"
		}
		tableSchema, ok = dbSchema[abstract.TableID{
			Namespace: "",
			Name:      schemaPrefix + table.Name,
		}]
		if !ok {
			return false
		}
	}
	for i, col := range tableSchema.Columns() {
		cm.emplace(col.ColumnName)[valueIndex] = &tableSchema.Columns()[i]
	}
	return true
}

func compareSchema(
	table abstract.TableDescription,
	lDBSchema abstract.DBSchema,
	rDBSchema abstract.DBSchema,
	tableErrors errorMap,
	equalDataTypes func(lDataType, rDataType string) bool,
) bool {
	columnMap := columnMap{}
	if !columnMap.fill(lDBSchema, table, sourceIndex) {
		tableErrors.addError(table.Fqtn(), genericError, "table not found in source DB")
		return false
	}
	if !columnMap.fill(rDBSchema, table, targetIndex) {
		tableErrors.addError(table.Fqtn(), genericError, "table not found in target DB")
		return false
	}

	ok := true
	for columnName, columnsSlice := range columnMap {
		lColumn, rColumn := columnsSlice[sourceIndex], columnsSlice[targetIndex]
		if lColumn == nil {
			tableErrors.addError(table.Fqtn(), schemaMismatchError, fmt.Sprintf("column '%v' not found in source table", rColumn.ColumnName))
			ok = false
			continue
		}

		if rColumn == nil {
			tableErrors.addError(table.Fqtn(), schemaMismatchError, fmt.Sprintf("column '%v' not found in target table", lColumn.ColumnName))
			ok = false
			continue
		}

		if !equalDataTypes(lColumn.DataType, rColumn.DataType) {
			errorDescription := fmt.Sprintf("column types differ for column '%v': (source) %v != %v (target)", columnName, lColumn.DataType, rColumn.DataType)
			tableErrors.addError(table.Fqtn(), schemaMismatchError, errorDescription)
			ok = false
		}
	}
	return ok
}

func getPrimaryKeys(table abstract.TableDescription, keys primaryKeys) ([]string, bool) {
	pKey, ok := keys[table.ID()]
	if !ok {
		var schemaPrefix string
		if table.Schema != "public" {
			schemaPrefix = table.Schema + "_"
		}
		pKey, ok = keys[abstract.TableID{
			Namespace: "",
			Name:      schemaPrefix + table.Name,
		}]
		if !ok {
			return nil, false
		}
	}
	return pKey, ok
}

func comparePrimaryKeys(
	table abstract.TableDescription,
	lPrimaryKeys primaryKeys,
	rPrimaryKeys primaryKeys,
	tableErrors errorMap,
) bool {
	lPkey, lOk := getPrimaryKeys(table, lPrimaryKeys)
	rPkey, rOk := getPrimaryKeys(table, rPrimaryKeys)
	if lOk != rOk {
		var errorDescription string
		if !lOk {
			errorDescription = fmt.Sprintf("no primary key defined for source table while primary key for target table is %v", rPkey)
		} else {
			errorDescription = fmt.Sprintf("no primary key defined for target table while primary key for source table is %v", lPkey)
		}
		tableErrors.addError(table.Fqtn(), schemaMismatchError, errorDescription)
		return false
	}
	if !slicesEqual(lPkey, rPkey) {
		tableErrors.addError(table.Fqtn(), schemaMismatchError, fmt.Sprintf("primary keys differ: (source) %v != %v (target)", lPkey, rPkey))
		return false
	}
	return true
}

func extractDouble(value interface{}) (float64, error) {
	switch val := value.(type) {
	case string:
		return strconv.ParseFloat(val, 64)
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	default:
		return math.NaN(), fmt.Errorf("can't convert %v to float64", val)
	}
}

func extractBytes(value interface{}) ([]byte, error) {
	switch val := value.(type) {
	case string:
		return []byte(val), nil
	case []byte:
		return val, nil
	default:
		return []byte{}, fmt.Errorf("can't convert %v to bytes", val)
	}
}

func comparePGInterval(interval1, interval2 string) bool {
	interval1 = strings.ReplaceAll(interval1, "days", "day")
	interval2 = strings.ReplaceAll(interval2, "days", "day")

	if len(interval1) > len(interval2) {
		interval1, interval2 = interval2, interval1
	}

	for i := 0; i < len(interval1); i++ {
		if interval1[i] != interval2[i] {
			return false
		}
	}

	for i := len(interval1); i < len(interval2); i++ {
		switch interval2[i] {
		case '0', '.', ':', ' ':
			continue
		default:
			return false
		}
	}
	return true
}

func compareSegments(segment1, segment2 string) bool {
	segment1 = strings.ReplaceAll(segment1, "[(", "(")
	segment1 = strings.ReplaceAll(segment1, ")]", ")")
	segment1 = strings.ReplaceAll(segment1, "((", "(")
	segment1 = strings.ReplaceAll(segment1, "))", ")")

	segment2 = strings.ReplaceAll(segment2, "[(", "(")
	segment2 = strings.ReplaceAll(segment2, ")]", ")")
	segment2 = strings.ReplaceAll(segment2, "((", "(")
	segment2 = strings.ReplaceAll(segment2, "))", ")")

	return segment1 == segment2
}

func rounded(n float64, err error) (float64, error) {
	if err != nil {
		return 0, xerrors.Errorf(`unable to round — input contains error: %w`, err)
	}

	nStr := fmt.Sprintf("%."+strconv.Itoa(roundingConst)+"f", n)
	n, err = strconv.ParseFloat(nStr, 64)
	if err != nil {
		return 0, xerrors.Errorf(`unable to round unparseable input: %w`, err)
	}

	return n, nil
}

func parseBox(box string) (interface{}, error) {
	// Example: (1.414213562373095,1.414213562373095),(-1.414213562373095,-1.414213562373095)
	firstComma := strings.Index(box, ",")
	if firstComma == -1 {
		return pgtype.Box{}, fmt.Errorf("couldn't convert %v to pgtype.Box", box)
	}

	x1, err := rounded(strconv.ParseFloat(box[1:firstComma], 64))
	if err != nil {
		return pgtype.Box{}, xerrors.Errorf(`unable to parse value "x1" from string "box": %w`, err)
	}

	secondComma := firstComma + 1 + strings.Index(box[firstComma+1:], ",")
	if secondComma == firstComma { // or just strings.Index(box[firstComma+1:] == -1
		return pgtype.Box{}, fmt.Errorf("couldn't convert %v to pgtype.Box", box)
	}

	y1, err := rounded(strconv.ParseFloat(box[firstComma+1:secondComma-1], 64))
	if err != nil {
		return pgtype.Box{}, xerrors.Errorf(`unable to parse value "y1" from string "box": %w`, err)
	}

	thirdComma := secondComma + 1 + strings.Index(box[secondComma+1:], ",")
	if thirdComma == secondComma {
		return pgtype.Box{}, fmt.Errorf("couldn't convert %v to pgtype.Box", box)
	}

	x2, err := rounded(strconv.ParseFloat(box[secondComma+2:thirdComma], 64))
	if err != nil {
		return pgtype.Box{}, xerrors.Errorf(`unable to parse value "x2" from string "box": %w`, err)
	}

	y2, err := rounded(strconv.ParseFloat(box[thirdComma+1:len(box)-1], 64))
	if err != nil {
		return pgtype.Box{}, xerrors.Errorf(`unable to parse value "y2" from string "box": %w`, err)
	}

	return pgtype.Box{
		P: [2]pgtype.Vec2{
			{X: x1, Y: y1},
			{X: x2, Y: y2},
		},
		Status: pgtype.Present,
	}, nil
}

func parseCircle(circle string) (interface{}, error) {
	// Example: <(1,0.3333333333333333),0.9249505911485288>
	firstComma := strings.Index(circle, ",")
	if firstComma == -1 {
		return pgtype.Circle{}, fmt.Errorf("couldn't convert %v to pgtype.Circle", circle)
	}

	x, err := rounded(strconv.ParseFloat(circle[2:firstComma], 64))
	if err != nil {
		return pgtype.Circle{}, xerrors.Errorf(`unable to parse value "x" from string "circle": %w`, err)
	}

	secondComma := firstComma + 1 + strings.Index(circle[firstComma+1:], ",")
	if secondComma == firstComma {
		return pgtype.Circle{}, fmt.Errorf("couldn't convert %v to pgtype.Circle", circle)
	}

	y, err := rounded(strconv.ParseFloat(circle[firstComma+1:secondComma-1], 64))
	if err != nil {
		return pgtype.Circle{}, xerrors.Errorf(`unable to parse value "y" from string "circle": %w`, err)
	}

	r, err := rounded(strconv.ParseFloat(circle[secondComma+1:len(circle)-1], 64))
	if err != nil {
		return pgtype.Circle{}, xerrors.Errorf(`unable to parse value "r" from string "circle": %w`, err)
	}

	return pgtype.Circle{
		P:      pgtype.Vec2{X: x, Y: y},
		R:      r,
		Status: pgtype.Present,
	}, nil
}

func parsePolygon(polygon string) (interface{}, error) {
	// Example: ((-2,0),(-1.7320508075688774,0.9999999999999999))
	start := 1
	var points []pgtype.Vec2
	for {
		if start >= len(polygon) {
			break
		}

		isLast := false
		innerComma := start + strings.Index(polygon[start:], ",")
		if innerComma == start-1 {
			return pgtype.Polygon{}, fmt.Errorf("couldn't convert %v to pgtype.Polygon", polygon)
		}

		outerComma := innerComma + 1 + strings.Index(polygon[innerComma+1:], ",")
		if outerComma == innerComma {
			outerComma = len(polygon) - 1
			isLast = true
		}

		x, err := rounded(strconv.ParseFloat(polygon[start+1:innerComma], 64))
		if err != nil {
			return pgtype.Polygon{}, xerrors.Errorf(`unable to parse value "x" from string "polygon": %w`, err)
		}

		y, err := rounded(strconv.ParseFloat(polygon[innerComma+1:outerComma-1], 64))
		if err != nil {
			return pgtype.Polygon{}, xerrors.Errorf(`unable to parse value "y" from string "polygon": %w`, err)
		}

		points = append(points, pgtype.Vec2{X: x, Y: y})
		start = outerComma + 1

		if isLast {
			break
		}
	}

	return pgtype.Polygon{
		P:      points,
		Status: pgtype.Present,
	}, nil
}

func compareGeometry(obj1, obj2 string, parser func(string) (interface{}, error)) (bool, error) {
	lObj, lErr := parser(obj1)
	rObj, rErr := parser(obj2)

	if lErr != nil {
		return false, xerrors.Errorf(`unable to parse first comparison object: %w`, lErr)
	} else if rErr != nil {
		return false, xerrors.Errorf(`unable to parse second comparison object: %w`, rErr)
	} else {
		return reflect.DeepEqual(lObj, rObj), nil
	}
}

func unwrapJSONNumber(in interface{}) interface{} {
	switch t := in.(type) {
	case json.Number:
		result, _ := t.Float64()
		return result
	default:
		return in
	}
}

func compareKeysets(
	left map[string]abstract.ChangeItem,
	right map[string]abstract.ChangeItem,
	table abstract.TableDescription,
	tableErrors errorMap,
	priorityComparators []ChecksumComparator,
) bool {
	matched := true

	keysAlreadyCompared := make(map[string]bool)
	for id, l := range left {
		if !l.IsRowEvent() {
			continue
		}

		r, ok := right[id]
		if !ok {
			tableErrors.addError(table.Fqtn(), missedKeyError, fmt.Sprintf("a value for key %q is present in the left table but missing in the right one", id))
			matched = false
			continue
		}

		if !r.IsRowEvent() {
			continue
		}

		colNameToSchemaIdxL := abstract.MakeMapColNameToIndex(l.TableSchema.Columns())
		colNameToSchemaIdxR := abstract.MakeMapColNameToIndex(r.TableSchema.Columns())

		keysAlreadyCompared[id] = true

	overColumns:
		for lIdx, lCol := range l.ColumnNames {
			for rIdx, rCol := range r.ColumnNames {
				if lCol != rCol {
					continue
				}
				colName := lCol

				lVal := l.ColumnValues[lIdx]
				rVal := r.ColumnValues[rIdx]

				lSIdx, lSok := colNameToSchemaIdxL[colName]
				if !lSok {
					tableErrors.addError(table.Fqtn(), columnMismatchError(colName), fmt.Sprintf("column schema is missing in the left table for key %q", id))
					matched = false
					continue overColumns
				}
				lSchema := l.TableSchema.Columns()[lSIdx]

				rSIdx, rSok := colNameToSchemaIdxR[colName]
				if !rSok {
					tableErrors.addError(table.Fqtn(), columnMismatchError(colName), fmt.Sprintf("column schema is missing in the right table for key %q", id))
					matched = false
					continue overColumns
				}
				rSchema := r.TableSchema.Columns()[rSIdx]

				comparisonResult, err := tryCompare(lVal, lSchema, rVal, rSchema, priorityComparators, false)
				if err != nil {
					tableErrors.addError(table.Fqtn(), columnMismatchError(colName), fmt.Sprintf("comparison failed for key %q: (source) %s ? %s (target): %v", id, valueAndSchemaHumanReadable(lVal, lSchema), valueAndSchemaHumanReadable(rVal, rSchema), err))
					matched = false
					continue overColumns
				}
				if !comparisonResult {
					tableErrors.addError(table.Fqtn(), columnMismatchError(colName), fmt.Sprintf("values differ for key %q: (source) %s != %s (target)", id, valueAndSchemaHumanReadable(lVal, lSchema), valueAndSchemaHumanReadable(rVal, rSchema)))
					matched = false
					continue overColumns
				}
				continue overColumns
			}
		}
	}

	for id, r := range right {
		if !r.IsRowEvent() {
			continue
		}

		if _, ok := keysAlreadyCompared[id]; ok {
			continue
		}

		matched = false
		tableErrors.addError(table.Fqtn(), missedKeyError, fmt.Sprintf("a value for key %q is present in the right table but missing in the left one", id))
	}

	return matched
}

func valueAndSchemaHumanReadable(value interface{}, schema abstract.ColSchema) string {
	return fmt.Sprintf("%v [%T | %q]", value, value, schema.OriginalType)
}

func tryCompare(lVal interface{}, lSchema abstract.ColSchema, rVal interface{}, rSchema abstract.ColSchema, priorityComparators []ChecksumComparator, intoArray bool) (bool, error) {
	lVal = unwrapJSONNumber(lVal)
	rVal = unwrapJSONNumber(rVal)

	lS := fmt.Sprintf("%v", lVal)
	rS := fmt.Sprintf("%v", rVal)

	if lS == rS {
		return true, nil
	}

	for _, pc := range priorityComparators {
		if comparable, equal, err := pc(lVal, lSchema, rVal, rSchema, intoArray); err != nil {
			return false, xerrors.Errorf("failed to compare by a priority comparator: %w", err)
		} else if comparable {
			return equal, nil
		}
	}

	if comparable, equal := tryCompareNulls(lVal, rVal); comparable {
		return equal, nil
	}

	if comparable, equal, err := tryCompareSlices(lVal, lSchema, rVal, rSchema, priorityComparators); err != nil {
		return false, xerrors.Errorf("failed to compare slices: %w", err)
	} else if comparable {
		return equal, nil
	}

	if comparable, equal, err := tryCompareTemporals(lVal, lSchema, rVal, rSchema); err != nil {
		return false, xerrors.Errorf("failed to compare temporal types: %w", err)
	} else if comparable {
		return equal, nil
	}

	if comparable, equal, err := tryComparePgTextRepresentation(lVal, lSchema, rVal, rSchema); err != nil {
		return false, xerrors.Errorf("failed to compare postgresql types: %w", err)
	} else if comparable {
		return equal, nil
	}

	if strings.HasSuffix(lSchema.OriginalType, ":json") || strings.HasSuffix(rSchema.OriginalType, ":json") ||
		strings.HasSuffix(lSchema.OriginalType, ":jsonb") || strings.HasSuffix(rSchema.OriginalType, ":jsonb") {
		return fmt.Sprintf("%v", lVal) == fmt.Sprintf("%v", rVal), nil
	}

	if lSchema.DataType == ytschema.TypeFloat64.String() || rSchema.DataType == ytschema.TypeFloat64.String() {
		lDouble, lErr := extractDouble(lVal)
		rDouble, rErr := extractDouble(rVal)

		if lErr == nil && rErr == nil {
			return lDouble == rDouble, nil
		}
	}

	lOriginalIsPG := strings.HasPrefix(lSchema.OriginalType, "pg:")
	rOriginalIsPG := strings.HasPrefix(rSchema.OriginalType, "pg:")

	if lOriginalIsPG || rOriginalIsPG {
		return tryComparePg(lOriginalIsPG, rOriginalIsPG, lVal, rVal, lS, rS, lSchema, rSchema)
	}

	return false, nil
}

func anyOfAmong(a string, b string, compareWith ...string) bool {
	for _, c := range compareWith {
		if a == c || b == c {
			return true
		}
	}
	return false
}

func tryCompareNulls(lVal interface{}, rVal interface{}) (comparable bool, result bool) {
	if lVal == nil && rVal == nil {
		return true, true
	}
	if lValJN, ok := lVal.(jsonx.JSONNull); ok {
		return true, lValJN.Equals(rVal)
	}
	if rValJN, ok := rVal.(jsonx.JSONNull); ok {
		return true, rValJN.Equals(lVal)
	}
	return false, false
}

func tryCompareSlices(lVal interface{}, lSchema abstract.ColSchema, rVal interface{}, rSchema abstract.ColSchema, priorityComparators []ChecksumComparator) (comparable bool, result bool, err error) {
	lValS, lValIsSlice := lVal.([]any)
	rValS, rValIsSlice := rVal.([]any)
	if !lValIsSlice || !rValIsSlice {
		return false, false, nil
	}
	result, err = compareSlices(lValS, lSchema, rValS, rSchema, priorityComparators)
	return true, result, err
}

func compareSlices(lVal []interface{}, lSchema abstract.ColSchema, rVal []interface{}, rSchema abstract.ColSchema, priorityComparators []ChecksumComparator) (bool, error) {
	if len(lVal) != len(rVal) {
		return false, nil
	}
	equal := true
	errors := util.NewErrs(nil)
	for i := range lVal {
		iEqual, err := tryCompare(lVal[i], colSchemaForSliceElement(lSchema), rVal[i], colSchemaForSliceElement(rSchema), priorityComparators, true)
		if err != nil {
			errors = append(errors, xerrors.Errorf("failed to compare slice element [%d]: %w", i, err))
		}
		equal = equal && iEqual
	}
	if !errors.Empty() {
		return false, errors
	}
	return equal, nil
}

// colSchemaForSliceElement transforms the given schema in such a way that it is suitable for comparison of an individual slice element
func colSchemaForSliceElement(s abstract.ColSchema) abstract.ColSchema {
	s.OriginalType = strings.TrimSuffix(s.OriginalType, "[]")
	return s
}

const pgTimestampWithoutTZFormat string = "2006-01-02 15:04:05.999999"

func tryCompareTemporals(lVal interface{}, lSchema abstract.ColSchema, rVal interface{}, rSchema abstract.ColSchema) (comparable bool, result bool, err error) {
	lS, lSOk := lVal.(string)
	rS, rSOk := rVal.(string)
	castsToString := lSOk && rSOk

	if mustReturn, comparable, result, err := tryCompareTemporalsPg(lVal, lSchema, rVal, rSchema); mustReturn {
		return comparable, result, err
	}

	switch {
	case lSchema.OriginalType == "pg:interval" && rSchema.OriginalType == "pg:interval":
		if !castsToString {
			return false, false, nil
		}
		return true, comparePGInterval(lS, rS), nil
	}

	if lSOk {
		lVal, _ = dateparse.ParseAny(lS)
	}
	if rSOk {
		rVal, _ = dateparse.ParseAny(rS)
	}
	lTime, lOk := lVal.(time.Time)
	rTime, rOk := rVal.(time.Time)
	if !lOk || !rOk {
		return false, false, nil
	}
	return true, lTime.Equal(rTime), nil
}

func tryComparePgTextRepresentation(lVal interface{}, lSchema abstract.ColSchema, rVal interface{}, rSchema abstract.ColSchema) (comparable bool, result bool, err error) {
	if lSchema.OriginalType != rSchema.OriginalType || !strings.HasPrefix(lSchema.OriginalType, "pg:") {
		return false, false, nil
	}
	lEncoder, ok := lVal.(pgtype.TextEncoder)
	if !ok {
		return false, false, nil
	}
	rEncoder, ok := rVal.(pgtype.TextEncoder)
	if !ok {
		return false, false, nil
	}

	lText, err := lEncoder.EncodeText(defaultPgConnInfo, nil)
	if err != nil {
		return false, false, xerrors.Errorf("encode left value: %w", err)
	}
	rText, err := rEncoder.EncodeText(defaultPgConnInfo, nil)
	if err != nil {
		return false, false, xerrors.Errorf("encode right value: %w", err)
	}

	return true, bytes.Equal(lText, rText), nil
}

func loadTopBottomKeyset(st abstract.SampleableStorage, table abstract.TableDescription, lgr log.Logger, fullOption bool) (map[string]abstract.ChangeItem, error) {
	var err error
	var keyset map[string]abstract.ChangeItem
	if fullOption {
		keyset, err = loadFull(st, table)
		if err != nil {
			return nil, xerrors.Errorf("Cannot load full keyset for table %s: %w", table.Fqtn(), err)
		}
	} else {
		keyset, err = loadTopBottom(st, table, lgr)
		if err != nil {
			return nil, xerrors.Errorf("Cannot load top/bottom keyset for table %s: %w", table.Fqtn(), err)
		}
	}
	return keyset, nil
}

func loadFull(st abstract.SampleableStorage, table abstract.TableDescription) (map[string]abstract.ChangeItem, error) {
	result := map[string]abstract.ChangeItem{}
	last := time.Now()
	upCtx := util.ContextWithTimestamp(context.Background(), last)
	if err := st.LoadTable(upCtx, table, func(input []abstract.ChangeItem) error {
		for _, row := range input {
			if row.Kind == abstract.InsertKind {
				keyVals := row.KeyVals()
				if len(keyVals) == 0 {
					return xerrors.Errorf("No key columns found for table %s", table.Fqtn())
				}
				result[strings.Join(keyVals, "-")] = row
			}
		}
		return nil
	}); err != nil {
		return nil, errors.CategorizedErrorf(categories.Source, "failed to load table '%s': %w", table.Fqtn(), err)
	}
	return result, nil
}

func loadTopBottom(st abstract.SampleableStorage, table abstract.TableDescription, lgr log.Logger) (map[string]abstract.ChangeItem, error) {
	result := map[string]abstract.ChangeItem{}
	lgr.Infof("table is to big %v (%v rows), would compare sample", table.Fqtn(), table.EtaRow)
	if err := st.LoadTopBottomSample(table, func(input []abstract.ChangeItem) error {
		for _, row := range input {
			if row.CommitTime == 0 {
				break
			}
			keyVals := row.KeyVals()
			if len(keyVals) == 0 {
				return xerrors.Errorf("No key columns found for table %s", table.Fqtn())
			}
			result[strings.Join(keyVals, "-")] = row
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func loadRandomKeyset(st abstract.SampleableStorage, table abstract.TableDescription) (map[string]abstract.ChangeItem, []map[string]interface{}, error) {
	result := map[string]abstract.ChangeItem{}
	var keySet []map[string]interface{}
	err := st.LoadRandomSample(table, func(input []abstract.ChangeItem) error {
		for _, row := range input {
			if row.CommitTime == 0 {
				break
			}
			keyVals := row.KeyVals()
			if len(keyVals) == 0 {
				return xerrors.Errorf("No key columns found for table %s", table.Fqtn())
			}
			result[strings.Join(keyVals, "-")] = row
			keySet = append(keySet, row.KeysAsMap())
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return result, keySet, nil
}

func loadExactKeyset(st abstract.SampleableStorage, table abstract.TableDescription, keySet []map[string]interface{}) (map[string]abstract.ChangeItem, error) {
	result := map[string]abstract.ChangeItem{}
	if err := st.LoadSampleBySet(table, keySet, func(input []abstract.ChangeItem) error {
		for _, row := range input {
			if row.CommitTime == 0 {
				break
			}
			keyVals := row.KeyVals()
			if len(keyVals) == 0 {
				return xerrors.Errorf("No key columns found for table %s", table.Fqtn())
			}
			result[strings.Join(keyVals, "-")] = row
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
