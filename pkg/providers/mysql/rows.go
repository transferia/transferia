//go:build !disable_mysql_provider

package mysql

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/net/html/charset"
)

// The action name for sync.
const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"
)

// RowsEvent is the event for row replication.
type RowsEvent struct {
	Table  *schema.Table
	Action string
	Header *replication.EventHeader
	// binlog has three update event version, v0, v1 and v2.
	// for v1 and v2, the rows number must be even.
	// Two rows for one event, format is [before update row, after update row]
	// for update v0, only one row for a event, and we don't support this version.
	Data *replication.RowsEvent

	// rows query is defined if option binlog_rows_query_log_events is enabled
	Query string
}

func newRowsEvent(table *schema.Table, action string, header *replication.EventHeader, event *replication.RowsEvent, rowsQuery string) *RowsEvent {
	e := new(RowsEvent)

	e.Table = table
	e.Action = action
	e.Header = header
	e.Data = event
	e.Query = rowsQuery

	e.handleUnsigned()

	return e
}

const maxMediumintUnsigned int32 = 16777215

func isBitSet(bitmap []byte, i uint64) bool {
	return bitmap[i>>3]&(1<<(uint(i)&7)) > 0
}

func isAllColumnsPresent(bitmap []byte, columnCount uint64) bool {
	for i := uint64(0); i < columnCount; i++ {
		if !isBitSet(bitmap, i) {
			return false
		}
	}
	return true
}

func (r *RowsEvent) Nullable(i uint64) bool {
	return isBitSet(r.Data.Table.NullBitmap, i)
}

func (r *RowsEvent) IsColumnPresent1(i uint64) bool {
	return isBitSet(r.Data.ColumnBitmap1, i)
}

func (r *RowsEvent) IsAllColumnsPresent1() bool {
	return isAllColumnsPresent(r.Data.ColumnBitmap1, r.Data.ColumnCount)
}

func (r *RowsEvent) IsColumnPresent2(i uint64) bool {
	return isBitSet(r.Data.ColumnBitmap2, i)
}

func (r *RowsEvent) IsAllColumnsPresent2() bool {
	return isAllColumnsPresent(r.Data.ColumnBitmap2, r.Data.ColumnCount)
}

func (r *RowsEvent) handleUnsigned() {
	// Handle Unsigned Columns here, for binlog replication, we can't know the integer is unsigned or not,
	// so we use int type but this may cause overflow outside sometimes, so we must convert to the really .
	// unsigned type
	if len(r.Table.UnsignedColumns) == 0 {
		return
	}

	for i := 0; i < len(r.Data.Rows); i++ {
		for _, columnIdx := range r.Table.UnsignedColumns {
			if len(r.Data.Rows[i]) <= columnIdx {
				logger.Log.Warnf("unable to locate unsigned column %v in row with %v cols", columnIdx, len(r.Data.Rows[i]))
				continue
			}
			switch value := r.Data.Rows[i][columnIdx].(type) {
			case int8:
				r.Data.Rows[i][columnIdx] = uint8(value)
			case int16:
				r.Data.Rows[i][columnIdx] = uint16(value)
			case int32:
				// problem with mediumint is that it's a 3-byte type. There is no compatible golang type to match that.
				// So to convert from negative to positive we'd need to convert the value manually
				if value < 0 && r.Table.Columns[columnIdx].Type == schema.TYPE_MEDIUM_INT {
					r.Data.Rows[i][columnIdx] = uint32(maxMediumintUnsigned + value + 1)
				} else {
					r.Data.Rows[i][columnIdx] = uint32(value)
				}
			case int64:
				r.Data.Rows[i][columnIdx] = uint64(value)
			case int:
				r.Data.Rows[i][columnIdx] = uint(value)
			default:
				// nothing to do
			}
		}
	}
}

func (r *RowsEvent) GetColumnName(i int) string {
	return r.Table.Columns[i].Name
}

func (r *RowsEvent) GetColumnRawType(i int) string {
	return r.Table.Columns[i].RawType
}

func (r *RowsEvent) GetColumnCollation(i int) string {
	return r.Table.Columns[i].Collation
}

func (r *RowsEvent) GetColumnSetValue(columnIndex int, setValueIndex int) string {
	return r.Table.Columns[columnIndex].SetValues[setValueIndex]
}

func (r *RowsEvent) GetColumnEnumValue(columnIndex int, enumValueIndex int64) string {
	return r.Table.Columns[columnIndex].EnumValues[enumValueIndex]
}

// String implements fmt.Stringer interface.
func (r *RowsEvent) String() string {
	return fmt.Sprintf("%s %s %v", r.Action, r.Table, r.Data.Rows)
}

func RestoreStringAndBytes(event *RowsEvent) error {
	for columnIndex := range event.Table.Columns {
		columnYtType := TypeToYt(event.GetColumnRawType(columnIndex))
		tryRestoreStringAndBytesTypes(event, columnIndex, columnYtType)
		if err := tryDecodeText(event, columnIndex); err != nil {
			return xerrors.Errorf("Decode text for column '%v' error: %w", event.Table.Columns[columnIndex].Name, err)
		}
	}
	return nil
}

func tryRestoreStringAndBytesTypes(event *RowsEvent, columnIndex int, columnYtType ytschema.Type) {
	for i := range event.Data.Rows {
		value := event.Data.Rows[i][columnIndex]
		if currBytes, ok := value.([]byte); ok && columnYtType != ytschema.TypeBytes {
			event.Data.Rows[i][columnIndex] = string(currBytes)
		}
		if currString, ok := value.(string); ok && columnYtType == ytschema.TypeBytes {
			event.Data.Rows[i][columnIndex] = []byte(currString)
		}
	}
}

func alterUTF16Charset(charsetName string) string {
	result := strings.ReplaceAll(charsetName, "utf16", "utf-16")
	// by default mysql utf16 charset is big endian but in golang.org/x/net/html that we use here it is processed as little endian
	if !strings.HasSuffix(strings.ToLower(charsetName), "le") {
		result += "be"
	}
	return result
}

func tryDecodeText(event *RowsEvent, columnIndex int) error {
	charsetName := extractCharset(event.GetColumnCollation(columnIndex))
	if charsetName == "" {
		return nil
	}
	if strings.HasPrefix(strings.ToLower(charsetName), "utf8") {
		return nil
	}
	if strings.HasPrefix(strings.ToLower(charsetName), "utf16") {
		charsetName = alterUTF16Charset(charsetName)
	}

	for i := range event.Data.Rows {
		var columnValueAsBytes []byte
		switch columnValue := event.Data.Rows[i][columnIndex].(type) {
		case string:
			columnValueAsBytes = []byte(columnValue)
		case []byte:
			columnValueAsBytes = columnValue
		default:
			continue
		}
		if len(columnValueAsBytes) == 0 {
			continue
		}

		utf8, err := toUtf8(columnValueAsBytes, charsetName)
		if err != nil {
			return xerrors.Errorf("Cannot convert %s from %s to utf8: %w", util.SampleHex(columnValueAsBytes, 1000), charsetName, err)
		}

		switch event.Data.Rows[i][columnIndex].(type) {
		case string:
			event.Data.Rows[i][columnIndex] = string(utf8)
		case []byte:
			event.Data.Rows[i][columnIndex] = utf8
		default:
			return xerrors.Errorf("Logic error, row item type change during cast")
		}
	}
	return nil
}

// Charset name may be derived from collation: https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html
func extractCharset(collation string) string {
	underscorePos := strings.IndexRune(collation, '_')
	if underscorePos == -1 {
		return ""
	}
	return collation[:underscorePos]
}

func toUtf8(data []byte, charsetName string) ([]byte, error) {
	reader, err := charset.NewReaderLabel(charsetName, bytes.NewReader(data))
	if err != nil {
		return nil, xerrors.Errorf("Cannot create charset reader: %w", err)
	}
	utf8data, err := io.ReadAll(reader)
	if err != nil {
		return nil, xerrors.Errorf("Read: %w", err)
	}
	return utf8data, nil
}

func Validate(event *RowsEvent) error {
	for i := range event.Data.Rows {
		if len(event.Table.Columns) != len(event.Data.Rows[i]) {
			builder := strings.Builder{}
			for _, column := range event.Table.Columns {
				if builder.Len() > 0 {
					builder.WriteString(", ")
				}
				builder.WriteString(column.Name)
			}
			return abstract.NewFatalError(
				xerrors.Errorf("Schema changes detected, table columns count: %v, event columns count: %v, table columns: [%v]",
					len(event.Table.Columns), len(event.Data.Rows[i]), builder.String()))
		}
	}
	return nil
}
