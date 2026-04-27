package raw_to_table_common

type DataType string

const (
	Bytes  DataType = "bytes"
	String DataType = "string"
	JSON   DataType = "json"
)

func (j DataType) String() string {
	return string(j)
}
