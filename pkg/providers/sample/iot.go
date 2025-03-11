package sample

import (
	_ "embed"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/yt/go/schema"
)

//go:embed data/iot-data.json
var iotJSONData []byte

type iotData struct {
	EventTypes      []string `json:"eventTypes"`
	DeviceTypes     []string `json:"deviceTypes"`
	MetricTypes     []string `json:"metricTypes"`
	DeviceStatuses  []string `json:"deviceStatuses"`
	EventSeverities []string `json:"eventSeverities"`
	EventSources    []string `json:"eventSources"`
}

var iotMarshaledData iotData

func init() {
	if err := json.Unmarshal(iotJSONData, &iotMarshaledData); err != nil {
		panic(err)
	}
}

const (
	sampleSourceEventID       = "event_id"
	sampleSourceTimeColumn    = "event_utc_ts"
	sampleSourceEventType     = "event_type"
	sampleSourceDeviceID      = "device_id"
	sampleSourceDeviceType    = "device_type"
	sampleSourceMetricValue   = "metric_value"
	sampleSourceMetricType    = "metric_type"
	sampleSourceLocationID    = "location_id"
	sampleSourceDeviceStatus  = "device_status"
	sampleSourceEventSeverity = "event_severity"
	sampleSourceEventSource   = "event_source"
	generateCustomIDLength    = 12
)

var (
	_ StreamingData = (*IotData)(nil)

	iotDataColumnSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: sampleSourceEventID, DataType: schema.TypeString.String(), Required: true, PrimaryKey: true},
		{ColumnName: sampleSourceTimeColumn, DataType: schema.TypeTimestamp.String(), Required: true, PrimaryKey: true},
		{ColumnName: sampleSourceEventType, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceDeviceID, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceDeviceType, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceMetricValue, DataType: schema.TypeFloat64.String(), Required: true},
		{ColumnName: sampleSourceMetricType, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceLocationID, DataType: schema.TypeInt64.String(), Required: true},
		{ColumnName: sampleSourceDeviceStatus, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceEventSeverity, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceEventSource, DataType: schema.TypeString.String(), Required: true},
	})
	iotDataColumns = []string{
		sampleSourceEventID,
		sampleSourceTimeColumn,
		sampleSourceEventType,
		sampleSourceDeviceID,
		sampleSourceDeviceType,
		sampleSourceMetricValue,
		sampleSourceMetricType,
		sampleSourceLocationID,
		sampleSourceDeviceStatus,
		sampleSourceEventSeverity,
		sampleSourceEventSource,
	}
)

type IotData struct {
	table         string
	eventID       string
	eventTime     time.Time
	eventType     string
	deviceID      string
	deviceType    string
	metricValue   float64
	metricType    string
	locationID    int64
	deviceStatus  string
	eventSeverity string
	eventSource   string
}

func (i *IotData) TableName() abstract.TableID {
	return abstract.TableID{
		Namespace: "",
		Name:      i.table,
	}
}

func (i *IotData) ToChangeItem(offset int64) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:          0,
		LSN:         uint64(offset),
		CommitTime:  uint64(i.eventTime.UnixNano()),
		Counter:     0,
		Kind:        abstract.InsertKind,
		Schema:      "",
		Table:       i.table,
		PartID:      "",
		TableSchema: iotDataColumnSchema,
		ColumnNames: iotDataColumns,
		ColumnValues: []interface{}{
			i.eventID,
			i.eventTime,
			i.eventType,
			i.deviceID,
			i.deviceType,
			i.metricValue,
			i.metricType,
			i.locationID,
			i.deviceStatus,
			i.eventSeverity,
			i.eventSource,
		},
		OldKeys: abstract.EmptyOldKeys(),
		TxID:    "",
		Query:   "",
		Size:    abstract.RawEventSize(util.SizeOfStruct(*i) - uint64(len(i.table))),
	}
}

func NewIot(
	table string,
) *IotData {
	return &IotData{
		table:         table,
		eventID:       generateCustomID(generateCustomIDLength),
		eventTime:     time.Now(),
		eventType:     gofakeit.RandomString(iotMarshaledData.EventTypes),
		deviceID:      gofakeit.UUID(),
		deviceType:    gofakeit.RandomString(iotMarshaledData.DeviceTypes),
		metricValue:   gofakeit.Float64Range(0, 100),
		metricType:    gofakeit.RandomString(iotMarshaledData.MetricTypes),
		locationID:    gofakeit.Int64(),
		deviceStatus:  gofakeit.RandomString(iotMarshaledData.DeviceStatuses),
		eventSeverity: gofakeit.RandomString(iotMarshaledData.EventSeverities),
		eventSource:   gofakeit.RandomString(iotMarshaledData.EventSources),
	}
}

func generateCustomID(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	id := make([]byte, length)
	for i := range id {
		id[i] = charset[rand.Intn(len(charset))]
	}
	return string(id)
}
