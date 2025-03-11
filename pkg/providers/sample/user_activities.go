package sample

import (
	_ "embed"
	"encoding/json"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/yt/go/schema"
)

//go:embed data/user-activities.json
var userActivitiesJSONData []byte

type userActivitiesData struct {
	EventTypes     []string `json:"eventTypes"`
	DeviceTypes    []string `json:"deviceTypes"`
	DeviceOS       []string `json:"deviceOS"`
	TrafficSources []string `json:"trafficSources"`
}

var userActivitiesMarshaledData userActivitiesData

func init() {
	if err := json.Unmarshal(userActivitiesJSONData, &userActivitiesMarshaledData); err != nil {
		panic(err)
	}
}

const (
	sampleSourceCityNameColumn      = "city_name"
	sampleSourceDeviceOSColumn      = "device_os"
	sampleSourceCountryNameColumn   = "country_name"
	sampleSourceEventRevenueColumn  = "event_revenue"
	sampleSourceTrafficSourceColumn = "traffic_source"
	sampleSourceUserIDColumn        = "user_id"
	sampleSourceSessionIDColumn     = "session_id"
	sampleSourceEventDuration       = "event_duration"
)

var (
	_ StreamingData = (*UserActivities)(nil)

	userActivitiesColumnSchema = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: sampleSourceEventID, DataType: schema.TypeString.String(), Required: true, PrimaryKey: true},
		{ColumnName: sampleSourceTimeColumn, DataType: schema.TypeTimestamp.String(), Required: true, PrimaryKey: true},
		{ColumnName: sampleSourceEventType, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceDeviceID, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceDeviceType, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceDeviceOSColumn, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceCountryNameColumn, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceCityNameColumn, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceEventRevenueColumn, DataType: schema.TypeFloat64.String(), Required: false},
		{ColumnName: sampleSourceTrafficSourceColumn, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceUserIDColumn, DataType: schema.TypeInt64.String(), Required: true},
		{ColumnName: sampleSourceSessionIDColumn, DataType: schema.TypeString.String(), Required: true},
		{ColumnName: sampleSourceEventDuration, DataType: schema.TypeFloat64.String(), Required: true},
	})

	userActivitiesColumns = []string{
		sampleSourceEventID,
		sampleSourceTimeColumn,
		sampleSourceEventType,
		sampleSourceDeviceID,
		sampleSourceDeviceType,
		sampleSourceDeviceOSColumn,
		sampleSourceCountryNameColumn,
		sampleSourceCityNameColumn,
		sampleSourceEventRevenueColumn,
		sampleSourceTrafficSourceColumn,
		sampleSourceUserIDColumn,
		sampleSourceSessionIDColumn,
		sampleSourceEventDuration,
	}
)

type UserActivities struct {
	table         string
	eventID       string
	eventTime     time.Time
	eventType     string
	deviceID      string
	deviceType    string
	deviceOS      string
	countryName   string
	cityName      string
	eventRevenue  float64
	trafficSource string
	userID        int64
	sessionID     string
	eventDuration float64
}

func (u *UserActivities) TableName() abstract.TableID {
	return abstract.TableID{
		Namespace: "",
		Name:      u.table,
	}
}

func (u *UserActivities) ToChangeItem(offset int64) abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:          0,
		LSN:         uint64(offset),
		CommitTime:  uint64(u.eventTime.UnixNano()),
		Counter:     0,
		Kind:        abstract.InsertKind,
		Schema:      "",
		Table:       u.table,
		PartID:      "",
		TableSchema: userActivitiesColumnSchema,
		ColumnNames: userActivitiesColumns,
		ColumnValues: []interface{}{
			u.eventID,
			u.eventTime,
			u.eventType,
			u.deviceID,
			u.deviceType,
			u.deviceOS,
			u.countryName,
			u.cityName,
			u.eventRevenue,
			u.trafficSource,
			u.userID,
			u.sessionID,
			u.eventDuration,
		},
		OldKeys: abstract.EmptyOldKeys(),
		TxID:    "",
		Query:   "",
		// removing table string size because it is not added in column values
		Size: abstract.RawEventSize(util.SizeOfStruct(*u) - uint64(len(u.table))),
	}
}

func NewUserActivities(table string) *UserActivities {
	return &UserActivities{
		table:         table,
		eventID:       generateCustomID(generateCustomIDLength),
		eventTime:     time.Now(),
		eventType:     gofakeit.RandomString(userActivitiesMarshaledData.EventTypes),
		deviceID:      gofakeit.UUID(),
		deviceType:    gofakeit.RandomString(userActivitiesMarshaledData.DeviceTypes),
		deviceOS:      gofakeit.RandomString(userActivitiesMarshaledData.DeviceOS),
		countryName:   gofakeit.Country(),
		cityName:      gofakeit.City(),
		eventRevenue:  gofakeit.Price(0, 1000),
		trafficSource: gofakeit.RandomString(userActivitiesMarshaledData.TrafficSources),
		userID:        gofakeit.Int64(),
		sessionID:     gofakeit.MacAddress(),
		eventDuration: gofakeit.Float64Range(1, 300),
	}
}
