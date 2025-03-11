package samples

import (
	_ "embed"
	"time"

	"github.com/transferria/transferria/pkg/parsers"
)

var (
	//go:embed kikimr_new.json
	KikimrNewConfig []byte
	//go:embed tm_280.json
	Tm280Config []byte
	//go:embed tm_280_yql.json
	Tm280YQLConfig []byte
	//go:embed kikimr.json
	KikimrConfig []byte
	//go:embed metrika_complex.json
	MetrikaComplexConfig []byte
	//go:embed metrika.json
	MetrikaConfig []byte
	//go:embed nel_sample.json
	NelSampleConfig []byte
	//go:embed mdb.json
	MdbSampleConfig []byte
	//go:embed taxi.json
	TaxiConfig []byte
	//go:embed taxi_yql.json
	TaxiYqlConfig []byte
	//go:embed sensitive.json
	SensitiveConfig []byte
	//go:embed sensitive_disabled.json
	SensitiveDisabledConfig []byte
	//go:embed tskv_sample.json
	TskvSampleConfig []byte
	//go:embed lf_timestamps.json
	LfTimestamps []byte
	//go:embed tskv_sample_yql.json
	TskvSampleYQLConfig []byte
	//go:embed json_sample.json
	JSONSampleConfig []byte
	//go:embed json_sample_yql.json
	JSONSampleYQLConfig []byte
	//go:embed yql_complex_primary_key.json
	YQLComplexPrimaryKeyConfig []byte
	//go:embed tm-5249.json
	TM5249Config []byte
)

var (
	//go:embed metrika_complex_sample
	MetrikaComplexSampleData []byte
	//go:embed json_sample
	JSONSampleData []byte
	//go:embed tskv_sample
	TskvSampleData []byte
	//go:embed mdb
	MDBData []byte
	//go:embed sensitive_sample
	SensitiveSampleData []byte
	//go:embed logfeller_timestamps_sample
	LogfellerTimestampsData []byte
	//go:embed taxi_sample
	TaxiData []byte
	//go:embed nel_sample
	NelSampleData []byte
	//go:embed metrika_small_sample
	MetrikaSampleData []byte
	//go:embed kikimr_sample
	KikimrSampleData []byte
	//go:embed kikimr_sample_new
	KikimrSampleNewData []byte
	//go:embed tm-5249.tskv
	TM5249Data []byte
)

const (
	MdbSample               = "mdb"                // json ("Format": "json")
	MetrikaSample           = "metrika"            // tskv ("Format": "tskv")
	MetrikaBigSample        = "metrika-big"        // ???
	MetikaComplexSample     = "metrika-complex"    // tskv ("Format": "tskv")
	TaxiSample              = "taxi"               // tskv ("Format": "tskv")
	TskvSample              = "tskv-sample"        // tskv ("Format": "tskv")
	TaxiYqlSample           = "taxi-yql"           // yql_parser config, format in data: tskv but where is this knowledge??? "Format" field is absent in parser-config!!!
	SensitiveSample         = "sensitive"          // logfeller ("Format": "tskv-log-with-timestamp")
	SensitiveDisabledSample = "sensitive-disabled" // logfeller ("Format": "tskv-log-with-timestamp")
	NelSample               = "nel_sample"         // yql_parser config, format in data: tskv but where is this knowledge??? "Format" field is absent in parser-config!!!
	TM280Sample             = "TM-280"             // json ("Format": "json")
	TM280YqlSample          = "TM-280-yql"         // yql_parser ???? IS IT USED? "Format" field is absent in parser-config!!!
	KikimrSample            = "kikimr-sample"      // logfeller ("Format":"kikimr-log")
	KikimrNew               = "kikimr-New"         // logfeller ("Format":"kikimr-new-log")
	JSONSample              = "json-sample"        // json ("Format": "json")
	JSONYqlSample           = "json-sample-yql"    // yql_parser config, format in data: json but where is this knowledge?? "Format" field is absent in parser-config!!!
	TskvYqlSample           = "tskv-sample-yql"    // yql_parser config, format in data: json but where is this knowledge?? "Format" field is absent in parser-config!!!
	LogfellerTimestamps     = "lf-timestamps"      // logfeller ("Format" : "logfeller-timestamps-test-log")
	TM5249                  = "TM5249"             // tskv with embsed \n
	YQLComplexPrimaryKey    = "yql_complex_primary_key"
)

var Data = map[string]parsers.Message{
	NelSample: {
		Offset:     123,
		SeqNo:      32,
		Key:        []byte("test_source_id"),
		CreateTime: time.Now(),
		WriteTime:  time.Now(),
		Value:      NelSampleData,
		Headers:    map[string]string{"some_field": "test"},
	},
	TM280Sample: {
		Offset:     123,
		SeqNo:      32,
		Key:        []byte("test_source_id"),
		CreateTime: time.Now(),
		WriteTime:  time.Now(),
		Value: []byte(`{"request_id":960372025831085293}
{"request_id": 18446744073709551615}`),
		Headers: map[string]string{"some_field": "test"},
	},
	MetrikaSample: {
		CreateTime: time.Now(),
		Value:      MetrikaSampleData,
	},
	MetrikaBigSample: {
		CreateTime: time.Now(),
		Value:      MetrikaSampleData,
	},
	MetikaComplexSample: {
		CreateTime: time.Now(),
		Value:      MetrikaComplexSampleData,
	},
	TaxiSample: {
		CreateTime: time.Now(),
		Value:      TaxiData,
	},
	TaxiYqlSample: {
		CreateTime: time.Now(),
		Value:      TaxiData,
	},
	SensitiveSample: {
		CreateTime: time.Now(),
		Value:      SensitiveSampleData,
	},
	SensitiveDisabledSample: {
		CreateTime: time.Now(),
		Value:      SensitiveSampleData,
	},
	KikimrSample: {
		CreateTime: time.Now(),
		Value:      KikimrSampleData,
	},
	KikimrNew: {
		CreateTime: time.Now(),
		Value:      KikimrSampleNewData,
	},
	MdbSample: {
		CreateTime: time.Now(),
		Value:      MDBData,
	},
	JSONSample: {
		CreateTime: time.Now(),
		Value:      JSONSampleData,
	},
	JSONYqlSample: {
		CreateTime: time.Now(),
		Value:      JSONSampleData,
	},
	TskvSample: {
		CreateTime: time.Now(),
		Value:      TskvSampleData,
	},
	TskvYqlSample: {
		CreateTime: time.Now(),
		Value:      TskvSampleData,
	},
	LogfellerTimestamps: {
		Offset:     123,
		SeqNo:      32,
		Key:        []byte("test_source_id"),
		CreateTime: time.Now(),
		WriteTime:  time.Now(),
		Value:      LogfellerTimestampsData,
		Headers:    map[string]string{"some_field": "test"},
	},
	YQLComplexPrimaryKey: {
		CreateTime: time.Now(),
		Value:      JSONSampleData,
	},
	TM5249: {
		CreateTime: time.Now(),
		Value:      TM5249Data,
	},
}

var Configs = map[string]string{
	NelSample:               string(NelSampleConfig),
	MdbSample:               string(MdbSampleConfig),
	MetrikaSample:           string(MetrikaConfig),
	MetikaComplexSample:     string(MetrikaComplexConfig),
	TaxiSample:              string(TaxiConfig),
	TaxiYqlSample:           string(TaxiYqlConfig),
	SensitiveSample:         string(SensitiveConfig),
	SensitiveDisabledSample: string(SensitiveDisabledConfig),
	KikimrSample:            string(KikimrConfig),
	KikimrNew:               string(KikimrNewConfig),
	TM280YqlSample:          string(Tm280YQLConfig),
	TM280Sample:             string(Tm280Config),
	JSONSample:              string(JSONSampleConfig),
	JSONYqlSample:           string(JSONSampleYQLConfig),
	TskvSample:              string(TskvSampleConfig),
	TskvYqlSample:           string(TskvSampleYQLConfig),
	LogfellerTimestamps:     string(LfTimestamps),
	YQLComplexPrimaryKey:    string(YQLComplexPrimaryKeyConfig),
	TM5249:                  string(TM5249Config),
}

// util
