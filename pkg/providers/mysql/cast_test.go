//go:build !disable_mysql_provider

package mysql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

/*
QUERY:
CREATE TABLE `all_data_types` (
                                              -- If you specify ZEROFILL for a numeric column, MySQL automatically adds the UNSIGNED attribute to the column.
    `tinyint`     TINYINT,
    `tinyint_def` TINYINT DEFAULT 0,
    `tinyint_u`   TINYINT UNSIGNED,
    `tinyint_z`   TINYINT ZEROFILL,
    `smallint`    SMALLINT,
    `smallint_u`  SMALLINT UNSIGNED,
    `smallint_z`  SMALLINT ZEROFILL,
    `mediumint`   MEDIUMINT,
    `mediumint_u` MEDIUMINT UNSIGNED,
    `mediumint_z` MEDIUMINT ZEROFILL,
    `int`         INT,
    `int_u`       INT UNSIGNED,
    `int_z`       INT ZEROFILL,
    `bigint`      BIGINT,
    `bigint_u`    BIGINT UNSIGNED,
    `bigint_z`    BIGINT ZEROFILL,

    `bool` BOOL,                              -- synonym to TINYINT(1)

    `decimal_10_2` DECIMAL(10, 2),            -- synonyms: decimal, dec, numeric, fixed
    `decimal_65_30` DECIMAL(65, 30),
    `decimal_65_0` DECIMAL(65, 0),
    `dec`     DEC,
    `numeric` NUMERIC(11, 3),
    `fixed`   FIXED,

                                              -- "As of MySQL 8.0.17, the UNSIGNED attribute is deprecated for columns of type FLOAT, DOUBLE, and DECIMAL (and any synonyms); you should expect support for it to be removed in a future version of MySQL."
    `float`            FLOAT(10, 2),          -- "As of MySQL 8.0.17, the nonstandard FLOAT(M,D) and DOUBLE(M,D) syntax is deprecated and you should expect support for it to be removed in a future version of MySQL."
    `float_z`          FLOAT(10, 2) ZEROFILL, -- same
    `float_53`         FLOAT(53),             -- same
    `real`             REAL(10, 2),           -- same && synonym to FLOAT
    `double`           DOUBLE,
    `double_precision` DOUBLE PRECISION,

    `bit`    BIT,
    `bit_5`  BIT(5),
    `bit_9`  BIT(9),
    `bit_64` BIT(64),

    `date`        DATE,
    `datetime`    DATETIME,
    `datetime_6`  DATETIME(6),
    `timestamp`   TIMESTAMP,
    `timestamp_2` TIMESTAMP(2),

    `time`   TIME,
    `time_2` TIME(2),
    `year`   YEAR,

    `char`        CHAR(10),
    `varchar`     VARCHAR(20),
    `varchar_def` VARCHAR(20) DEFAULT 'default_value',

    `binary`    BINARY(20),
    `varbinary` VARBINARY(20),

    `tinyblob`   TINYBLOB,
    `blob`       BLOB,
    `blob_def`   BLOB DEFAULT ('default_value'),
    `mediumblob` MEDIUMBLOB,
    `longblob`   LONGBLOB,

    `tinytext` TINYTEXT  ,
    `text` TEXT,
    `mediumtext` MEDIUMTEXT  ,
    `longtext` LONGTEXT  ,

    `enum` ENUM('1', '2', '3'),
    `set`  SET ('1', '2', '3'),

    -- spatial

    `geometry`    GEOMETRY SRID 4326,
    `coordinates` POINT,
    `linestring`  LINESTRING,
    `boundary`    POLYGON,

    `multipoint`         MULTIPOINT,
    `multilinestring`    MULTILINESTRING,
    `multipolygon`       MULTIPOLYGON,
    `geometrycollection` GEOMETRYCOLLECTION,

    -- json

    `json` JSON
)

#------------------------------------------------------------
# max/min
# - float & double - not really max&min
# - timestamp - shifted on 3h bcs of timezone
# not tested:
# - decimal max/min
# - varchar/binary/blob max length
# - enum/set - don't have min/max
# - all spatial types
# notes:
# - '-838:59:59' for time - block by jdbc (`time` & `time_2`)

INSERT INTO all_data_types
(`tinyint`, `tinyint_def`, `tinyint_u`, `tinyint_z`, `smallint`, `smallint_u`, `smallint_z`, `mediumint`, `mediumint_u`, `mediumint_z`, `int`       , `int_u`    , `int_z`    , `bigint`             , `bigint_u`           , `bigint_z`           ,  `bool`, `decimal_10_2` ,`decimal_65_30`, `decimal_65_0`, `dec`          , `numeric`      , `float`          , `float_z`, `float_53`, `real`, `double`                 , `double_precision`, `bit`, `bit_5`, `bit_9`, `bit_64`, `date`       , `datetime`            , `datetime_6`          , `timestamp`           , `timestamp_2`         ,  `time`      , `time_2`     , `year`, `char`, `varchar`, `varchar_def`, `binary`, `varbinary`, `tinyblob`, `blob`, `blob_def`, `mediumblob`, `longblob`, `tinytext`, `text`, `mediumtext`, `longtext`, `enum` , `set` , `geometry`, `coordinates`, `linestring`, `boundary`, `multipoint`, `multilinestring`, `multipolygon`, `geometrycollection`, `json`                            )
VALUES
(-128	  , -128         , 0          , 0          , -32768    , 0           , 0           , -8388608   , 0            , 0            , -2147483648 , 0          , 0          , -9223372036854775808 , 0                    , 0                    , 0      , '3.50'         , NULL          , NULL          , '3.50'         , '3.50'         , 1.175494351E-38  , NULL     , NULL      , NULL  , -1.7976931348623157E+308 , NULL              , 0    , 0      , NULL   , NULL    , '1970-01-01' , '1000-01-01 00:00:00' , '1000-01-01 00:00:00' , '1970-01-01 03:00:01' , '1970-01-01 03:00:01' , '-838:59:59' , '-838:59:59' , '1901', 0     , ''       , ''           , ''      , ''         , ''        , ''    , ''        , ''          , ''        , ''        , ''    , ''          , ''        , '1'    , '1'   , NULL      , NULL         , NULL        , NULL      , NULL        , NULL             , NULL          , NULL                , '{}'                              )
,
(127      , 127          , 255        , 255        , 32767     , 65535       , 65535       , 8388607    , 16777215     , 16777215     , 2147483647  , 4294967295 , 4294967295 , 9223372036854775807  , 18446744073709551615 , 18446744073709551615 , 1      , '12345678.1'   , NULL          , NULL          , '12345678.1'   , '12345678.1'   , 3.402823466E+7   , NULL     , NULL      , NULL  , -2.2250738585072014E-308 , NULL              , 1    , 31     , NULL   , NULL    , '2038-01-19' , '9999-12-31 23:59:59' , '9999-12-31 23:59:59' , '2038-01-19 03:14:07' , '2038-01-19 03:14:07' , '838:59:59'  , '838:59:59'  , '2155', 255   , NULL     , NULL         , NULL    , NULL       , NULL      , NULL  , NULL      , NULL        , NULL      , NULL      , NULL  , NULL        , NULL      , '3'    , '3'   , NULL      , NULL         , NULL        , NULL      , NULL        , NULL             , NULL          , NULL                , '{"a":"b", "c":1, "d":{}, "e":[]}')
;

*/

type schemaTestCase struct {
	dataType   string
	columnType string
	ytType     schema.Type
}

var globalArray = []schemaTestCase{
	// DATA_TYPE, COLUMN_TYPE, expected YT_TYPE
	{"bigint", "bigint(20)", schema.TypeInt64},
	{"bigint", "bigint(20) unsigned", schema.TypeUint64},
	{"bigint", "bigint(20) unsigned zerofill", schema.TypeUint64},
	{"binary", "binary(20)", schema.TypeBytes},
	{"bit", "bit(1)", schema.TypeBytes},
	{"bit", "bit(5)", schema.TypeBytes},
	{"bit", "bit(64)", schema.TypeBytes},
	{"bit", "bit(9)", schema.TypeBytes},
	{"blob", "blob", schema.TypeBytes},
	{"blob", "blob", schema.TypeBytes}, // DEFAULT value doesn't affect DATA_TYPE & COLUMN_TYPE
	{"tinyint", "tinyint(1)", schema.TypeInt8},
	{"polygon", "polygon", schema.TypeBytes},
	{"char", "char(10)", schema.TypeString},
	{"point", "point", schema.TypeBytes},
	{"date", "date", schema.TypeDate},
	{"datetime", "datetime", schema.TypeTimestamp},
	{"datetime", "datetime(6)", schema.TypeTimestamp},
	{"decimal", "decimal(10,0)", schema.TypeFloat64}, // dec
	{"decimal", "decimal(10,0)", schema.TypeFloat64}, // fixed
	{"decimal", "decimal(10,2)", schema.TypeFloat64},
	{"decimal", "decimal(65,0)", schema.TypeFloat64},
	{"decimal", "decimal(65,30)", schema.TypeFloat64},
	{"double", "double", schema.TypeFloat64},
	{"double", "double", schema.TypeFloat64}, // PRECISION doesn't affect DATA_TYPE & COLUMN_TYPE
	{"enum", "enum('1','2','3')", schema.TypeString},
	{"float", "float(10,2)", schema.TypeFloat64},
	{"double", "double", schema.TypeFloat64},
	{"float", "float(10,2) unsigned zerofill", schema.TypeFloat64},
	{"geometry", "geometry", schema.TypeBytes},
	{"geomcollection", "geomcollection", schema.TypeBytes},
	{"int", "int(11)", schema.TypeInt32},
	{"int", "int(10) unsigned", schema.TypeUint32},
	{"int", "int(10) unsigned zerofill", schema.TypeUint32},
	{"json", "json", schema.TypeAny},
	{"linestring", "linestring", schema.TypeBytes},
	{"longblob", "longblob", schema.TypeBytes},
	{"longtext", "longtext", schema.TypeString},
	{"mediumblob", "mediumblob", schema.TypeBytes},
	{"mediumint", "mediumint(9)", schema.TypeInt32},
	{"mediumint", "mediumint(8) unsigned", schema.TypeUint32},
	{"mediumint", "mediumint(8) unsigned zerofill", schema.TypeUint32},
	{"mediumtext", "mediumtext", schema.TypeString},
	{"multilinestring", "multilinestring", schema.TypeBytes},
	{"multipoint", "multipoint", schema.TypeBytes},
	{"multipolygon", "multipolygon", schema.TypeBytes},
	{"decimal", "decimal(11,3)", schema.TypeFloat64},
	{"double", "double(10,2)", schema.TypeFloat64},
	{"set", "set('1','2','3')", schema.TypeString},
	{"smallint", "smallint(6)", schema.TypeInt16},
	{"smallint", "smallint(5) unsigned", schema.TypeUint16},
	{"smallint", "smallint(5) unsigned zerofill", schema.TypeUint16},
	{"text", "text", schema.TypeString},
	{"time", "time", schema.TypeString},
	{"time", "time(2)", schema.TypeString},
	{"timestamp", "timestamp", schema.TypeTimestamp},
	{"timestamp", "timestamp(2)", schema.TypeTimestamp},
	{"tinyblob", "tinyblob", schema.TypeBytes},
	{"tinyint", "tinyint(4)", schema.TypeInt8},
	{"tinyint", "tinyint(4)", schema.TypeInt8}, // DEFAULT value doesn't affect DATA_TYPE & COLUMN_TYPE
	{"tinyint", "tinyint(3) unsigned", schema.TypeUint8},
	{"tinyint", "tinyint(3) unsigned zerofill", schema.TypeUint8},
	{"tinytext", "tinytext", schema.TypeString},
	{"varbinary", "varbinary(20)", schema.TypeBytes},
	{"varchar", "varchar(20)", schema.TypeString},
	{"varchar", "varchar(20)", schema.TypeString}, // DEFAULT value doesn't affect DATA_TYPE & COLUMN_TYPE
	{"year", "year(4)", schema.TypeString},
}

func TestAll(t *testing.T) {
	for _, el := range globalArray {
		ytType := typeToYt(el.dataType, el.columnType)
		require.Equal(t, ytType, el.ytType, fmt.Sprintf("inputs: %s, %s", el.dataType, el.columnType))

		inferredDataType := abstract.TrimMySQLType(el.columnType)
		require.Equal(t, inferredDataType, el.dataType, fmt.Sprintf("columnType: %s", el.columnType))
	}
}
