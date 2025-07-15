## Type System Definition for YDB


### YDB Source Type Mapping

| YDB TYPES | TRANSFER TYPE |
| --- | ----------- |
|Int64|int64|
|Int32|int32|
|Int16|int16|
|Int8|int8|
|Uint64|uint64|
|Uint32|uint32|
|Uint16|uint16|
|Uint8|uint8|
|Float|float|
|Double|double|
|String|string|
|Decimal<br/>DyNumber<br/>Utf8<br/>Uuid|utf8|
|Bool|boolean|
|Date|date|
|Datetime|datetime|
|Timestamp|timestamp|
|REST...|any|



### YDB Target Type Mapping

| TRANSFER TYPE | YDB TYPES |
| --- | ----------- |
|int64|Int64|
|int32|Int32|
|int16|Int32|
|int8|Int32|
|uint64|Uint64|
|uint32|Uint32|
|uint16|Uint32|
|uint8|Uint8|
|float|N/A|
|double|Double|
|string|String|
|utf8|Utf8|
|boolean|Bool|
|date|Date|
|datetime|Datetime|
|timestamp|Timestamp|
|any|Json|
