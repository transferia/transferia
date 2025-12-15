# Supported debezium serializer settings


## data-transfer specific settings

Настройки, начинающиеся с префикса "dt." - это data-transfer специфичные возможности, которые vanilla debezium не даёт возможности настроить.


### dt.batching.max.size

настройка актуально только в том случае, если используется json-сериализация со schema-registry
настройка, будучи выставленной в не-ноль - включает батчинг, и задает верхнюю границу батчей в байтах. В одно сообщение очереди помещаются последовательно N логических сообщений, без каких-либо дополнительных байт, и без каких-либо новых форматов. Данные, сериализованные таким образом, возможно однозначно декодировать, но только при использовании сериализации в JSON, а без использования схема-регистри это теряет смысл.
зачем это сделано - когда используется схема-регистри, сообщения становятся очень маленькими, и клиенты/протоколы как минимум некоторых очередей (logbroker/yds) на наших тестах показывали в 2.5 раза больше перфоманса, если маленькие сообщения сбатчевать.
когда может быть полезно - когда вы оптимизируете тяжелую поставку в очередь, из которой читаете дата-трансфером
как оно работает - сообщения накапливаются в буфер, когда новое сообщение превысит значение dt.batching.max.size - имеющийся буфер сохраняется, и новое сообщение добавляется в пустой буфер. Этот батчинг происходит до компрессии в клиенте очереди. Также настройка не должна влиять на latency - условно говоря, если dt.batching.max.size=1мб, и прилетает 1.5мб, мы просто сформируем два батча и тут же отправим.
во сколько рекомендуется выставлять - мы исторически выставляли в 1 мегабайт (1048576). Тут, конечно, можно поэкспериментировать, и крутить сколько лимиты очередей позволяют, но наши эксперименты показали, что важнее наличие батчинга, чем конкретные значения верхней границы. Поэтому 1 мегабайт на наш вкус выглядит вполне обоснованным.
по умолчанию - ноль (т.е. батчинг выключен)
внимание - сообщения, прошедшие через батчинг, могут быть прочитаны только дата-трансфером. Также неизвестно будет ли это работать с кафкой, поскольку в кафке у мессаджей есть ключи, а для этого подхода концепция ключа становится достаточно бессмысленной.


### dt.unknown.types.policy

Vanilla debezium postgres connector игнорирует user-defined types - он просто молча теряет эти колонки. Data-transfer стремится никогда не терять молча данные - поэтому по умолчанию, встречая user-defined types, data-transfer debezium serializer падает (это значение "fail"). Но если хочется достичь поведения как у vanilla debezium - можно выставить значение "skip".
Появилось и третье возможное значение: to_string - на случай, если хочется получать user-defined types строкой.
Возможные значения: "fail", "skip", "to_string"
Значение по умолчанию: "fail"


### dt.add.original.type.info

Vanilla debezium сужает типы необратимым образом. Data-transfer позволяет добавлять в debezium-мессадже информацию об исходных типах данных - что позволяет при желании полностью восстановить исходные данные (единственное найденное исключение - постгресовые типы with time zone - тут timezone теряется безвозвратно)
Возможные значения: "true", "false"
Значение по умолчанию: "false"


### dt.source.type

Служебное поле, выставляемое автоматически. Необходимо, чтобы генерировать полностью debezium-like сообщения:
- Поле payload.source заполнено по-разному у pg и у mysql (и соответственно поле "schema" должно описать соответствующие поля корректно)
- На сообщениях op='d', mysql (в отличии от postgres) в поле before содержит значениях всех колонок удаляемой строки


### dt.mysql.timezone

В mysql тип данных timestamp под капотом хранится в UTC, но каждый раз при подключении клиента, для сессии выводится timezone, и все timestamp конвертятся в эту timezone.
Vanilla debezium всегда выдаёт в debezium-мессадже значения типа mysql:timestamp, приведенные к UTC.
Пользователь может переопределить это, указав любую другую таймзону в формате IANA. Например, если пользователь работает в MSK таймзоне и захочет получать debezium-мессаджи с mysql:timestamp в MSK таймзоне - через эту настройку он может это сделать.
Эта настройка всегда перетирает настройку таймзоны mysql-эндпоинта источника.
Возможные значения: любые значения из IANA timezone database
Значение по умолчанию: "UTC"


## supported debezium settings


### database.dbname

Постгресовая настройка, содержит имя database для подключения.

Мы это заполняем сами полем Database модели pg-src.
https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-connector-properties
В debezium-мессадже представлено полем payload.source.db


### database.server.name

Начиная с debezium 2.0 было переименовано в topic.prefix - мы это сделаем в тикете TM-5128. По сути оно и является topic.prefix - обычно debezium message пишется в топик, названный что-то вроде "a.b.c", где
- a: topix.prefix
- b: schema name (or database name)
- c: table name

Мы это заполняем из queue-dst-модели, если пользователь задал topic_prefix. В очередях у нас есть развилка:
- Пользователь может указать либо topic_prefix - и тогда мы ведем себя как vanilla debezium - для каждой таблицы имеется отдельный топик.
- Пользователь может указать full_topic_path - тогда мы события всех таблиц пишем в один топик. Это полезнее всего в кейсе save_tx_order=true - тогда все события идут в порядке транзакций, и пользователь может обрабатывать все события всех таблиц одной транзакции.

save_tx_order же настраиваема потому, что:
- Режим save_tx_order=true форсит количество воркеров и потоков в 1 - поскольку тут важен именно порядок событий, который невозможно распараллелить. Если разрешать пользователю выставить значение >1 в случае логброкер-приемника, например, это приведет к ошибкам "более одного продьюсера с одним sourceID".
- Режим save_tx_order=false нужен в случаях, когда в очередь заливается снапшот - тогда параллелизм возможен, и save_tx_order=true будет только мешать, ограничивая поставку одной партицией.

https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-connector-properties
https://debezium.io/documentation/reference/stable/connectors/mysql.html
В debezium-мессадже представлено полем payload.source.name


### decimal.handling.mode

Как в debezium message представимы DECIMAL'ы. По умолчанию это сделано вырвиглазно: это bigint в based64, который представляет правильную последовательность десятичных цифр, а в схеме задается оффсет, где должна стоять точка.

Например вот как это выглядит в payload:

```
"DECIMAL_5":"W5s="
```
И вот как выглядит описание этого поля в schema:
```
{
    "field":"DECIMAL_5",
    "name":"org.apache.kafka.connect.data.Decimal",
    "optional":true,
    "parameters":{"connect.decimal.precision":"5","scale":"0"},
    "type":"bytes",
    "version":1
}
```

Никто в здравом уме не хочет это парсить, поэтому есть еще 2 варианта:
- string - самый предпочтительный вариант. Потери данных точно нет, и DECIMAL просто представляется строкой
- double - тут на больших значениях возможна потеря данных, но некоторые пользователи гарантированно носят небольшие данные в DECIMAL колонках, и хотят работать со значениями как с double'ами.

Возможные значения: "precise", "double", "string"
Значение по умолчанию: "precise"
Поддержанные источники: pg, mysql


### interval.handling.mode

postgres-специфичная настройка, достаточно описана в debezium документации:
https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-connector-properties
Возможные значения: "numeric", "string"
Значение по умолчанию: "numeric"

### unavailable.value.placeholder
Значение для TOAST'ed колонок. По умолчанию: __debezium_unavailable_value

### money.fraction.digits
Существует в виде заглушки) как понадобится - реализуем

### binary.handling.mode
Существует в виде заглушки) как понадобится - реализуем

### time.precision.mode
Существует в виде заглушки) как понадобится - реализуем

### hstore.handling.mode
Существует в виде заглушки) как понадобится - реализуем

### tombstones.on.delete

Позволяет не генерировать tombstone-евенты на delete'ах.

### key.converter
см value.converter

### key.converter.schemas.enable
см value.converter.schemas.enable

### key.converter.schema.registry.url
см value.converter.schema.registry.url

### key.converter.basic.auth.credentials.source
см value.converter.basic.auth.credentials.source

### key.converter.basic.auth.user.info
см value.converter.basic.auth.user.info

### key.converter.ssl.ca
см value.converter.ssl.ca

### value.converter
Возможные значения:
- org.apache.kafka.connect.json.JsonConverter - дефолтное значение - дефолтные debezium json
- io.confluent.connect.json.JsonSchemaConverter - для использования confluent schema registry

### value.converter.schemas.enable
Настройка конвертера org.apache.kafka.connect.json.JsonConverter
Возможные значения:
- true - дефолтное значение - в каждое сообщение включается описание схемы в ключе "schema"
- false - ключ "schema" не включается в сообщение, и содержимое ключа "payload" вытаскивается на более верхний уровень

### value.converter.schema.registry.url
Настройка конвертера io.confluent.connect.json.JsonSchemaConverter
Эта настройка нужна для использования confluent schema registry - в SR сохраняется схема.

### value.converter.basic.auth.credentials.source
Возможное значение: USER_INFO
Нужно только для совместимости с дебезиумом, функциональной нагрузки не несёт.

### value.converter.basic.auth.user.info
login:password через двоеточие для авторизации с confluent schema registry

### value.converter.ssl.ca
Содержимое SSL сертификата confluent schema registry
Если не указан - просто не проверяется
