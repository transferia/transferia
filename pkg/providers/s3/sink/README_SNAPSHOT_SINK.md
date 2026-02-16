# SnapshotSink - Компонент загрузки снапшотов в S3

## Обзор

**SnapshotSink** — это специализированный компонент для загрузки полных снапшотов таблиц в S3-совместимое хранилище. Он обрабатывает начальную полную загрузку данных (snapshot-трансферы) в отличие от [`ReplicationSink`](replication_sink.go), который обрабатывает инкрементальные изменения.

## Что такое SnapshotWriter

[`snapshotWriter`](snapshot_writer.go) — это ключевой компонент, который управляет жизненным циклом записи данных снапшота в S3. Он координирует три основных процесса:

1. **Сериализацию данных** - преобразование [`abstract.ChangeItem`](../../abstract/change_item.go) в нужный формат (JSON, CSV, Parquet, Raw)
2. **Потоковую передачу** - передача данных через pipe в S3 uploader
3. **Синхронизацию** - координация между процессом записи и асинхронной загрузкой в S3

## Как работает SnapshotSink

### Архитектура потока данных

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. InitTableLoad Event                                              │
│    Получено событие начала загрузки таблицы                         │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 2. Первый InsertKind Event                                          │
│    • Вызывается initSnapshotLoaderIfNotInited(fullTableName, ref)   │
│    • Создается io.Pipe() для потоковой передачи                     │
│    • Создается snapshotWriter с:                                    │
│      - BatchSerializer (JSON/CSV/Parquet/Raw)                       │
│      - Writer (с опциональным сжатием gzip/zlib)                    │
│      - ctx/cancel для управления контекстом                         │
│      - uploadDone канал для синхронизации                           │
│      - uploadOnce для безопасного завершения                        │
│    • Запускается горутина с s3Client.Upload()                       │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 3. Последующие InsertKind Events                                    │
│    • Данные сериализуются через snapshotWriter.write()              │
│    • SerializeAndWrite записывает в Writer                          │
│    • Writer (опционально сжимает и) записывает в pipeWriter         │
│    • S3 uploader читает из pipeReader и загружает в S3              │
│    • Процесс полностью потоковый - нет полной буферизации           │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 4. Разбиение на файлы (если MaxItemsPerFile или MaxBytesPerFile)    │
│    • FileSplitter.addItems() определяет сколько строк поместится    │
│    • При достижении лимита writeChunkAndRotate():                    │
│      - Записывает то, что помещается в текущий файл                │
│      - Вызывает rotateFile():                                       │
│        · Закрывается текущий snapshotWriter                        │
│        · Создается новый pipe и snapshotWriter                     │
│      - Оставшиеся элементы записываются в новый файл               │
│    • Счетчик файлов с 5-значным номером: .00000, .00001, .00002... │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 5. DoneTableLoad Event                                               │
│    • Вызывается doneTableLoad() → snapshotWriter.close()            │
│    • Закрывается serializer (получаем последние байты)              │
│    • Закрывается writer (flush всех данных, закрытие pipe)          │
│    • Блокируется на чтении из uploadDone канала                     │
│    • Возвращается результат загрузки (успех или ошибка)             │
│    • snapshotWriter сбрасывается в nil                              │
└─────────────────────────────────────────────────────────────────────┘
```

### Ключевые особенности

1. **Потоковая передача без буферизации** - данные передаются напрямую в S3 через pipe, минимизируя использование памяти
2. **Асинхронная загрузка** - загрузка в S3 происходит в отдельной горутине параллельно с записью данных
3. **Разбиение на файлы** - поддержка разбиения больших таблиц на несколько файлов через [`FileSplitter`](file_splitter.go) по количеству строк и/или объёму байт
4. **Гибкая сериализация** - поддержка JSON, CSV, Parquet, Raw форматов
5. **Опциональное сжатие** - gzip или zlib сжатие через [`Writer`](writer/writer.go)

## Соотношение snapshotWriter и SnapshotSink

```
┌──────────────────────────────────────────────────────────────────┐
│                         SnapshotSink                             │
│  (Главный оркестратор snapshot-трансферов)                       │
│                                                                  │
│  Обязанности:                                                    │
│  • Обработка событий жизненного цикла (Init/Done TableLoad)      │
│  • Разделение входных ChangeItem на типы (processItemsAndReturn  │
│    Inserts) и обработка Drop/Truncate/DDL событий                │
│  • Управление FileSplitter для разбиения файлов                  │
│  • Создание и управление snapshotWriter экземплярами             │
│  • Генерация S3 ключей через s3ObjectRef                        │
│  • Ротация файлов через writeChunkAndRotate/rotateFile           │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │              snapshotWriter (текущий)                      │ │
│  │  (Управляет одной загрузкой файла)                         │ │
│  │                                                            │ │
│  │  Обязанности:                                              │ │
│  │  • Сериализация данных через BatchSerializer               │ │
│  │  • Запись в Writer (с сжатием)                             │ │
│  │  • Управление контекстом (ctx/cancel)                      │ │
│  │  • Синхронизация с асинхронной загрузкой                   │ │
│  │  • Финализация и ожидание завершения                       │ │
│  │                                                            │ │
│  │  ┌──────────────────────────────────────────────────────┐ │ │
│  │  │         BatchSerializer                              │ │ │
│  │  │  (JSON/CSV/Parquet/Raw)                              │ │ │
│  │  └──────────────────────────────────────────────────────┘ │ │
│  │                          ↓                                 │ │
│  │  ┌──────────────────────────────────────────────────────┐ │ │
│  │  │         Writer (writer/writer.go)                    │ │ │
│  │  │  (Опциональное gzip/zlib сжатие)                     │ │ │
│  │  └──────────────────────────────────────────────────────┘ │ │
│  │                          ↓                                 │ │
│  │  ┌──────────────────────────────────────────────────────┐ │ │
│  │  │         io.Pipe (pipeWriter)                         │ │ │
│  │  └──────────────────────────────────────────────────────┘ │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │              FileSplitter                                  │ │
│  │  (Управляет разбиением на файлы)                           │ │
│  │                                                            │ │
│  │  • Отслеживает количество строк и байт в файле            │ │
│  │  • Управляет счётчиком файлов для каждого s3ObjectRef     │ │
│  │  • Определяет сколько элементов помещается в текущий файл  │ │
│  │  • Поддерживает лимиты: MaxItemsPerFile и MaxBytesPerFile │ │
│  └────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
                              ↓
                    ┌─────────────────────┐
                    │   io.Pipe (reader)  │
                    └─────────────────────┘
                              ↓
                    ┌─────────────────────┐
                    │  s3Client.Upload    │
                    │  (Горутина)         │
                    └─────────────────────┘
                              ↓
                    ┌─────────────────────┐
                    │    Object Storage   │
                    └─────────────────────┘
```

### Взаимодействие компонентов

1. **SnapshotSink** получает батчи [`abstract.ChangeItem`](../../abstract/change_item.go) через метод [`Push()`](snapshot_sink.go)
2. **processItemsAndReturnInserts()** разделяет события по типам: InsertKind собирает, остальные обрабатывает (Drop, Truncate, Init/Done TableLoad, DDL)
3. **processSnapshot()** инициирует запись InsertKind элементов в цикле через **writeChunkAndRotate()**
4. **FileSplitter.addItems()** определяет сколько элементов помещается в текущий файл
5. **snapshotWriter** использует **BatchSerializer.SerializeAndWrite()** для потоковой сериализации и записи
6. **Writer** применяет опциональное сжатие (gzip/zlib) и записывает в **io.Pipe**
7. **s3Client.Upload** в отдельной горутине читает из pipe и загружает в S3
8. При достижении лимита **rotateFile()** закрывает текущий writer и создаёт новый

## Компоненты экосистемы

### 1. SnapshotSink (snapshot_sink.go)

**Главный оркестратор** snapshot-трансферов.

**Ключевые поля:**

- `transferID` — идентификатор трансфера
- `operationTimestamp` — временная метка операции (строка)
- `s3Client` — клиент для работы с S3
- `cfg` — конфигурация S3Destination
- `snapshotWriter` — текущий writer (nil, если не инициализирован)
- `fileSplitter` — менеджер разбиения на файлы
- `logger`, `metrics` — логирование и метрики

**Ключевые методы:**

- [`Push(input []abstract.ChangeItem)`](snapshot_sink.go) — главная точка входа
- [`processItemsAndReturnInserts(input []abstract.ChangeItem)`](snapshot_sink.go) — разделение событий по типам, возвращает `[]*abstract.ChangeItem`
- [`processSnapshot(insertItems []*abstract.ChangeItem)`](snapshot_sink.go) — обработка данных снапшота в цикле
- [`writeChunkAndRotate(ref, table, items)`](snapshot_sink.go) — запись порции данных и ротация файла при достижении лимита
- [`writeBatch(items, table)`](snapshot_sink.go) — сериализация и запись батча
- [`rotateFile(ref, table)`](snapshot_sink.go) — закрытие текущего файла и создание нового
- [`initSnapshotLoaderIfNotInited(fullTableName, ref)`](snapshot_sink.go) — инициализация writer и загрузки
- [`initPipe(fullTableName, ref, keyPartNumber)`](snapshot_sink.go) — создание pipe, writer, горутины загрузки
- [`makeS3ObjectRef(row)`](snapshot_sink.go) — создание s3ObjectRef из ChangeItem
- [`doneTableLoad()`](snapshot_sink.go) — финализация загрузки таблицы
- [`Close()`](snapshot_sink.go) — закрытие sink

### 2. snapshotWriter (snapshot_writer.go)

**Менеджер жизненного цикла** записи одного файла снапшота.

**Ключевые поля:**

- `ctx` / `cancel` — контекст для управления горутиной загрузки
- `key` — S3 ключ файла
- `serializer` — `serializer.BatchSerializer` для преобразования данных
- `writer` — `io.WriteCloser` (с опциональным сжатием)
- `uploadDone` — канал для получения результата загрузки
- `uploadOnce` — `sync.Once` для безопасного однократного завершения

**Ключевые методы:**

- [`write(items []*abstract.ChangeItem) (int, error)`](snapshot_writer.go) — сериализация и запись батча через `SerializeAndWrite(ctx, items, writer)`
- [`close() error`](snapshot_writer.go) — финализация: закрытие serializer, запись последних байт, закрытие writer, ожидание `uploadDone`
- [`finishUpload(err error)`](snapshot_writer.go) — сигнал о завершении загрузки через `uploadOnce.Do()`, отправка результата в `uploadDone` и вызов `cancel()`

### 3. FileSplitter (file_splitter.go)

**Управляет разбиением** больших таблиц на несколько файлов.

**Ключевые поля:**

- `counterByRef` — счётчик файлов для каждого s3ObjectRef
- `rowsByRef` — количество строк в текущем файле
- `bytesByRef` — количество байт в текущем файле
- `maxItemsPerFile` — лимит строк на файл
- `maxBytesPerFile` — лимит байт на файл

**Ключевые методы:**

- [`addItems(ref s3ObjectRef, items []*abstract.ChangeItem) int`](file_splitter.go) — определяет сколько элементов помещается в текущий файл, учитывая лимиты строк и байт; всегда пропускает минимум 1 элемент на файл
- [`increaseKey(ref s3ObjectRef) string`](file_splitter.go) — инициализирует (при первом вызове) или инкрементирует счётчик, сбрасывает трекеры строк/байт, возвращает полный S3 ключ
- [`key(ref s3ObjectRef) string`](file_splitter.go) — возвращает текущий полный S3 ключ
- [`keyNumber(ref s3ObjectRef) int`](file_splitter.go) — возвращает текущий номер счётчика

### 4. Writer (writer/writer.go)

**Обёртка для сжатия** данных перед записью в pipe.

**Поддерживаемые кодировки:**

- `GzipEncoding` (`"GZIP"`) — gzip сжатие
- `ZlibEncoding` (`"ZLIB"`) — zlib сжатие
- `NoEncoding` (`"UNCOMPRESSED"`) — без сжатия

**Особенности:**

- При закрытии сначала закрывает внутренний writer (flush сжатых данных), затем закрывает обёрнутый closer (pipeWriter)

### 5. s3ObjectRef (s3_object_ref.go)

**Представляет ссылку** на логический файловый поток в S3. Файлы одного потока разделяют namespace, tableName, partID и т.д.; отличаются только номером (counter) через FileSplitter.

**Ключевые поля:**

- `layout` — префикс директории
- `namespace`, `tableName` — идентификаторы таблицы
- `partID`, `partIDHash` — идентификатор партиции и его MD5-хеш (8 символов)
- `timestamp` — временная метка операции
- `outputFormat` — формат данных
- `encoding` — кодировка/сжатие

**Ключевые методы:**

- [`fullKey(counter int) string`](s3_object_ref.go) — генерирует полный S3 ключ: `[<layout>/]<namespace>/<table_name>/part-<timestamp>-<partIDHash>.<NNNNN>.<format>[.gz]`
- [`basePath() string`](s3_object_ref.go) — возвращает базовый путь: `<namespace>/<table_name>` или просто `<table_name>`

### 6. s3Client (s3_client.go)

**Интерфейс для работы с S3**, позволяет мокировать в тестах.

```go
type s3Client interface {
    Upload(input *s3manager.UploadInput) (*s3manager.UploadOutput, error)
    DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
}
```

Реализация `s3ClientImpl` оборачивает `*s3.S3` (для DeleteObject) и `*s3manager.Uploader` (для Upload).

### 7. BatchSerializer (из pkg/serializer)

**Интерфейс сериализации** данных в различные форматы.

```go
type BatchSerializer interface {
    Serialize(items []*abstract.ChangeItem) ([]byte, error)
    SerializeAndWrite(ctx context.Context, items []*abstract.ChangeItem, writer io.Writer) (int, error)
    Close() ([]byte, error)
}
```

**Реализации:**

- `JSONBatchSerializer` — JSON Lines формат
- `CsvBatchSerializer` — CSV формат
- `ParquetBatchSerializer` — Parquet колоночный формат
- `RawBatchSerializer` — прямая передача без преобразований

## Диаграмма взаимосвязей компонентов

```
                    ┌─────────────────────────────────────────┐
                    │         abstract.ChangeItem             │
                    │    (Входные данные от источника)        │
                    └──────────────────┬──────────────────────┘
                                       │
                                       ▼
                    ┌─────────────────────────────────────────┐
                    │          SnapshotSink                   │
                    │  • Push() - главная точка входа         │
                    │  • processItemsAndReturnInserts()       │
                    │  • processSnapshot() +                  │
                    │    writeChunkAndRotate()                 │
                    │  • Обработка Init/Done/Drop/Truncate    │
                    └──────────────────┬──────────────────────┘
                                       │
                    ┌──────────────────┼──────────────────────┐
                    │                  │                      │
                    ▼                  ▼                      ▼
        ┌──────────────────┐  ┌──────────────┐  ┌──────────────────┐
        │  FileSplitter    │  │ s3ObjectRef  │  │   s3Client       │
        │  • addItems()    │  │ • fullKey()  │  │   • Upload()     │
        │  • increaseKey() │  │ • basePath() │  │   • Delete()     │
        │  • key()         │  │              │  │                  │
        │  • keyNumber()   │  │              │  │                  │
        └──────────────────┘  └──────────────┘  └──────────────────┘
                    │
                    ▼
        ┌─────────────────────────────────────────┐
        │         snapshotWriter                  │
        │  • write() - запись данных              │
        │  • close() - финализация                │
        │  • finishUpload() - сигнал завершения   │
        │  • ctx/cancel/uploadOnce                │
        └──────────────────┬──────────────────────┘
                           │
            ┌──────────────┼──────────────┐
            │              │              │
            ▼              ▼              ▼
    ┌──────────────┐ ┌──────────┐ ┌────────────┐
    │BatchSerializer│ │  Writer  │ │ io.Pipe    │
    │ • JSON       │ │ • GZIP   │ │ • reader   │
    │ • CSV        │ │ • ZLIB   │ │ • writer   │
    │ • Parquet    │ │ • UNCOMP │ │            │
    │ • Raw        │ │          │ │            │
    └──────────────┘ └──────────┘ └─────┬──────┘
                                         │
                                         ▼
                              ┌──────────────────┐
                              │ s3Client.Upload  │
                              │   (Горутина)     │
                              └─────────┬────────┘
                                        │
                                        ▼
                              ┌──────────────────┐
                              │   ObjectStorage  │
                              │  (Хранилище)     │
                              └──────────────────┘
```

## Стратегия именования файлов

### Формат S3 ключа

```
[{layout}/]{namespace}/{tableName}/part-{timestamp}-{partIDHash}.{NNNNN}.{format}[.gz]
```

**Компоненты:**

1. **layout** (опционально) — префикс директории из `cfg.Layout`
   - Пример: `my_prefix/subdir`
   - Может не быть задан — в этом случае опускается

2. **namespace/tableName** — путь на основе таблицы
   - Если namespace задан: `{namespace}/{tableName}`
   - Если namespace пуст: `{tableName}`

3. **part-{timestamp}-{partIDHash}** — уникальный идентификатор файла
   - `timestamp` — `operationTimestamp` трансфера (unix timestamp в виде строки)
   - `partIDHash` — первые 8 символов MD5-хеша partID (если partID пуст, хешируется строка `"default"`)

4. **NNNNN** — 5-значный номер части файла (управляется FileSplitter)
   - `00000`, `00001`, `00002`, ...

5. **format** — формат данных (в lowercase)
   - `json`, `csv`, `parquet`, `raw`

6. **.gz** — суффикс сжатия (только если `cfg.OutputEncoding == GzipEncoding`)

### Примеры

```
# Простой снапшот без разбиения
my_prefix/subdir/public/users/part-1707649200-a1b2c3d4.00000.json.gz

# Снапшот без namespace
my_prefix/subdir/orders/part-1707649200-e5f6a7b8.00000.json.gz

# Снапшот с разбиением на файлы
my_prefix/subdir/public/users/part-1707649200-a1b2c3d4.00000.json.gz
my_prefix/subdir/public/users/part-1707649200-a1b2c3d4.00001.json.gz
my_prefix/subdir/public/users/part-1707649200-a1b2c3d4.00002.json.gz

# CSV формат без сжатия
my_prefix/subdir/public/orders/part-1707649200-a1b2c3d4.00000.csv

# Parquet формат
my_prefix/subdir/analytics/events/part-1707649200-c3d4e5f6.00000.parquet

# Без layout
public/users/part-1707649200-a1b2c3d4.00000.json
```

## Потокобезопасность

- **SnapshotSink.Push()** предполагается однопоточным (вызывается из одного потока)
- **snapshotWriter** не является потокобезопасным
- Горутина загрузки работает независимо
- Синхронизация через каналы (`uploadDone`), `sync.Once` (`uploadOnce`) и контекст (`ctx`/`cancel`)

## Ограничения

1. **Однопоточный Push()** — не предназначен для конкурентных вызовов
2. **Один активный snapshotWriter** — только один файл загружается одновременно
3. **Нет частичного восстановления** — снапшот должен завершиться полностью

## Связанные файлы

- [`snapshot_sink.go`](snapshot_sink.go) — главный компонент
- [`snapshot_writer.go`](snapshot_writer.go) — менеджер записи
- [`file_splitter.go`](file_splitter.go) — разбиение на файлы
- [`writer/writer.go`](writer/writer.go) — сжатие данных
- [`s3_object_ref.go`](s3_object_ref.go) — ссылки на S3 объекты
- [`s3_client.go`](s3_client.go) — интерфейс S3 клиента

### Поток данных

1. **ChangeItem** → **SnapshotSink.Push()** — входные события
2. **processItemsAndReturnInserts()** — разделение по типам, обработка системных событий
3. **processSnapshot()** → **writeChunkAndRotate()** — запись с ротацией
4. **snapshotWriter.write()** → **BatchSerializer.SerializeAndWrite()** → **Writer** → **Pipe** — сериализация, сжатие, запись
5. **Pipe** → **s3Client.Upload** (горутина) → **S3** — асинхронная загрузка
6. **finishUpload()** → **uploadDone** канал → **snapshotWriter.close()** — сигнал о завершении
