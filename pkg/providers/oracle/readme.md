# Oracle

## Описание работы

### Подключение
Для подключения лучше использовать TNS connection string, выданный вам администратором базы данных. Или вы можете написать его сами.

### Копирование
Копирование запоминает текущий SCN базы. Потом выполняет выборку данных из таблиц запросом вроде: `select <все колонки> from <таблицы> as of scn < текущий SCN>`. То есть для достижения консистентности используется `flashback`. По умолчанию копирование параллелиться потаблично. Дополнительно вы можете выбирать параллельно и данные внутри таблицы - об этом ниже.

### Репликация
Репликация берет текущий SCN (или запомненный ранее, если перед репликацией происходило копирование). Далее выбирается файл лога, который содержит новый SCN. Файл лога читается с помощью `SYS.DBMS_LOGMNR`, все закомиченные транзакции переносятся. Как только транзакция перенеслась, то записывается прогресс в трекер прогресса. Трекер прогресса - служебная таблица, которая создается при старте копирования/репликации. Далее выбирается следующий файл лога и репликация продолжается. Если новых изменений нет - то репликация ждет несколько миллисекунд и пробует снова прочитать изменения.

### CDB/PDB
Если вы находитесь в CDB/PDB среде, то будет происходить выбор контейнера при каждом обращении к базе. Нужный PDB контейнер настраивается в форме эндпоинта. Если поле оставить пустым - то переключение контейнера не будет. Для выбора схемы и копировании данных используется контейнер указанный в форме, то есть контейнер с вашими данными. При репликации, для чтения логов используется рутовый контейнер `cdb$root`. Если вы планируете репликацию в CDB/PDB среде вам потребуется common user. Для копирования такой пользователь не нужен.

## Пререквизиты

Настройка базы и выдача прав зависит от того, что вы собираетесь делать.

### Для копирования таблиц

Что бы скопировать таблицы нужны следующие права для пользователя, от которого будет работать DataTransfer;
1. Права на чтение системной view `V$DATABASE`;
2. Права на использование `flashback` для таблиц, которые нужно скопировать;
3. Права на чтение таблиц, которые нужно скопировать;
4. Права на запись;
5. Права на чтение системных view `dba_extents` и `dba_objects`, если вы хотите иметь возможность параллельного чтения из таблиц при копировании;

Права на запись нужны для создания служебной таблицы с трекером прогресса.

### Для репликации через LogMiner

Что бы запустить репликацию на таблицах надо:
1. Нужен минимальный `supplemental logging` на уровне базы;
2. Для реплицируемых таблиц нужен `supplemental logging` с первичными ключами;
3. Права на чтение системных view `V$DATABASE`, `V$LOG`, `V$LOGFILE`, `V$ARCHIVED_LOG`;
4. Права на использование пакета `SYS.DBMS_LOGMNR`;
5. Права на чтение схемы таблиц, которые нужно реплицировать;
6. Права на запись;

Права на запись, также, как и при копировании, нужны, что бы записать трекер прогресса.

### Прочее

Если вы находитесь в CDB/PDB среде, то для репликации придется создать common user'а (пользователь, имя которого начинается с `c##` или `C##`). Дополнительно, для доступа к данным необходимо выдать права на чтение нужного PDB контейнера с данными, которые нужно скопировать. Также необходима возможность переключать контейнер между тем, где данные и рутовым `cdb$root` контейнером. Нужный PDB контейнер (с данными) нужно будет указать в форме настройки эндпоинта.

## Описание скрытых настроек эндпоинта

`TrackerType` - Возможные значения `NoLogTracker`, `InMemory`, `Embedded`. `NoLogTracker` - не записывать прогресс вообще, может помочь, если в базу источник нельзя писать, работает только для копирования, при выключенном `flashback`. `InMemory` - держать прогресс в памяти, может помочь, если в базу источник нельзя писать, работает только для копирования при при включенном `flashback`. `Embedded` - писать прогресс в базу источник, значение по умолчанию, репликация работает только в этом режиме.

`CLOBReadingStrategy` - Возможные значения `ReadCLOB`, `ReadCLOBAsBLOB`, `ReadCLOBAsBLOBIfFunctionExists`. `ReadCLOB` - читать `CLOB` колонки по умолчанию для драйвера. `ReadCLOBAsBLOB` - не читать `CLOB` колонки напрямую, вместо этого использовать функцию преобразования `CLOB` в `BLOB` и читать значение колонки как массив байт. Потом, в нашем коде байты преобразуются в строку. `ReadCLOBAsBLOBIfFunctionExists` - если функции для преобразования `CLOB` в `BLOB` существует - то преобразование будет происходить. Если функция не существует - то преобразования не будет. Преобразовывать `CLOB` в `BLOB` может быть полезно, когда клиент Oracle не может преобразовать значение колонки в строку правильно и падает с ошибкой `ORA-03106: fatal two-task communication protocol error` или `SIGSEGV`.

`UseUniqueIndexesAsKeys` - Возможность использовать уникальный индекс в качестве первичного ключа, если первичного ключа нет в таблице. Выбирается индекс с минимальным количеством колонок.

`IsNonConsistentSnapshot` - Отключить использование `flashback` при копировании. Может помочь, если таблица слишком большая и перенести её с использованием `flashback` просто невозможно. Или если в таблицу уже не происходит запись. При использовании этой опции таблицы переносятся в не консистентном состоянии относительно друг друга.

`UseParallelTableLoad` - Параллельное чтение каждой таблицы при копировании. Консистентность не нарушается. Значительно ускорят копирование.

`ParallelTableLoadDegreeOfParallelism` - Количество потоков параллельного чтения на каждую таблицу при копировании.

### Функция для преобразования CLOB в BLOB
```sql
DROP FUNCTION CLOB_TO_BLOB;
CREATE FUNCTION CLOB_TO_BLOB(
  value            IN CLOB,
  charset_id       IN INTEGER DEFAULT DBMS_LOB.DEFAULT_CSID,
  error_on_warning IN NUMBER  DEFAULT 0
) RETURN BLOB
IS
  result       BLOB;
  dest_offset  INTEGER := 1;
  src_offset   INTEGER := 1;
  lang_context INTEGER := DBMS_LOB.DEFAULT_LANG_CTX;
  warning      INTEGER;
  warning_msg  VARCHAR2(50);
BEGIN
  IF (value IS NULL) OR LENGTH(value) = 0 THEN
      RETURN result;
  END IF;

  DBMS_LOB.CREATETEMPORARY(
    lob_loc => result,
    cache   => TRUE,
    dur     => DBMS_LOB.CALL
  );

  DBMS_LOB.CONVERTTOBLOB(
    dest_lob     => result,
    src_clob     => value,
    amount       => LENGTH(value),
    dest_offset  => dest_offset,
    src_offset   => src_offset,
    blob_csid    => charset_id,
    lang_context => lang_context,
    warning      => warning
  );

  IF warning != DBMS_LOB.NO_WARNING THEN
    IF warning = DBMS_LOB.WARN_INCONVERTIBLE_CHAR THEN
      warning_msg := 'Warning: Inconvertible character.';
    ELSE
      warning_msg := 'Warning: (' || warning || ') during CLOB conversion.';
    END IF;

    IF error_on_warning = 0 THEN
      DBMS_OUTPUT.PUT_LINE( warning_msg );
    ELSE
      RAISE_APPLICATION_ERROR(
        -20567, -- random value between -20000 and -20999
        warning_msg
      );
    END IF;
  END IF;

  RETURN result;
END clob_to_blob;
/
```

## Для разработчика

### Установка Oracle локально

Можно использовать аот это образ с Oracle 11g XE - [Oracle 11g XE](https://hub.docker.com/r/oracleinanutshell/oracle-xe-11g). Помните, что XE может в себе содержать только 10гб данных!

Запускаем контейнер:
```bash
docker pull oracleinanutshell/oracle-xe-11g
docker run -d -p 49161:1521 -e ORACLE_DISABLE_ASYNCH_IO=true -e ORACLE_ALLOW_REMOTE=true --name oracle oracleinanutshell/oracle-xe-11g
```

Подключаемся:
```
hostname: localhost
port: 49161
sid: xe
username: sys
password: oracle
internal_logon: sysdba
```

Или можно подключиться как в описании образа.

### Необходимые библиотеки

Что бы подключаться к Oracle RDMS вам потребуются оракловые клиентские бинари - [Оракловые клиентские бинари](https://www.oracle.com/ru/database/technologies/instant-client/downloads.html). Выбирайте версию `21` или `19`, `basic` или `full` под свою платформу. Лучше устанавливайте пакет. Иначе, при выборе `zip` архива, придется добавить путь к ним в `path` и в пути поиска `LD` для `nix` систем.

### Oracle CLI

Что бы установить Oracle CLI, необходимо скачать `SQL*Plus Package` там же, где качали бинари - [Оракловые клиентские бинари](https://www.oracle.com/ru/database/technologies/instant-client/downloads.html).

### Сборка

Что бы собрать DataTransfer с поддержкой Oracle должен быть включен `CGO` - `export CGO_ENABLED=1`. Для сборка через `ya make` этого достаточно, дата плейн соберется с поддержкой Оракла.

Что бы собрать через `go build` придется дополнительно установить `zstd` (он нужен, так как у нас есть зависимость на него в аркадии, которая не может конкретно собраться через `go build`):
```bash
git clone https://github.com/facebook/zstd
cd zstd
make
sudo make install
```
Теперь надо показать компилятору Go, где искать `zstd`, для `nix` систем это `export CGO_LDFLAGS=/usr/local/lib/libzstd.a`. Теперь DataTransfer получится собрать через `go build`. Дополнительно, поддержка Оракла спрятана за билд тег, то есть полная команда для сборки с поддержкой Оракла будет такая:
```bash
go build -tags=oracle
```

### Работа с Oracle LogMiner

Для отладки проблем с репликацией из Oracle полезно будет уметь пользоваться `Oracle LogMiner`. `Oracle LogMiner` - это пакет для чтения redo логов. Что такое redo логи вы сможете прочитать на сайте Oracle. Для выполнения всех представленых тут зпросов вам понадобиться пользователь, которого вы ранее создавали для репликации (соотв., читайте выше, как создать такого пользователя). Если вы находитесь в CDB/PDB окружении, то сначала сам нужно выбрать рутовый контейнер, только из рутового контейнера есть доступ к redo логам:

```sql
ALTER SESSION SET CONTAINER = cdb$root
```

Что бы прочитать какой либо лог, сначала его надо найти, в этом нам поможет следующий запрос:

```sql
select a.SEQUENCE# as ID, a.NAME as FILE_NAME, a.FIRST_CHANGE# as FROM_SCN, a.NEXT_CHANGE# as TO_SCN
from V$ARCHIVED_LOG a
where a.THREAD# = 1 and a.STATUS = 'A' and a.STANDBY_DEST = 'NO'
union
select l.SEQUENCE# as ID, f.MEMBER as FILE_NAME, l.FIRST_CHANGE# as FROM_SCN, l.NEXT_CHANGE# as TO_SCN
from V$LOG l
inner join V$LOGFILE f on f.GROUP# = l.GROUP#
where l.THREAD# = 1 and l.STATUS in ('CURRENT', 'ACTIVE', 'INACTIVE') and l.ARCHIVED = 'NO' and f.STATUS is null
order by FROM_SCN desc;
```

Запрос выводит все доступные логи на данном сервере. Для каждого лога вы можете увидеть его `ID`, `имя` и границы `SCN` данного лога. Если вы знаете конкретный SCN с которого надо начать чтение - то выбирайте файл лога, в который входит нужный SCN. С помощью следующего запроса вы сможете инициировать LogMiner файлом лога, который необходимо прочитать:

```sql
BEGIN
    DBMS_LOGMNR.ADD_LOGFILE('<имя файла лога>');
END;
```

После того, как файл лога добавлен в LogMiner (вам надо подставить имя файла лога, полученное на предыдущем этапе), можно начать чтение лога (вам нужно будет поставить в запрос нужный SCN - это может быть нужный вам SCN или SCN начала лога, который можно посмотреть в списке логов):

```sql
BEGIN
    DBMS_LOGMNR.START_LOGMNR(STARTSCN => <SCN с которого начать чтение>, OPTIONS => SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG+SYS.DBMS_LOGMNR.SKIP_CORRUPTION+SYS.DBMS_LOGMNR.NO_SQL_DELIMITER+SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT+SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT);
END;
```

С помощью этого запроса вы сможете увидеть содержимое лога, описание полей вы сможете найти в документации Oracle - [https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/V-LOGMNR_CONTENTS.html](Описание V$LOGMNR_CONTENTS):

```sql
select
	SCN, RS_ID, SSN, (XIDUSN||'.'||XIDSLT||'.'||XIDSQN) as XID, OPERATION_CODE, SEG_OWNER, TABLE_NAME, ROW_ID, SQL_REDO, CSF, STATUS, INFO
from V$LOGMNR_CONTENTS
where
	(SEG_OWNER is null or SEG_OWNER not in ('ANONYMOUS', 'AUDSYS', 'CTXSYS', 'DBSNMP', 'DGPDB_INT', 'DVF', 'DVSYS', 'GGSYS', 'GSMADMIN_INTERNAL', 'GSMCATUSER', 'GSMUSER', 'EXFSYS', 'LBACSYS', 'MDSYS', 'MGMT_VIEW', 'OLAPSYS', 'ORDDATA', 'OWBSYS', 'ORDPLUGINS', 'ORDSYS', 'OUTLN', 'SI_INFORMTN_SCHEMA', 'SYS', 'SYSMAN', 'SYSTEM', 'SYSBACKUP', 'SYSDG', 'SYSKM', 'SYSRAC', 'SYSMAN', 'DBSNMP', 'WK_TEST', 'WKSYS', 'WKPROXY', 'WMSYS', 'XDB', 'APEX_PUBLIC_USER', 'APEX_040000', 'DIP', 'FLOWS_040100', 'FLOWS_FILES', 'MDDATA', 'ORACLE_OCM', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR', 'XS$NULL', 'UNKNOWN'))
	and (TABLE_NAME is null or TABLE_NAME not in ('DATA_TRANSFER_TRACKER'))
```

Завершить работу с LogMiner необходимо следующим образом:

```sql
BEGIN
    DBMS_LOGMNR.END_LOGMNR();
END;
```