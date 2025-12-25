## Data Model

One of our objectives was to standardize change-log events. It gives us fast storage integration and may simplify custom application integration. As the most common discriminant, we decide to stay on the following change-log event structure:


* **ID** (`uint64`) The unique identifier for the operation. Each store has its own semantics but is often derived from a transaction identifier.
* **Commit Time** (`uint64`) Unix epoch time for transaction in nanoseconds. All databases have such functionality, so we rely on the clock on the source database.
* **Kind** (one of` INSERT | UPDATE | DELETE | DDL`) kind of event. Change-log may be one of `insert/update/delete`. Also, we must reserve special events for DDL operations. Since our source storage can alter greatly, we must add one more type.
* **Table** (`string`) Name of the table from which the event was acquired.
* **Values** (`[]any`) List of column values. Values are serialized to some common format (JSON or proto).
* **OldValues** (`[]any`) List of column values before change applied. It may be filled only in the `UPDATE` event kind. Values are serialized to some common format (JSON or proto).
* **TableSchema** (one of` Schema | SchemaID`) List of columns that contain these events. To minimize serialized value, we may use `SchemaID `in our schema registry. Each schema has a unique monotone-grown sequence. `SchemaID` is also unique and monotone; the sinker may decide to try to alter data once the previously upserted `SchemaID` is before the current one. `SchemaID `refers to the schema registry. We’ll discuss this more in-depth in further sections.

This format is easy to apply to any database. All it needs is to insert, update or delete rows. But since we support various databases, some event types are not supported.

Our connected storages are split into two major groups:



* **Mutable databases** and
* **Immutable databases**.

Mutable databases are transactional or NoSQL databases (<span style="text-decoration:underline;">PostgreSQL</span>, <span style="text-decoration:underline;">MySQL</span> etc.).

The immutable ones are analytical databases. They have limited support for updates and deletes, requiring a different approach. We use the raw format for such databases as append-only logs, which store raw logs and optional applications for “`INSERT ONLY."` The user may implement a cron task for such tables, which transforms and accumulates the raw logs into a point-in-time replica of the source database.

