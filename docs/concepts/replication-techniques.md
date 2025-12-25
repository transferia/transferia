# Replication Techniques

{{ data-transfer-name }} supports multiple data replication techniques to handle various use cases:

## Snapshot Load
- Full dataset replication with configurable parallelism and throttling.
- Supports deduplication strategies for consistency during restarts.

## Transaction Log Streaming
- Real-time replication of changes using database transaction logs.
- Guarantees at-least-once delivery semantics, supporting idempotent and transactional targets.


Data replication is a crucial topic in the concept of storage. Each storage that aims to be durable must store data in several replicas. There are several techniques to achieve durable data storage, the most popular ones based on transaction logs.

The typical creation of replicas has 2 phases: snapshot load and transaction log streaming.

Since **{{ data-transfer-name }}** supports various databases, we must remember that each has a subset of features we may expect.

We intentionally decided to treat them all as a source and target equally, but some may or may not support streaming or snapshot. It must support at least a snapshot or transaction log source to include storage type into **{{ data-transfer-name }}**.


## Snapshot load

Another important topic is the issue of load distribution and latency. Here we have two conflicting requirements.

One states that we must minimize latency. To achieve it, we have to load our data in parallel, usually leading to more resource consumption on the side and **{{ data-transfer-name }}** itself. Regarding the **{{ data-transfer-name }}** resource consumption, it doesn't matter since total work time is even with or without parallel, but storage may be prone to overload.

Another requirement is that we must not overload it. Without knowing the definition of overload, we should provide control over how much it is loaded. All connected databases are different (not only in type but also in installation). We cannot choose a single workload, and we must provide users with a tool for controlling the degree of parallelism and/or throttling strategies that could drive snapshot costs down.

Snapshot load takes two stages - `pre-processing` and `processing`.

At the pre-processing stage:

1. The table is cut into shards (by the number of shards configured or inferred from the target) according to the specified sharding key.
2. Shards are cut into partitions for additional parallelization and reading from the source.
3. Entries are grouped into blocks; each block is compressed for subsequent transmission over the network.
4. Cooked shards are stored in the task state in the **{{ data-transfer-name }}** directory in the shards subdirectory.
5. A prepared shard is a set of partitions, each row of which contains one block (data column) along with metainformation (format, compression, row count columns, etc.).
6. All blocks are fixed at the pre-processing stage and do not change upon insertion.
7. All lines generated at the pre-processing stage are validated against the schema registry for data integrity. If the pre-processing is successful, shards should contain only valid blocks that can be safely inserted into the target tables.

At some sources, it is possible to run this stage in parallel even for a single table (like <span style="text-decoration:underline;">ClickHouse</span>); other DBMS may not have such a feature (like <span style="text-decoration:underline;">PostgreSQL</span>), so we have to run it in a single thread per shard per table.

At the pre-processing stage, the table is segmented into shards, each of which is a collection of disjoint table rows. Filling shard in a first approximation works like this: it subtracts this sequence, inserts it into the table and periodically records progress.

The sequence of shard blocks is virtual. Physically, the blocks of one shard are distributed across several partition tables.

The shard loader cuts the input (virtual) sequence of prepared (at the preprocessing stage) shard blocks into fixed-size packs and commits the progress after the completion of the insertion of all blocks of the next pack of the shard. A barrier is established between the packs: blocks from the next pack are only inserted once all blocks from the previous pack are inserted.

Shard filling will continue from the last uncommitted block pack in case of interruption and restart of the task. This may lead to duplicates over the snapshot load process. Users may choose different strategies to handle it.

For example, ClickHouse has a deduplication engine, which will drop inserts that the server has already accepted during a previous run; all we have to do is replay the same insert (it compares the hash sum of data).

For other targets, we may consider reducing parallel batch processing to one or adding a derived state tracker that will transactionally mark parts as done in the target database progress table, so we could restore the previous state from the target (if it supports transactions).

Each pack is inserted under a transaction with a lock on the original shard and commit offset with the commit of this transaction; offset moves at both sides - source and targets.

The uploads of different shards run in parallel and independently in different jobs. Reading blocks for one shard are paralleled by a partition key (inferred or configured).

The order of inserting blocks inside one bundle can be arbitrary, so the upload does the parallel insertion of blocks of the pack through different replicas of the shard and several connections to the same replica.

During upload, the shard availability of the target database is monitored. If the monitor loses the quorum (each target may define its own rule of defining that quorum) of the shard target, then new inserts into the shard are blocked.

The motivation is simple: if only 1 out of 3 sharded machines is available, then the inserted data is not replicated, so it is easy to lose it if only one machine fails.

The upload job may crash or be interrupted by the user at an arbitrary moment of execution. Still, it can be started again (for example, on another machine) and continue copying until successful completion. Restarting should not lead to losses or duplicates in the copied data (of course, modulo some assumptions).

In the process, the task fixes the current stage (creating shards, filling shards, creating tables etc.). When restarting, the task skips the completed stages and begins work by repeating the last incomplete.

All tables manage operations are idempotent, so we skip it completely on restart. Shard upload jobs capture their progress, and only a small fraction of the inserts are retried.


## Transaction log streaming

Once a snapshot is uploaded, we can start replication from the prepared replication log position. When the snapshot is completed, we start the streaming job. We could start one or more jobs depending on the source shard count. Usually, one shard needs one independent executor (or `worker`) to complete the replication process.

Most traditional database systems offer access to their replication log, which allows clients to learn about the updates happening in the database in real time. Many infrastructures for real-time apps are built on top of this functionality. Despite the differences in implementations, they often operate with similar abstractions. By identifying the lowest common denominator, we can begin synchronizing data between systems seamlessly.

**{{ data-transfer-name }}** supports transaction semantics across multiple database types. For each change in source storage, it can annotate and propagate required metadata such as transaction ID, storage commit timestamp and log sequence number (or `LSN`). The worker is a stateless pull process, not preserving any own state.

The transport layer supports guaranteed at least once delivery semantics by default; it will commit progress to source storage once the sinker acknowledgment is received. An event may be delivered multiple times only in the case of failures in the communication channel between the source and sinker or a hard failure in the **{{ data-transfer-name }}** worker process. Therefore, the target database (or `Sink`) must be idempotent in applying the delivered events or maintain transactional commit semantics aligned with the source.
