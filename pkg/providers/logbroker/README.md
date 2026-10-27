# Logbroker source discovery

Ordinary `LfSource` replication resolves the instances of `logbroker`,
`logbroker-prestable` and `logbroker-serbia` through the Go Federated Topic SDK
at each `Run` or `Fetch`. The installation catalog provides the bootstrap address.

Discovery uses `/logbroker-federation/<account>` for every account in the topic
list. Its endpoints are combined into a single list, with one reader per unique
endpoint regardless of the number of accounts. Every reader receives the complete
original topic list, consumer, database and read settings. Existing installation
normalization (including Serbia's database prefix and TLS settings) is preserved.
The physical database paths returned by discovery are not used to change reader
connections, and parser-visible topic names are not rewritten.

The SDK's database descriptors are trusted without additional validation.
`READ_ONLY` and temporarily unavailable instances remain in the read set: write
availability does not prove that their data has been consumed. An empty result
for any account fails startup rather than silently reading a partial topology.
Discovery has a five-minute budget for all accounts combined. `Fetch` retains its
overall twenty-second deadline, including discovery, and tries readers
sequentially, returning the first error or nonempty sample.

The original endpoint configuration is not modified by discovery. Each replication
attempt takes one snapshot of the instance list and does not watch membership
changes while running. Connection initialization and temporary Persqueue errors
retain their existing retries. A `Disconnect` is handled by the PQv1 SDK's
reconnection mechanism and does not itself trigger discovery. When the overall
source finishes, pending connection attempts are canceled and started readers
are stopped. A subsequent replication attempt creates a new source and repeats
discovery using the configuration already loaded by the data plane.

Multi-YT and Queue-to-S3 continue to use the static instance lists and their
existing configuration and partition coordinates. Explicit `Instance`
connections, single-database installations and the native `LbSource` do not
perform federation discovery. The common YDB driver and YDS/Logbroker writers
do not enable federation discovery implicitly.
