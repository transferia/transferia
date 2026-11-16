# Federation mock

Local plaintext gRPC server implementing YDB endpoint discovery, federation
discovery and topic reading. It can be used by tests of SDK readers and provider
pipelines without starting a real YDB instance.

```go
server := federationmock.NewServer(t, federationmock.Config{
    Consumer:                 "readers/consumer",
    Topic:                    "topic",
    FederationDatabasePrefix: "/logbroker-federation",
    SingleDatabase:           true,
})
endpoint := server.Endpoint()
```

`NewServer` registers server and listener cleanup with `testing.TB.Cleanup`.
Register client/source cleanup afterwards so clients stop before the server.
Configure the embedded `Config` before connecting clients; changing it while
requests are in progress is not supported.

By default discovery returns databases `one` and `two` for each account, at
separate local endpoints. They reuse the configured topic, partition 0 and
partition-session ID 1, with initial offsets 7 and 42. Each stream sends one
ZSTD-compressed message containing its database path. `SingleDatabase` returns
only `one`; `SameEndpoint` advertises both databases at the same endpoint.

`Terminal`, `NonFederated`, `OffsetGap` and `CorruptBatch` enable error scenarios.
`DiscoveryHook` receives the account extracted from the federation database path.
`ReadBarrier` delays sending messages, and releasing `Reconnect` interrupts the
first stream for each database with `Unavailable`.

`Started()`, `Commits()` and `Stops()` expose read-only event channels;
`Errors()` reports unexpected stream errors. `ActiveStreams()` reports the
current stream count. Drain events as the scenario proceeds: channels are
buffered to 16 events and are not closed during cleanup.
