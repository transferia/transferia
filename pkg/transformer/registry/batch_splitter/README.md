Lets imagine we use BatchSplitter middleware and set MaxItemsPerBatch to 1000. The following diagram shows how does this middleware fit into the data transfer process(and how it interacts with bufferer in particular)

```mermaid
sequenceDiagram
    participant Souce
    participant Bufferer
    participant Middleware
    participant Sink
    Souce->>Bufferer: AsyncPush(changeItems [count = 1000])
    Souce->>Bufferer: AsyncPush(changeItems [count = 3000])
    Souce->>Bufferer: AsyncPush(changeItems [count = 700])
    Souce-->Sink: Trigger to push changes is activated
    Bufferer->>Middleware: Push(changeItems [count = 4700])
    Middleware->>Sink: Push(changeItems [count = 1000])
    Middleware->>Sink: Push(changeItems [count = 1000])
    Middleware->>Sink: Push(changeItems [count = 1000])
    Middleware->>Sink: Push(changeItems [count = 1000])
    Middleware->>Sink: Push(changeItems [count = 700])
```