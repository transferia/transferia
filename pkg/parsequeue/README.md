# ParseQueue

Concurrent pipeline for processing data through three stages: **parse** → **push** → **ack**.

## Guarantees

- Sequential per-message processing: parse → push → ack order is preserved
- Configurable parsing parallelism (default: 10)
- Push operations are serialized to respect Add ordering
- Ack operations are serialized and called only after successful push

## Regular vs Waitable

- **ParseQueue** (`New`) provides basic async processing with `Add()` and `Close()`
- **WaitableParseQueue** (`NewWaitable`) adds `Wait()` method that blocks until all added messages are acked, useful in case of source rebalancing

## Error Handling

**Fail-fast**: any error in parse, push, or ack immediately:
- Stops accepting new messages
- Cancels internal context (`Done()` closes)
- Stores first error accessible via `Error()`
- Subsequent errors are ignored

`Error()` is safe to call concurrently and guaranteed final after `Close()`.

`Done()` returns a channel that closes when the queue is closed or an error occurs. Use it in `select` statements to detect queue termination.
