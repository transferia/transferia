# Batching Logger

Batching Logger is a Go logging wrapper that prevents log spam by aggregating repeated log messages and providing periodic summaries instead of logging every occurrence.

## Overview

The Batching Logger solves the problem of log spam when the same message is logged repeatedly in a short time period. Instead of flooding the logs with identical messages, it:

1. **Allows the first N occurrences** of each unique message to be logged immediately
2. **Suppresses subsequent occurrences** of the same message within a time window
3. **Provides periodic summaries** showing how many messages were suppressed

## Architecture

The batching logger consists of two main components:

### 1. BatchingLogger
The main logger wrapper that implements the `log.Logger` interface and delegates to an underlying logger while checking with the spam aggregator whether each message should be logged.

### 2. SpamAggregator
The core logic component that:
- Tracks message counts by classification (level + message content)
- Manages flush intervals and thresholds
- Handles periodic cleanup of old entries
- Generates summary messages for suppressed logs

## Key Concepts

### Classification
Each log message is classified by:
- **Level**: The log level (TRACE, DEBUG, INFO, WARN, ERROR, FATAL)
- **Key**: The message content

### Thresholds and Windows
- **Threshold**: Maximum number of immediate logs allowed per classification per window (default: 32)
- **Flush Interval**: Time window duration (default: 1 minute)
- **Max Keys**: Maximum number of unique message classifications to track (default: 1024)

## Configuration

```go
type BatchingOptions struct {
    FlushInterval time.Duration // How often to flush and summarize (default: 1 minute)
    Threshold     int          // Max immediate logs per message per window (default: 32)
    MaxKeys       int          // Max unique message types to track (default: 1024)
}
```

## Usage

### Basic Usage

```go
import (
    "a.yandex-team.ru/library/go/core/log"
    "go/internal/logger/batching_logger"
)

// Create a batching logger with default settings
logger := batching_logger.NewBatchingLogger(baseLogger, nil)

// Use like any other logger
logger.Info("Processing item", log.String("id", "123"))
logger.Error("Failed to process", log.Error(err))
```

### Custom Configuration

```go
// Create with custom settings
opts := &batching_logger.BatchingOptions{
    FlushInterval: 30 * time.Second, // Flush every 30 seconds
    Threshold:     10,               // Allow 10 immediate logs per message
    MaxKeys:       500,              // Track up to 500 unique messages
}
logger := batching_logger.NewBatchingLogger(baseLogger, opts)
```

### Named Loggers

```go
// Child loggers share the same aggregator
childLogger := logger.WithName("component")
```

## How It Works

### Message Flow

1. **First N Messages**: When a new message is logged, the first `Threshold` occurrences are logged immediately
2. **Subsequent Messages**: Additional occurrences of the same message are counted but not logged
3. **Periodic Flush**: Every `FlushInterval`, the aggregator:
   - Logs summary messages for suppressed logs (e.g., "got 25 messages: connection failed")
   - Resets counters for active messages
   - Removes old, inactive message classifications

### Example Behavior

With `Threshold=3` and `FlushInterval=1m`:

```go
// These 3 will be logged immediately
logger.Error("Database connection failed")  // ✓ Logged
logger.Error("Database connection failed")  // ✓ Logged  
logger.Error("Database connection failed")  // ✓ Logged

// These will be suppressed
logger.Error("Database connection failed")  // ✗ Suppressed
logger.Error("Database connection failed")  // ✗ Suppressed
logger.Error("Database connection failed")  // ✗ Suppressed
logger.Error("Database connection failed")  // ✗ Suppressed

// After 1 minute, a summary is logged:
// ERROR: "got 4 messages: Database connection failed"
```

### Memory Management

- **Max Keys Limit**: When the number of tracked message types exceeds `MaxKeys`, the oldest entries are flushed and removed
- **Automatic Cleanup**: Messages that haven't been seen for `2 * FlushInterval` are automatically removed from tracking
- **Counter Reset**: Active message counters are reset (but capped at threshold) after each flush to allow some immediate logging in the next window

### Thread Safety

The batching logger is fully thread-safe:
- Uses `sync.Mutex` to protect the message count map
- Uses `atomic.Bool` for flush state management
- Safe for concurrent use across multiple goroutines

## Implementation Details

### Flush Mechanism

The flush process is triggered by a background goroutine that:
1. Waits for the configured `FlushInterval`
2. Locks the aggregator
3. Processes all tracked messages:
   - Logs summaries for messages exceeding the threshold
   - Removes old entries (not seen for 2x flush interval)
   - Resets active counters (capped at threshold)
4. Marks flush as complete

### Level Handling

- Most log levels generate summaries at the same level
- `FATAL` level summaries are logged as `ERROR` level to avoid program termination

### Performance Considerations

- **Low Overhead**: Only adds a map lookup and counter increment for each log call
- **Bounded Memory**: Limited by `MaxKeys` configuration
- **Efficient Cleanup**: Automatic removal of old entries prevents memory leaks
- **Minimal Blocking**: Fast critical sections with background flush processing

## Limitations

- **Message content based**: Only identical message strings are aggregated (different parameters create different classifications)
- **Memory usage**: Tracks metadata for up to `MaxKeys` unique message types
- **Delayed summaries**: Suppressed message counts are only reported at flush intervals
- **No persistence**: Message counts are lost on application restart