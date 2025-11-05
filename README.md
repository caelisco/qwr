# qwr - Query Writer Reader

A Go library for SQLite that provides serialised writes and concurrent reads with optional context support. qwr uses a worker pool pattern with a single worker to squentally queue writes to SQLite. It supports a configurable reader with connections.

## Quick Start

```go
package main

import (
    "log"
    
    "github.com/caelisco/qwr"
    "github.com/caelisco/qwr/profile"
)

func main() {
    // Create manager with default options
    manager, err := qwr.New("test.db").
        Reader(profile.ReadBalanced()).
        Writer(profile.WriteBalanced()).
        Open()
    if err != nil {
        log.Fatal(err)
    }
    defer manager.Close()

    // Write bypassing queue
    result, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Alice").Write()

    // Synchronous write
    result, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Jane").Execute()
    if err != nil {
        log.Fatal(err)
    }

    // Asynchronous write
    jobID, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Bob").Async()
    if err != nil {
        log.Fatal(err)
    }

    // Read data
    rows, err := manager.Query("SELECT * FROM users").Read()
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
}
```

### Write Operations

**Direct Write**
Bypasses the worker queue for immediate execution.

```go
result, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Alice").Write()
```

**Synchronous Write**
Uses the worker pool to serialise the write, blocking until the write is complete.
```go
result, err := manager.Query("INSERT INTO USERS (name) VALUES (?)", "Jane").Execute()
```

**Async Write**
Asynchronous writes are non-blocking and are guarded by an error queue. The error queue attempts to retry transactions automatically with an exponential backoff + jitter. Certain errors are not recoverable.

```go
jobID, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Bob").Async()
```

Note: Async operations that fail are automatically added to the error queue. Since there's no immediate error return, you must check the error queue to detect failures:

```go
if jobErr, found := manager.GetErrorByID(jobID); found {
    log.Printf("Async job %d failed: %v", jobID, jobErr.Error())
}
```

**Batch Write**
Batching creates a slice of queries for deferred processing. Writing occurs either at a timed interval, or once the queue depth reaches a pre-determined threshold. An experimental feature inlines common queries in the batch to optimise the write process.

```go
// These will be automatically batched together
manager.Query("INSERT INTO users (name) VALUES (?)", "Charlie").Batch()
manager.Query("INSERT INTO users (name) VALUES (?)", "Diana").Batch()
```

**Transactions**
Multi-statement atomic operations.
```go
tx := manager.Transaction().
    Add("INSERT INTO users (name) VALUES (?)", "Eve").
    Add("UPDATE users SET active = ? WHERE name = ?", true, "Eve")

result, err := tx.Write() // or tx.Exec() for async
```

### Read Operations

Read operations use the reader connection pool and can be executed concurrently:

**Multiple Rows**:
```go
rows, err := manager.Query("SELECT * FROM users WHERE active = ?", true).Read()
```

**Single Row**:
```go
row, err := manager.Query("SELECT name FROM users WHERE id = ?", 1).ReadRow()
```

### Prepared Statements

Prepared statements are cached in a sync.Map automatically when enabled. This reduces preparation overhead for repeated queries:
```go
result, err := manager.Query("INSERT INTO users (name) VALUES (?)", "Alice").
    Prepared().
    Write()
```

### Context Support (Optional)

Contexts can be used for timeouts and cancellation but are not required. The library functions normally without any context usage:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Per-query context
result, err := manager.Query("SELECT * FROM users").
    WithContext(ctx).
    Read()

// Manager-level context (affects all operations)
manager, err := qwr.New("test.db").
    WithContext(ctx).
    Open()
```

## Database Profiles

Database profiles configure connection pools and SQLite PRAGMA settings for different use cases. qwr includes several pre-configured profiles:

### Read Profiles
- `profile.ReadLight()` - Low resource usage (5 connections, 30MB cache)
- `profile.ReadBalanced()` - General purpose (10 connections, 75MB cache)  
- `profile.ReadHeavy()` - High concurrency (25 connections, 150MB cache)

### Write Profiles
- `profile.WriteLight()` - Basic performance (50MB cache, 4KB pages)
- `profile.WriteBalanced()` - Most applications (100MB cache, 8KB pages)
- `profile.WriteHeavy()` - High volume (200MB cache, 8KB pages)

### Custom Profiles
```go
customProfile := profile.New().
    WithMaxOpenConns(15).
    WithCacheSize(-102400). // 100MB
    WithJournalMode(profile.JournalWal).
    WithSynchronous(profile.SyncNormal).
    WithPageSize(8192)

manager, err := qwr.New("test.db").
    Reader(customProfile).
    Writer(profile.WriteBalanced()).
    Open()
```

## Configuration Options

The Options struct controls various aspects of qwr's behaviour. All options have sensible defaults:

```go
options := qwr.Options{
    // Worker configuration
    WorkerQueueDepth:  50000,         // Queue buffer size
    EnableReader:      true,          // Enable read operations
    EnableWriter:      true,          // Enable write operations
    
    // Batching
    BatchSize:         200,           // Queries per batch
    BatchTimeout:      1*time.Second, // Max batch wait time
    InlineInserts:     true,          // Combine INSERT statements
    
    // Context behaviour  
    UseContexts:       false,         // Default context usage
    
    // Statement caching
    StmtCacheSampleRate: 100,         // Metrics sampling rate (1 in N operations)
    StmtCacheSlowThreshold: 10*time.Millisecond, // Threshold for slow query logging
    
    // Error handling
    ErrorQueueMaxSize: 1000,          // Max errors in memory
    ErrorLogPath:      "",            // Path for persistent error logging (empty = disabled)
    EnableAutoRetry:   false,         // Automatic retry
    MaxRetries:        3,             // Max retry attempts
    BaseRetryDelay:    30*time.Second,// Initial retry delay
    RetryInterval:     30*time.Second,// Retry check frequency
    
    // Timeouts
    JobTimeout:         30*time.Second, // Individual job timeout
    TransactionTimeout: 30*time.Second, // Transaction timeout
    RetrySubmitTimeout: 5*time.Second,  // Retry submission timeout
    QueueSubmitTimeout: 5*time.Minute,  // Timeout for context-free queue submissions

    // Monitoring
    EnableMetrics:      true,            // Enable metrics collection
    LogLevel:           slog.LevelInfo,  // Logging verbosity (Debug, Info, Warn, Error)
}
```

## Error Handling & Retry

For asynchronous processing of queries, qwr provides error classifications to optimise retry strategies.

### Enhanced Error Classification
qwr classifies errors:
- **Connection Errors**: File I/O issues, permission problems → Linear backoff retry
- **Lock Errors**: Database busy, locked → Exponential backoff retry
- **Constraint Violations**: Unique key, foreign key, NOT NULL → No retry (permanent failure)
- **Schema Errors**: Missing tables/columns, syntax errors → No retry (permanent failure)  
- **Resource Errors**: Disk full, out of memory → Linear backoff retry
- **Timeout Errors**: Context timeouts, deadlines → Linear backoff retry
- **Permission Errors**: Access denied, read-only → No retry (permanent failure)
- **Internal Errors**: qwr-specific errors → No retry
- **Unknown Errors**: Unclassified → No retry (safe default)

### Error Logging
Errors can be persisted to a separate SQLite database for analysis. When enabled (via `ErrorLogPath` option), errors are logged with full context:
- SQL statement and parameters (CBOR encoded)
- Error type and message
- Retry attempts and timestamps
- Failure reason

You can customise the path:
```go
options := qwr.Options{
    ErrorLogPath: "/path/to/custom_errors.db", // Enable persistent error logging
}
```

If `ErrorLogPath` is empty (default), persistent error logging is disabled.

### Retry Configuration
```go
options := qwr.Options{
    EnableAutoRetry:   true,
    MaxRetries:        3,
    BaseRetryDelay:    30 * time.Second,
    RetryInterval:     30 * time.Second,
}

manager, err := qwr.New("test.db", options).Open()
```

### Error Queue Management
```go
// Get all errors
errors := manager.GetErrors()

// Get specific error with enhanced information
if jobErr, found := manager.GetErrorByID(jobID); found {
    fmt.Printf("Job %d failed: %v\n", jobID, jobErr.Error())
    
    // Access structured error information
    if qwrErr := jobErr.errType; qwrErr != nil {
        fmt.Printf("Error category: %s\n", qwrErr.Category)
        fmt.Printf("Retry strategy: %s\n", qwrErr.Strategy) 
        fmt.Printf("Context: %+v\n", qwrErr.Context)
    }
}

// Manually retry a failed job
if err := manager.RetryJob(jobID); err != nil {
    switch {
    case errors.Is(err, qwr.ErrJobNotFound):
        fmt.Printf("Job %d not found in error queue\n", jobID)
    case errors.Is(err, qwr.ErrRetrySubmissionFailed):
        fmt.Printf("Failed to resubmit job %d\n", jobID)
    default:
        fmt.Printf("Retry error: %v\n", err)
    }
}

// Clear error queue
manager.ClearErrors()
```

### Automatic Batching
The batch collector groups multiple operations together and executes them in a single transaction. This reduces the number of database round trips:
```go
// Configure batching
options := qwr.Options{
    BatchSize:    200,                // Execute after 200 queries
    BatchTimeout: 1 * time.Second,    // Or after 1 second
    InlineInserts: true,              // Combine similar INSERT statements
}
```

### Statement Caching
Prepared statements are stored in memory to avoid re-parsing SQL. The cache uses `sync.Map` and grows unbounded - there are no size limits enforced.

### Inline INSERT Optimisation (Experimental)
Inline INSERT optimises statements with identical SQL are combined into a single multi-value INSERT:
```go
// These individual statements:
INSERT INTO users (name) VALUES ('Alice')
INSERT INTO users (name) VALUES ('Bob')
INSERT INTO users (name) VALUES ('Charlie')

// Becomes:
INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie')
```

## Monitoring & Metrics

qwr provides basic performance monitoring and operational visibility. Metrics collection can be disabled for performance.

### Metrics Configuration
```go
options := qwr.Options{
    EnableMetrics: true,  // Enable/disable metrics collection (default: true)
}
```

### Performance Metrics
```go
metrics := manager.GetMetrics()
fmt.Printf("Jobs processed: %d\n", metrics.JobsProcessed)
fmt.Printf("Processing rate: %.2f jobs/sec\n", metrics.ProcessingRate)
fmt.Printf("Error rate: %.2f%%\n", metrics.ErrorRate*100)
fmt.Printf("Queue length: %d\n", metrics.CurrentQueueLen)
```

### Cache Metrics
```go
cacheMetrics := manager.GetCacheMetrics()
for name, stats := range cacheMetrics {
    fmt.Printf("%s cache hit ratio: %.2f%%\n", name, stats.HitRatio*100)
}
```

### Error Queue Stats
```go
errorStats := manager.GetErrorQueueStats()
fmt.Printf("Pending retries: %d\n", errorStats.PendingRetries)
```

### Wait for Queue to Drain
```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := manager.WaitForIdle(ctx); err != nil {
    log.Printf("Timeout waiting for queue to drain: %v", err)
}
```

### Manual Cache Management
```go
// Reset all caches
manager.ResetCaches()

// Reset metrics only
manager.ResetMetrics()
```

### Database Maintenance
```go
// Full vacuum (rebuilds entire database)
err := manager.RunVacuum()

// Incremental vacuum (reclaims some space)
err := manager.RunIncrementalVacuum(1000) // 1000 pages

// WAL checkpoint
err := manager.RunWalCheckpoint("PASSIVE")
```

## Caveats & Design Decisions

### Unbounded Statement Cache

The prepared statement cache uses `sync.Map` and **grows without bounds** - there is no size limit or eviction policy.

This is an intentional design decision to maximise performance. Most applications use a finite set of SQL queries, so the cache naturally reaches a stable size.

Applications that generate dynamic SQL with varying literals (e.g., embedding timestamps or IDs directly in SQL strings instead of using parameters) will cause unbounded memory growth.

- Always use parameterised queries with `?` placeholders
- Avoid building SQL strings with dynamic values embedded

### Error Queue Overflow Behaviour

When the error queue exceeds `ErrorQueueMaxSize`, the oldest errors are persisted to the error log database and then removed from the in-memory queue.

1. In-memory queue fills to `ErrorQueueMaxSize` (default: 1,000 errors)
2. New errors trigger overflow handling
3. Oldest errors are written to the SQLite3 error log database
4. Oldest errors are removed from memory to make space
5. Error information is preserved on disk for later analysis

If the error log database write fails (e.g., disk full, permissions), the error data is permanently lost.

## Using Custom SQLite Drivers

qwr uses `modernc.org/sqlite` by default, but you can bring your own SQLite driver using the `NewSQL()` constructor:

```go
package main

import (
    "database/sql"
    "log"

    "github.com/caelisco/qwr"
    "github.com/caelisco/qwr/profile"
    _ "github.com/mattn/go-sqlite3" // Your preferred SQLite driver
)

func main() {
    // Open your own database connections
    readerDB, err := sql.Open("sqlite3", "test.db")
    if err != nil {
        log.Fatal(err)
    }

    writerDB, err := sql.Open("sqlite3", "test.db")
    if err != nil {
        log.Fatal(err)
    }

    // Create options with error log path
    opts := qwr.Options{
        EnableReader:  true,
        EnableWriter:  true,
        ErrorLogPath:  "test_errors.db", // Optional: enable persistent error logging
    }

    // Pass connections and options to qwr
    manager, err := qwr.NewSQL(readerDB, writerDB, opts).
        Reader(profile.ReadBalanced()).
        Writer(profile.WriteBalanced()).
        Open()
    if err != nil {
        log.Fatal(err)
    }
    defer manager.Close() // This will also close readerDB and writerDB

    // Use manager normally...
}
```
The Manager takes full ownership of your database connections, so a call to `Manager.Close()` will close both reader and writer connections. Profiles can still be applied to your connections (profiles are just specified SQLite PRAGMAS). You can optionally setup `ErrorLogDB` in Options to enable persistent error logging for async writes.

You can also set the error log path using the fluent API:
```go
manager, err := qwr.NewSQL(readerDB, writerDB).
    WithErrorDB("custom_errors.db").
    Reader(profile.ReadBalanced()).
    Writer(profile.WriteBalanced()).
    Open()
```

qwr was inspired by numerous articles that describe using SQLite3 in production systems.

### Consider SQLite
**Author**: Wesley Aptekar-Cassels
**URL**: https://blog.wesleyac.com/posts/consider-sqlite

### Scaling SQLite to 4M QPS on a Single Server
**Author**: Expensify Engineering
**URL**: https://use.expensify.com/blog/scaling-sqlite-to-4m-qps-on-a-single-server

### Your Database is Your Prison — Here's How Expensify Broke Free
**Author**: First Round Review
**URL**: https://review.firstround.com/your-database-is-your-prison-heres-how-expensify-broke-free/

### How (and Why) to Run SQLite in Production
**Author**: Stephen Margheim
**URL**: https://fractaledmind.github.io/2023/12/23/rubyconftw/

### Gotchas with SQLite in Production
**Author**: Anže Pečar
**URL**: https://blog.pecar.me/sqlite-prod
