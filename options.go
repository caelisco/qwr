package qwr

import (
	"log/slog"
	"time"
)


// Options holds configuration for the qwr manager's internal behavior.
//
// IMPORTANT: Options are immutable after manager startup. They can only be set
// during manager construction via New() and cannot be modified at runtime.
// To change options, you must stop the application, modify the options, and restart.
type Options struct {
	// WorkerQueueDepth sets the buffer size for the write queue.
	// Higher values allow more queries to be buffered but use more memory.
	// Default: 50000. Typical range: 1000-100000 depending on throughput needs.
	WorkerQueueDepth int

	// EnableReader determines if the reader database connection pool is created and used.
	// Disable for write-only applications to save resources.
	// Default: true
	EnableReader bool

	// EnableWriter determines if the writer database connection (and worker) is created and used.
	// Disable for read-only applications to save resources.
	// Default: true
	EnableWriter bool

	// BatchSize is the number of queries to collect before automatically executing the batch.
	// Larger batches improve throughput but increase latency and memory usage.
	// Default: 200. Typical range: 50-1000 depending on query size and latency requirements.
	BatchSize int

	// BatchTimeout is the maximum time to wait before executing a partial batch.
	// Ensures timely execution even when BatchSize isn't reached.
	// Default: 1 second. Typical range: 100ms-5s depending on latency requirements.
	BatchTimeout time.Duration

	// InlineInserts determines whether to try combining similar INSERT statements.
	// Combines multiple INSERT INTO table VALUES (...) into single multi-value INSERT.
	// Improves performance for bulk inserts but requires identical SQL structure.
	// Default: true
	InlineInserts bool

	// UseContexts determines whether to use context-based methods by default.
	// When false, operations use non-context methods unless WithContext() is called.
	// Default: false (contexts are opt-in for better performance)
	UseContexts bool


	// StmtCacheSampleRate controls detailed metrics sampling (1 in N operations).
	// Higher values reduce metrics overhead but provide less detailed monitoring.
	// Default: 100 (sample every 100th operation). Set to 1 for full sampling.
	StmtCacheSampleRate int64

	// StmtCacheSlowThreshold is the threshold for recording slow query preparation.
	// Preparations taking longer than this are logged for performance analysis.
	// Default: 10ms. Typical range: 1ms-100ms depending on performance requirements.
	StmtCacheSlowThreshold time.Duration

	// ErrorQueueMaxSize is the maximum number of errors to retain in memory.
	// When full, oldest errors are persisted to disk and removed from memory.
	// Default: 1000. Higher values provide more error history but use more memory.
	ErrorQueueMaxSize int

	// EnableAutoRetry determines whether to automatically retry failed async operations.
	// When enabled, retriable errors (database locks, timeouts) are retried automatically.
	// Default: false (manual retry control)
	EnableAutoRetry bool

	// RetryInterval is how often to check for jobs ready to retry.
	// Shorter intervals provide faster retry response but use more CPU.
	// Default: 30 seconds. Typical range: 10s-5m depending on urgency needs.
	RetryInterval time.Duration

	// MaxRetries is the maximum number of retry attempts for a failed job.
	// After exceeding this limit, jobs are marked as permanently failed.
	// Default: 3. Typical range: 1-10 depending on reliability requirements.
	MaxRetries int

	// BaseRetryDelay is the base delay for exponential backoff retry logic.
	// Actual delay is BaseRetryDelay * 2^(attempt-1) with jitter.
	// Default: 30 seconds. Longer delays reduce database pressure during issues.
	BaseRetryDelay time.Duration

	// JobTimeout is the default timeout for individual job execution.
	// Applied to queries, prepared statement execution, and other single operations.
	// Default: 30 seconds. Adjust based on expected query complexity and database performance.
	JobTimeout time.Duration

	// TransactionTimeout is the default timeout for transaction execution.
	// Applied to multi-statement transactions including BEGIN, queries, and COMMIT.
	// Default: 30 seconds. Should be longer than JobTimeout for complex transactions.
	TransactionTimeout time.Duration

	// RetrySubmitTimeout is the timeout for submitting retry jobs to the worker queue.
	// Prevents retry logic from hanging when the queue is full or worker is stopped.
	// Default: 5 seconds. Should be shorter than RetryInterval.
	RetrySubmitTimeout time.Duration

	// QueueSubmitTimeout is the timeout for context-free submissions to wait for queue space.
	// Only applies to SubmitWaitNoContext/SubmitNoWaitNoContext methods.
	// Prevents deadlock when queue is full by failing after this timeout.
	// Default: 5 minutes. Should be long enough to allow queue to drain during high load.
	QueueSubmitTimeout time.Duration

	// LogLevel sets the logging level using slog.Level constants.
	// Controls verbosity of internal QWR logging (Debug, Info, Warn, Error).
	// Default: slog.LevelInfo. Use slog.LevelDebug for detailed troubleshooting.
	LogLevel slog.Level

	// EnableMetrics determines whether to collect performance and operational metrics.
	// When disabled, all metrics collection is skipped for better performance.
	// Default: true (metrics collection enabled)
	EnableMetrics bool

	// ErrorLogPath is the path for persistent error logging database.
	// If empty, persistent error logging is disabled (in-memory error queue still works).
	// Default: "" (persistent logging disabled)
	ErrorLogPath string
}

// Validate validates and sets defaults for all options
func (o *Options) Validate() error {
	o.SetDefaults()

	// Add validation logic here if needed
	if o.WorkerQueueDepth < 0 {
		o.WorkerQueueDepth = 1000
	}

	if o.BatchSize < 0 {
		o.BatchSize = 200
	}


	if o.ErrorQueueMaxSize < 0 {
		o.ErrorQueueMaxSize = 1000
	}

	if o.MaxRetries < 0 {
		o.MaxRetries = 3
	}

	if o.StmtCacheSampleRate < 1 {
		o.StmtCacheSampleRate = 100
	}

	return nil
}

// SetDefaults applies default values if not set.
func (o *Options) SetDefaults() {
	if o.WorkerQueueDepth <= 0 {
		o.WorkerQueueDepth = 50000
	}

	if !o.EnableReader && !o.EnableWriter {
		o.EnableReader = true
		o.EnableWriter = true
	}

	if o.BatchSize <= 0 {
		o.BatchSize = 200
	}
	if o.BatchTimeout <= 0 {
		o.BatchTimeout = 1 * time.Second
	}


	if o.StmtCacheSampleRate <= 0 {
		o.StmtCacheSampleRate = 100
	}
	if o.StmtCacheSlowThreshold <= 0 {
		o.StmtCacheSlowThreshold = 10 * time.Millisecond
	}

	if o.ErrorQueueMaxSize <= 0 {
		o.ErrorQueueMaxSize = o.WorkerQueueDepth
	}

	if o.RetryInterval <= 0 {
		o.RetryInterval = 30 * time.Second
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = 3
	}
	if o.BaseRetryDelay <= 0 {
		o.BaseRetryDelay = 30 * time.Second
	}

	// Set timeout defaults - consolidating hardcoded values
	if o.JobTimeout <= 0 {
		o.JobTimeout = 30 * time.Second
	}
	if o.TransactionTimeout <= 0 {
		o.TransactionTimeout = 30 * time.Second
	}
	if o.RetrySubmitTimeout <= 0 {
		o.RetrySubmitTimeout = 5 * time.Second
	}
	if o.QueueSubmitTimeout <= 0 {
		o.QueueSubmitTimeout = 5 * time.Minute
	}

	// Set log level default if not set (slog.Level zero value is LevelInfo)
	if o.LogLevel == 0 {
		o.LogLevel = slog.LevelInfo
	}

	// Note: Boolean defaults (InlineInserts, EnableMetrics) are handled by
	// copying from DefaultOptions when creating a new Manager.
	// We don't set them here because we can't distinguish between
	// explicitly set false and unset (both are false).
}

// DefaultOptions provides a common starting point for configuration.
var DefaultOptions = Options{
	WorkerQueueDepth:       1000,
	EnableReader:           true,
	EnableWriter:           true,
	BatchSize:              200,
	BatchTimeout:           1 * time.Second,
	InlineInserts:          true,
	UseContexts:            false,
	StmtCacheSampleRate:    100,
	StmtCacheSlowThreshold: 10 * time.Millisecond,
	ErrorQueueMaxSize:      1000,
	EnableAutoRetry:        false,
	RetryInterval:          30 * time.Second,
	MaxRetries:             3,
	BaseRetryDelay:         30 * time.Second,
	JobTimeout:             30 * time.Second,
	TransactionTimeout:     30 * time.Second,
	RetrySubmitTimeout:     5 * time.Second,
	QueueSubmitTimeout:     5 * time.Minute,
	LogLevel:               slog.LevelInfo,
	EnableMetrics:          true,
}
