package qwr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/caelisco/qwr/profile"
)

// qwrBuilder provides a fluent API for building a Manager
type qwrBuilder struct {
	// Embedding the Manager eliminates duplication
	*Manager

	// Builder-specific fields
	path       string
	logHandler slog.Handler
	logOutput  *os.File
	logLevel   slog.Level // Track current log level
}

// New creates a new qwr Manager instance builder
//
// Options are immutable after construction. They can only be set here during
// manager creation and cannot be modified at runtime. If no options are provided,
// DefaultOptions will be used.
//
// To change options, you must stop the application, create a new manager with
// different options, and restart.
func New(path string, opts ...Options) *qwrBuilder {
	// Create a manager with base configuration
	manager := &Manager{
		options: DefaultOptions, // Always start with defaults
		path:    path,           // Store database path for logging context
	}

	// Override with user options if provided
	if len(opts) > 0 {
		manager.options = opts[0]
	} else {
		manager.options = DefaultOptions
	}

	// Validate and set defaults for options
	manager.options.Validate()

	// Create the builder with the embedded manager
	return &qwrBuilder{
		Manager:    manager,
		path:       path,
		logHandler: slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: manager.options.LogLevel}),
		logLevel:   manager.options.LogLevel, // Use log level from options
	}
}

// NewSQL creates a new qwr Manager instance builder using user-provided database connections.
// This allows you to bring your own SQLite driver (e.g., mattn/go-sqlite3 instead of modernc.org/sqlite).
//
// Parameters:
//   - reader: Database connection for read operations (pass nil to disable reader)
//   - writer: Database connection for write operations (pass nil to disable writer)
//   - opts: Optional configuration options (variadic). If not provided, DefaultOptions will be used.
//
// Important notes:
//   - Passing nil for reader/writer automatically disables that connection (sets EnableReader/EnableWriter to false)
//   - The Manager takes full ownership of the provided database connections
//   - Calling Manager.Close() will close these database connections
//   - You should not use these connections directly after passing them to NewSQL()
//   - Profiles will be applied to your connections (including SQLite PRAGMAs)
//   - If you provide a non-SQLite database, PRAGMA errors are your responsibility
//   - Use WithErrorLogPath() to enable persistent error logging to disk
//
// Example with mattn/go-sqlite3:
//
//	import _ "github.com/mattn/go-sqlite3"
//
//	readerDB, _ := sql.Open("sqlite3", "mydb.db")
//	writerDB, _ := sql.Open("sqlite3", "mydb.db")
//	opts := qwr.Options{ErrorLogPath: "errors.db"} // Optional error logging
//	manager, err := qwr.NewSQL(readerDB, writerDB, opts).
//	    Reader(profile.ReadBalanced()).
//	    Writer(profile.WriteBalanced()).
//	    Open()
func NewSQL(reader, writer *sql.DB, opts ...Options) *qwrBuilder {
	// Create a manager with base configuration
	manager := &Manager{
		options: DefaultOptions,
		reader:  reader,
		writer:  writer,
	}

	// Override with user options if provided
	if len(opts) > 0 {
		manager.options = opts[0]
	} else {
		manager.options = DefaultOptions
	}

	// Automatically disable reader/writer if nil connections are provided
	if reader == nil {
		manager.options.EnableReader = false
	}
	if writer == nil {
		manager.options.EnableWriter = false
	}

	// Validate and set defaults for options
	manager.options.Validate()

	// Create the builder with the embedded manager
	return &qwrBuilder{
		Manager:    manager,
		logHandler: slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: manager.options.LogLevel}),
		logLevel:   manager.options.LogLevel,
	}
}

// WithContext sets a context for the manager and enables context usage
func (mb *qwrBuilder) WithContext(ctx context.Context) *qwrBuilder {
	mb.ctx = ctx
	mb.internalCtx = ctx // Set internal context to same as user context
	mb.options.UseContexts = true
	return mb
}

// WithLogLevel sets the log level for the manager
func (mb *qwrBuilder) WithLogLevel(level slog.Level) *qwrBuilder {
	mb.logLevel = level // Store the level

	// Update existing handler with new level
	opts := &slog.HandlerOptions{Level: level}
	output := os.Stderr
	if mb.logOutput != nil {
		output = mb.logOutput
	}

	// Preserve existing handler type
	switch mb.logHandler.(type) {
	case *slog.JSONHandler:
		mb.logHandler = slog.NewJSONHandler(output, opts)
	default:
		mb.logHandler = slog.NewTextHandler(output, opts)
	}

	return mb
}

// WithLogHandler sets a custom slog handler for the manager
func (mb *qwrBuilder) WithLogHandler(handler slog.Handler) *qwrBuilder {
	mb.logHandler = handler
	return mb
}

// WithJSONLogs configures the manager to use JSON-formatted logs
func (mb *qwrBuilder) WithJSONLogs() *qwrBuilder {
	output := os.Stderr
	if mb.logOutput != nil {
		output = mb.logOutput
	}

	// Use the stored log level
	opts := &slog.HandlerOptions{Level: mb.logLevel}
	mb.logHandler = slog.NewJSONHandler(output, opts)
	return mb
}

// WithLogOutput sets the output for logs
func (mb *qwrBuilder) WithLogOutput(output *os.File) *qwrBuilder {
	mb.logOutput = output

	// Use the stored log level
	opts := &slog.HandlerOptions{Level: mb.logLevel}

	// Recreate handler with new output
	switch mb.logHandler.(type) {
	case *slog.JSONHandler:
		mb.logHandler = slog.NewJSONHandler(output, opts)
	default:
		mb.logHandler = slog.NewTextHandler(output, opts)
	}

	return mb
}

// Debug sets the log level to Debug for the manager
func (mb *qwrBuilder) Debug() *qwrBuilder {
	return mb.WithLogLevel(slog.LevelDebug)
}

// Reader sets the reader profile
func (mb *qwrBuilder) Reader(p *profile.Profile) *qwrBuilder {
	mb.readerProfile = p
	return mb
}

// Writer sets the writer profile
func (mb *qwrBuilder) Writer(p *profile.Profile) *qwrBuilder {
	mb.writerProfile = p
	return mb
}

// WithErrorDB sets the path for the error log database.
// If not set, persistent error logging is disabled.
// This method works with both New() and NewSQL() constructors.
// In-memory error logging could cause unbounded memory growth in long-running applications.
func (mb *qwrBuilder) WithErrorDB(path string) *qwrBuilder {
	// Reject :memory: for error queue - it could cause memory leaks
	if path == ":memory:" {
		slog.Warn("Rejecting :memory: for error log database - could cause unbounded memory growth. Error logging will be disabled.")
		return mb
	}
	mb.options.ErrorLogPath = path
	return mb
}

// Open initializes and opens database connections
//
// After Open() is called, all options become immutable and cannot be changed.
// The manager must be closed and recreated to modify configuration.
func (mb *qwrBuilder) Open() (*Manager, error) {
	// Set up the slog logger with the configured handler
	logger := slog.New(mb.logHandler)
	slog.SetDefault(logger)

	// Validate path only if we need to open databases ourselves
	if mb.reader == nil && mb.writer == nil && mb.path == "" {
		return nil, errors.New("database path cannot be empty when not using NewSQL()")
	}

	// Ensure internalCtx is never nil - required by BatchCollector
	if mb.internalCtx == nil {
		mb.internalCtx = context.Background()
	}

	// Create directory for database file if it doesn't exist (only if using file-based constructor)
	if mb.path != "" && mb.path != ":memory:" {
		dir := filepath.Dir(mb.path)
		if dir != "." {
			if err := os.MkdirAll(dir, 0700); err != nil {
				return nil, fmt.Errorf("failed to create directory for database: %w", err)
			}
		}
	}

	// Initialize metrics if enabled - required for statement caches
	var metrics *Metrics
	if mb.options.EnableMetrics {
		metrics = NewMetrics()
	}

	// Initialize reader if enabled
	if mb.options.EnableReader {
		if mb.readerProfile == nil {
			mb.readerProfile = profile.ReadBalanced()
		}

		// Open database if not already provided by user
		if mb.reader == nil {
			slog.Debug("Opening reader connection", "path", mb.path)
			var err error
			mb.reader, err = open(mb.path, mb.readerProfile)
			if err != nil {
				return nil, err
			}
		} else {
			// Apply profile to user-provided database
			slog.Debug("Applying reader profile to user-provided connection")
			if err := mb.readerProfile.Apply(mb.reader); err != nil {
				return nil, fmt.Errorf("failed to apply reader profile: %w", err)
			}
		}

		// Initialize reader statement cache
		slog.Debug("Initializing reader statement cache")
		var readerMetrics *StmtCacheMetrics
		if metrics != nil {
			readerMetrics = &metrics.ReaderStmtMetrics
		}
		mb.readStmtCache = NewStmtCache(
			readerMetrics,
			mb.options,
		)
	}

	// Initialize writer if enabled
	if mb.options.EnableWriter {
		if mb.writerProfile == nil {
			mb.writerProfile = profile.WriteBalanced()
		}

		// Open database if not already provided by user
		if mb.writer == nil {
			slog.Debug("Opening writer connection", "path", mb.path)
			var err error
			mb.writer, err = open(mb.path, mb.writerProfile)
			if err != nil {
				if mb.reader != nil {
					_ = mb.reader.Close()
				}
				return nil, err
			}
		} else {
			// Apply profile to user-provided database
			slog.Debug("Applying writer profile to user-provided connection")
			if err := mb.writerProfile.Apply(mb.writer); err != nil {
				if mb.reader != nil {
					_ = mb.reader.Close()
				}
				return nil, fmt.Errorf("failed to apply writer profile: %w", err)
			}
		}

		// Initialize writer statement cache
		slog.Debug("Initializing writer statement cache")
		var writerMetrics *StmtCacheMetrics
		if metrics != nil {
			writerMetrics = &metrics.WriterStmtMetrics
		}
		mb.writeStmtCache = NewStmtCache(
			writerMetrics,
			mb.options,
		)

		// Create worker pool - no ErrorQueue dependency
		slog.Debug("Initializing worker pool")
		mb.serialiser = NewWorkerPool(
			mb.writer,
			mb.options.WorkerQueueDepth,
			metrics,
			mb.writeStmtCache,
			mb.options,
		)

		// Initialize ErrorQueue
		// Only initialize with persistent DB if ErrorLogPath is explicitly set
		slog.Debug("Initializing error queue", "errorLogPath", mb.options.ErrorLogPath)
		mb.errorQueue = NewErrorQueue(nil, metrics, mb.options, mb.path)

		// Set up async error handler - Manager handles retries
		mb.serialiser.SetAsyncErrorHandler(mb.handleAsyncError)

		// Start the worker pool
		if mb.ctx != nil {
			mb.serialiser.Start(mb.ctx)
		} else {
			mb.serialiser.Start(context.Background())
		}

		// Start retry processor if auto-retry is enabled
		if mb.options.EnableAutoRetry {
			mb.startRetryProcessor()
		}

		// Initialize batch collector
		slog.Debug("Initializing batch collector",
			"batchSize", mb.options.BatchSize,
			"timeout", mb.options.BatchTimeout)
		mb.batcher = NewBatchCollector(
			mb.internalCtx,
			mb.serialiser,
			mb.options,
			mb.path,
		)
	}

	slog.Info("QWR manager initialized successfully: " + mb.Database())
	return mb.Manager, nil
}

// Helper function to open a database with a profile
func open(path string, profile *profile.Profile) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	// Apply profile settings
	if err := profile.Apply(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}
