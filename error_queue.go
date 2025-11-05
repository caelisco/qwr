package qwr

import (
	"container/list"
	"database/sql"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/caelisco/qwr/profile"
	"github.com/fxamacker/cbor/v2"
)

// ErrorQueue maintains a simple registry of failed async operations
type ErrorQueue struct {
	mu        sync.RWMutex
	errors    map[int64]*list.Element
	errorList *list.List
	db        *sql.DB

	metrics *Metrics
	options Options // Store options by value since they're immutable
	dbPath  string  // Database path for logging context
}

// NewErrorQueue creates a new error queue
func NewErrorQueue(ws *WriteSerialiser, metrics *Metrics, opts Options, dbPath string) *ErrorQueue {
	eq := &ErrorQueue{
		errors:    make(map[int64]*list.Element),
		errorList: list.New(),
		metrics:   metrics,
		options:   opts, // Store by value
		dbPath:    dbPath,
	}

	// Only initialize error log DB if ErrorLogPath is set and is not :memory:
	// We reject :memory: to prevent unbounded memory growth in long-running apps
	if opts.ErrorLogPath != "" && opts.ErrorLogPath != ":memory:" {
		eq.initErrorLogDb(opts.ErrorLogPath)
	}

	return eq
}

// dbName extracts the database filename for logging context
func (eq *ErrorQueue) dbName() string {
	if eq.dbPath == "" {
		return ""
	}
	return filepath.Base(eq.dbPath)
}

// Close shuts down the error queue
func (eq *ErrorQueue) Close() {
	if eq.db != nil {
		eq.db.Close()
	}
}

// Store adds or overwrites an error in the queue, or immediately persists non-retriable errors
func (eq *ErrorQueue) Store(jobErr JobError) {
	// Non-retriable errors are immediately persisted to DB and not queued for retry
	if jobErr.errType == nil || !jobErr.errType.IsRetriable() {
		eq.PersistError(jobErr, "non_retriable")
		errTypeStr := "unknown"
		if jobErr.errType != nil {
			errTypeStr = jobErr.errType.Strategy.String()
		}
		slog.Debug("Non-retriable error persisted to database", "jobID", jobErr.Query.ID(), "errorType", errTypeStr, "db", eq.dbName())
		return
	}

	eq.mu.Lock()
	defer eq.mu.Unlock()

	if elem, exists := eq.errors[jobErr.Query.ID()]; exists {
		elem.Value = jobErr
		eq.errorList.MoveToBack(elem)
		return
	}

	elem := eq.errorList.PushBack(jobErr)
	eq.errors[jobErr.Query.ID()] = elem

	if eq.metrics != nil {
		eq.metrics.recordErrorAdded()
	}

	eq.enforceMaxSize()

	errTypeStr := "unknown"
	if jobErr.errType != nil {
		errTypeStr = jobErr.errType.Strategy.String()
	}
	slog.Debug("Added error to queue", "jobID", jobErr.Query.ID(), "errorType", errTypeStr, "db", eq.dbName())
}

// Get retrieves a specific error by job ID
func (eq *ErrorQueue) Get(jobID int64) (JobError, bool) {
	eq.mu.RLock()
	defer eq.mu.RUnlock()

	if elem, exists := eq.errors[jobID]; exists {
		jobErr, ok := elem.Value.(JobError)
		if !ok {
			return JobError{}, false
		}
		return jobErr, true
	}
	return JobError{}, false
}

// Remove removes an error from the queue
func (eq *ErrorQueue) Remove(jobID int64) bool {
	eq.mu.Lock()
	defer eq.mu.Unlock()

	if elem, exists := eq.errors[jobID]; exists {
		eq.errorList.Remove(elem)
		delete(eq.errors, jobID)
		slog.Debug("Removed error from queue", "jobID", jobID, "db", eq.dbName())
		return true
	}
	return false
}

// GetAll returns all errors in the queue in chronological order
func (eq *ErrorQueue) GetAll() []JobError {
	eq.mu.RLock()
	defer eq.mu.RUnlock()

	errors := make([]JobError, 0, eq.errorList.Len())
	for e := eq.errorList.Front(); e != nil; e = e.Next() {
		jobErr, ok := e.Value.(JobError)
		if ok {
			errors = append(errors, jobErr)
		}
	}
	return errors
}

// Count returns the number of errors in the queue
func (eq *ErrorQueue) Count() int {
	eq.mu.RLock()
	defer eq.mu.RUnlock()
	return eq.errorList.Len()
}

// Clear removes all errors from the queue
func (eq *ErrorQueue) Clear() {
	eq.mu.Lock()
	defer eq.mu.Unlock()

	count := eq.errorList.Len()
	eq.errors = make(map[int64]*list.Element)
	eq.errorList = list.New()
	slog.Info("Cleared error queue", "removedCount", count, "db", eq.dbName())
}

// enforceMaxSize removes oldest entries when queue exceeds max size
func (eq *ErrorQueue) enforceMaxSize() {
	if eq.options.ErrorQueueMaxSize <= 0 {
		return
	}

	excess := eq.errorList.Len() - eq.options.ErrorQueueMaxSize
	if excess <= 0 {
		return
	}

	slog.Warn("Error queue overflow: removing oldest entries",
		"removing", excess,
		"maxSize", eq.options.ErrorQueueMaxSize,
		"db", eq.dbName())

	for i := 0; i < excess && eq.errorList.Front() != nil; i++ {
		oldest := eq.errorList.Front()
		jobErr, ok := oldest.Value.(JobError)
		if !ok {
			// Skip corrupted entry
			eq.errorList.Remove(oldest)
			continue
		}

		eq.PersistError(jobErr, "queue_overflow")
		eq.errorList.Remove(oldest)
		delete(eq.errors, jobErr.Query.ID())

		if eq.metrics != nil {
			eq.metrics.recordErrorDropped()
		}
	}
}

// GetReadyForRetry returns errors that are ready for retry
func (eq *ErrorQueue) GetReadyForRetry(now time.Time, maxRetries int) []JobError {
	eq.mu.RLock()
	defer eq.mu.RUnlock()

	readyErrors := make([]JobError, 0)
	for e := eq.errorList.Front(); e != nil; e = e.Next() {
		jobErr, ok := e.Value.(JobError)
		if !ok {
			continue
		}

		if jobErr.ShouldRetry(maxRetries) &&
			!jobErr.nextRetryAt.IsZero() &&
			now.After(jobErr.nextRetryAt) {
			readyErrors = append(readyErrors, jobErr)
		}
	}
	return readyErrors
}

// PersistError logs an error to the database
func (eq *ErrorQueue) PersistError(jobErr JobError, reason string) error {
	if eq.db == nil {
		return nil
	}

	var argsCbor []byte

	if len(jobErr.Query.Args) > 0 {
		if encoded, err := cbor.Marshal(jobErr.Query.Args); err == nil {
			argsCbor = encoded
		}
	}

	insertSQL := `
		INSERT INTO error_log (
			job_id, error_message, error_type, sql_statement, 
			args_cbor, attempts, first_error_at, last_error_at, reason
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := eq.db.Exec(insertSQL,
		jobErr.Query.ID(),
		jobErr.err.Error(),
		func() string {
			if jobErr.errType != nil {
				return jobErr.errType.Strategy.String()
			}
			return "unknown"
		}(),
		jobErr.Query.SQL,
		argsCbor,
		jobErr.Query.retries,
		jobErr.timestamp,
		time.Now(),
		reason)

	if err != nil {
		slog.Error("Failed to persist error to database", "jobID", jobErr.Query.ID(), "error", err, "db", eq.dbName())
	}

	return err
}

// initErrorLogDb creates and initialises the error log database
func (eq *ErrorQueue) initErrorLogDb(errorLogPath string) {
	var err error

	eq.db, err = sql.Open("sqlite", errorLogPath)
	if err != nil {
		slog.Error("Failed to open error log database", "path", errorLogPath, "error", err, "db", eq.dbName())
		return
	}

	p := profile.WriteBalanced()

	if err := p.Apply(eq.db); err != nil {
		slog.Error("Failed to apply profile to error log database", "error", err, "db", eq.dbName())
		eq.db.Close()
		return
	}

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS error_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id INTEGER NOT NULL,
			error_message TEXT NOT NULL,
			error_type TEXT NOT NULL,
			sql_statement TEXT,
			args_cbor BLOB,
			attempts INTEGER DEFAULT 1,
			first_error_at DATETIME,
			last_error_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			reason TEXT NOT NULL
		)
	`

	if _, err := eq.db.Exec(createTableSQL); err != nil {
		slog.Error("Failed to create error log table", "error", err, "db", eq.dbName())
		eq.db.Close()
		return
	}

	slog.Debug("Error log database initialised", "path", errorLogPath, "db", eq.dbName())
}
