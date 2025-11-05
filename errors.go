package qwr

import (
	"errors"
	"strings"
	"time"
)

// Error constants
var (
	ErrReaderDisabled            = errors.New("reader is disabled")
	ErrWriterDisabled            = errors.New("writer is disabled")
	ErrResultNotFound            = errors.New("result not found")
	ErrInvalidResult             = errors.New("invalid result type")
	ErrQueryTooLarge             = errors.New("query exceeds maximum allowed size")
	ErrStatementCacheFull        = errors.New("statement cache is full")
	ErrHashCollision             = errors.New("statement hash collision")
	ErrErrorQueueDisabled        = errors.New("error queue is disabled")
	ErrJobNotFound               = errors.New("job not found in error queue")
	ErrWorkerNotRunning          = errors.New("worker pool is not running")
	ErrQueueTimeout              = errors.New("timeout waiting for queue to accept submission")
	ErrRetrySubmissionFailed     = errors.New("failed to resubmit job for retry")
	ErrInvalidQuery              = errors.New("query is invalid")
	ErrNilPreparedStatement      = errors.New("internal error: prepared statement is nil before execution")
	ErrNilPreparedStatementCache = errors.New("internal error: global prepared statement cache returned nil statement without error")
	ErrFailedToPrepareStatement  = errors.New("failed to prepare statement")
	ErrPreparedCacheRequired     = errors.New("prepared statement cache is required when using prepared queries")
	ErrCacheClosed               = errors.New("statement cache is closed")
	ErrBatchContainsNonQuery     = errors.New("batch jobs can only contain Query jobs, not Transaction or nested Batch jobs")
	ErrConnectionUnhealthy       = errors.New("database connection is unhealthy")
)


// ErrorCategory provides granular error classification for better handling
type ErrorCategory int

const (
	// ErrorCategoryConnection indicates connection-related errors
	ErrorCategoryConnection ErrorCategory = iota
	// ErrorCategoryLock indicates database locking/concurrency errors
	ErrorCategoryLock
	// ErrorCategoryConstraint indicates constraint violation errors
	ErrorCategoryConstraint
	// ErrorCategorySchema indicates schema-related errors
	ErrorCategorySchema
	// ErrorCategoryResource indicates resource exhaustion errors
	ErrorCategoryResource
	// ErrorCategoryTimeout indicates timeout/deadline errors
	ErrorCategoryTimeout
	// ErrorCategoryPermission indicates access control errors
	ErrorCategoryPermission
	// ErrorCategoryInternal indicates internal QWR errors
	ErrorCategoryInternal
	// ErrorCategoryUnknown indicates unclassified errors
	ErrorCategoryUnknown
)

// String returns the string representation of ErrorCategory
func (ec ErrorCategory) String() string {
	switch ec {
	case ErrorCategoryConnection:
		return "connection"
	case ErrorCategoryLock:
		return "lock"
	case ErrorCategoryConstraint:
		return "constraint"
	case ErrorCategorySchema:
		return "schema"
	case ErrorCategoryResource:
		return "resource"
	case ErrorCategoryTimeout:
		return "timeout"
	case ErrorCategoryPermission:
		return "permission"
	case ErrorCategoryInternal:
		return "internal"
	case ErrorCategoryUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

// RetryStrategy defines how errors should be retried
type RetryStrategy int

const (
	// RetryStrategyNone indicates no retry should be attempted
	RetryStrategyNone RetryStrategy = iota
	// RetryStrategyImmediate indicates immediate retry with no delay
	RetryStrategyImmediate
	// RetryStrategyExponential indicates exponential backoff retry
	RetryStrategyExponential
	// RetryStrategyLinear indicates linear backoff retry
	RetryStrategyLinear
)

// String returns the string representation of RetryStrategy
func (rs RetryStrategy) String() string {
	switch rs {
	case RetryStrategyNone:
		return "none"
	case RetryStrategyImmediate:
		return "immediate"
	case RetryStrategyExponential:
		return "exponential"
	case RetryStrategyLinear:
		return "linear"
	default:
		return "unknown"
	}
}

// QWRError provides structured error information with enhanced classification
type QWRError struct {
	// Original error from the underlying operation
	Original error
	// Category of the error for granular handling
	Category ErrorCategory
	// RetryStrategy for this specific error
	Strategy RetryStrategy
	// Context provides additional information about the error
	Context map[string]any
	// Timestamp when the error occurred
	Timestamp time.Time
	// Operation that caused the error (query, transaction, etc.)
	Operation string
}

// Error implements the error interface
func (qe *QWRError) Error() string {
	if qe.Original != nil {
		return qe.Original.Error()
	}
	return "unknown QWR error"
}

// Unwrap allows error unwrapping for errors.Is and errors.As
func (qe *QWRError) Unwrap() error {
	return qe.Original
}

// IsRetriable returns true if the error should be retried
func (qe *QWRError) IsRetriable() bool {
	return qe.Strategy != RetryStrategyNone
}

// NewQWRError creates a new structured QWR error
func NewQWRError(original error, category ErrorCategory, strategy RetryStrategy, operation string) *QWRError {
	return &QWRError{
		Original:  original,
		Category:  category,
		Strategy:  strategy,
		Context:   make(map[string]any),
		Timestamp: time.Now(),
		Operation: operation,
	}
}

// WithContext adds context information to the error
func (qe *QWRError) WithContext(key string, value any) *QWRError {
	if qe.Context == nil {
		qe.Context = make(map[string]any)
	}
	qe.Context[key] = value
	return qe
}

// ClassifyError provides enhanced error classification with detailed categorization
func ClassifyError(err error, operation string) *QWRError {
	if err == nil {
		return NewQWRError(nil, ErrorCategoryInternal, RetryStrategyNone, operation)
	}

	errMsg := strings.ToLower(err.Error())

	// Database locking errors (most common retriable case)
	if strings.Contains(errMsg, "database is locked") ||
		strings.Contains(errMsg, "database locked") ||
		strings.Contains(errMsg, "sqlite_busy") ||
		strings.Contains(errMsg, "busy") {
		return NewQWRError(err, ErrorCategoryLock, RetryStrategyExponential, operation).
			WithContext("retry_delay", "short").
			WithContext("max_attempts", 5)
	}

	// Context timeout errors
	if strings.Contains(errMsg, "context") &&
		(strings.Contains(errMsg, "deadline exceeded") || strings.Contains(errMsg, "timeout")) {
		return NewQWRError(err, ErrorCategoryTimeout, RetryStrategyLinear, operation).
			WithContext("retry_delay", "medium").
			WithContext("max_attempts", 3)
	}

	// File I/O and access errors
	if strings.Contains(errMsg, "i/o error") ||
		strings.Contains(errMsg, "broken pipe") ||
		strings.Contains(errMsg, "permission denied") ||
		strings.Contains(errMsg, "access denied") {
		return NewQWRError(err, ErrorCategoryConnection, RetryStrategyLinear, operation).
			WithContext("retry_delay", "medium").
			WithContext("max_attempts", 2)
	}

	// Resource exhaustion errors
	if strings.Contains(errMsg, "i/o error") ||
		strings.Contains(errMsg, "disk full") ||
		strings.Contains(errMsg, "no space left") ||
		strings.Contains(errMsg, "out of memory") ||
		strings.Contains(errMsg, "resource temporarily unavailable") {
		return NewQWRError(err, ErrorCategoryResource, RetryStrategyLinear, operation).
			WithContext("retry_delay", "long").
			WithContext("max_attempts", 2)
	}

	// Constraint violation errors (non-retriable)
	if strings.Contains(errMsg, "constraint") ||
		strings.Contains(errMsg, "unique") ||
		strings.Contains(errMsg, "foreign key") ||
		strings.Contains(errMsg, "check constraint") ||
		strings.Contains(errMsg, "not null") ||
		strings.Contains(errMsg, "primary key") {
		return NewQWRError(err, ErrorCategoryConstraint, RetryStrategyNone, operation).
			WithContext("data_issue", true)
	}

	// Schema-related errors (non-retriable)
	if strings.Contains(errMsg, "no such table") ||
		strings.Contains(errMsg, "no such column") ||
		strings.Contains(errMsg, "ambiguous column") ||
		strings.Contains(errMsg, "syntax error") ||
		strings.Contains(errMsg, "near") { // SQLite syntax error pattern
		return NewQWRError(err, ErrorCategorySchema, RetryStrategyNone, operation).
			WithContext("schema_issue", true)
	}

	// Permission/access errors (non-retriable)
	if strings.Contains(errMsg, "permission denied") ||
		strings.Contains(errMsg, "access denied") ||
		strings.Contains(errMsg, "unauthorized") ||
		strings.Contains(errMsg, "readonly") ||
		strings.Contains(errMsg, "read-only") {
		return NewQWRError(err, ErrorCategoryPermission, RetryStrategyNone, operation).
			WithContext("access_issue", true)
	}

	// Check for known internal QWR errors
	for _, qwrErr := range []error{
		ErrReaderDisabled, ErrWriterDisabled, ErrWorkerNotRunning,
		ErrErrorQueueDisabled,
	} {
		if errors.Is(err, qwrErr) {
			return NewQWRError(err, ErrorCategoryInternal, RetryStrategyNone, operation).
				WithContext("internal_error", true)
		}
	}

	// Default to unknown category with no retry for safety
	return NewQWRError(err, ErrorCategoryUnknown, RetryStrategyNone, operation).
		WithContext("unclassified", true)
}
