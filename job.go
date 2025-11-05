package qwr

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"
)

// Job represents a database job that can be executed
type Job struct {
	Type        JobType
	Query       Query
	Transaction Transaction
	BatchJob    BatchJob
}

type JobType int

const (
	JobTypeQuery JobType = iota
	JobTypeTransaction
	JobTypeBatch
)

// ID returns the unique identifier for this job
func (j Job) ID() int64 {
	switch j.Type {
	case JobTypeQuery:
		return j.Query.ID()
	case JobTypeTransaction:
		return j.Transaction.ID()
	case JobTypeBatch:
		return j.BatchJob.ID()
	default:
		return 0
	}
}

// ExecuteWithContext runs the job against the database with context
func (j Job) ExecuteWithContext(ctx context.Context, db *sql.DB) JobResult {
	switch j.Type {
	case JobTypeQuery:
		return j.Query.ExecuteWithContext(ctx, db)
	case JobTypeTransaction:
		return j.Transaction.ExecuteWithContext(ctx, db)
	case JobTypeBatch:
		return j.BatchJob.ExecuteWithContext(ctx, db)
	default:
		return JobResult{}
	}
}

// JobResult represents the outcome of a job execution
type JobResult struct {
	Type             ResultType
	QueryResult      QueryResult
	TransactionResult TransactionResult
	BatchResult      BatchResult
}

type ResultType int

const (
	ResultTypeQuery ResultType = iota
	ResultTypeTransaction
	ResultTypeBatch
)

// ID returns the ID of the job that produced this result
func (jr JobResult) ID() int64 {
	switch jr.Type {
	case ResultTypeQuery:
		return jr.QueryResult.ID()
	case ResultTypeTransaction:
		return jr.TransactionResult.ID()
	case ResultTypeBatch:
		return jr.BatchResult.ID()
	default:
		return 0
	}
}

// Error returns any error that occurred during execution
func (jr JobResult) Error() error {
	switch jr.Type {
	case ResultTypeQuery:
		return jr.QueryResult.Error()
	case ResultTypeTransaction:
		return jr.TransactionResult.Error()
	case ResultTypeBatch:
		return jr.BatchResult.Error()
	default:
		return nil
	}
}

// Duration returns how long the job took to execute
func (jr JobResult) Duration() time.Duration {
	switch jr.Type {
	case ResultTypeQuery:
		return jr.QueryResult.Duration()
	case ResultTypeTransaction:
		return jr.TransactionResult.Duration()
	case ResultTypeBatch:
		return jr.BatchResult.Duration()
	default:
		return 0
	}
}

// JobError represents an error that occurred during async job execution
type JobError struct {
	Query       Query         // The query that failed
	err         error         // The error that occurred
	timestamp   time.Time     // When the error occurred
	duration    time.Duration // How long the job took before failing
	errType     *QWRError     // Classification of the error
	nextRetryAt time.Time     // When to attempt next retry
}

func (je JobError) ID() int64 {
	return je.Query.ID()
}

// String returns a string representation of the error
func (je JobError) String() string {
	return fmt.Sprintf("JobError{ID:%d, Error:%v, Timestamp:%v, Retries:%d}",
		je.Query.ID(), je.err, je.timestamp.Format(time.RFC3339), je.Query.retries)
}

// GetSQL returns the SQL statement and arguments
func (je JobError) SQL() (string, []any) {
	return je.Query.SQL, je.Query.Args
}

// Age returns how long ago the error occurred
func (je JobError) Age() time.Duration {
	return time.Since(je.timestamp)
}

// CalculateNextRetry calculates when this job should be retried next
// Uses exponential backoff with jitter to prevent thundering herd
func (je *JobError) CalculateNextRetry(baseDelay time.Duration) {
	// Exponential backoff: baseDelay * 2^(retries)
	// Cap shift amount to prevent integer overflow (max 2^30)
	shift := je.Query.retries
	if shift > 30 {
		shift = 30
	}
	delay := baseDelay * time.Duration(1<<shift)

	// Add ±25% jitter to prevent synchronized retry storms
	variation := delay / 4
	jitterAmount := time.Duration(rand.Int63n(int64(variation*2))) - variation
	delay += jitterAmount

	je.nextRetryAt = time.Now().Add(delay)
}

// ShouldRetry determines if this error should be retried based on error type and retry count
func (je JobError) ShouldRetry(maxRetries int) bool {
	return je.errType != nil && je.errType.IsRetriable() && je.Query.retries < maxRetries
}

// CreateRetryQuery creates a new query for retry with incremented retry count
func (je JobError) CreateRetryQuery() Query {
	retryQuery := je.Query
	retryQuery.retries++
	return retryQuery
}

// NewQueryJob creates a Job from a Query
func NewQueryJob(q Query) Job {
	return Job{Type: JobTypeQuery, Query: q}
}

// NewTransactionJob creates a Job from a Transaction
func NewTransactionJob(t Transaction) Job {
	return Job{Type: JobTypeTransaction, Transaction: t}
}

// NewBatchJob creates a Job from a BatchJob
func NewBatchJobJob(b BatchJob) Job {
	return Job{Type: JobTypeBatch, BatchJob: b}
}

// NewQueryResult creates a JobResult from a QueryResult
func NewQueryResult(qr QueryResult) JobResult {
	return JobResult{Type: ResultTypeQuery, QueryResult: qr}
}

// NewTransactionResult creates a JobResult from a TransactionResult
func NewTransactionResult(tr TransactionResult) JobResult {
	return JobResult{Type: ResultTypeTransaction, TransactionResult: tr}
}

// NewBatchResult creates a JobResult from a BatchResult
func NewBatchResult(br BatchResult) JobResult {
	return JobResult{Type: ResultTypeBatch, BatchResult: br}
}
