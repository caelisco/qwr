package qwr

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// QueryBuilder object pool to reduce allocations
var queryBuilderPool = sync.Pool{
	New: func() any {
		return &QueryBuilder{
			query: Query{
				Args: make([]any, 0, 6), // Pre-allocate some capacity
			},
		}
	},
}

// GetQueryBuilder gets a pre-allocated QueryBuilder object from pool
func GetQueryBuilder() *QueryBuilder {
	qb, ok := queryBuilderPool.Get().(*QueryBuilder)
	if !ok {
		panic("qwr: pool corruption detected - object in queryBuilderPool is not a *QueryBuilder")
	}
	return qb
}

// ReleaseQueryBuilder returns a QueryBuilder to the pool after cleaning it
func ReleaseQueryBuilder(qb *QueryBuilder) {
	if qb != nil {
		// Reset QueryBuilder fields
		qb.manager = nil
		qb.ctx = nil
		qb.useContexts = false

		// Reset embedded Query fields
		qb.query.SQL = ""
		for i := range qb.query.Args {
			qb.query.Args[i] = nil
		}
		qb.query.Args = qb.query.Args[:0]
		qb.query.id = 0
		qb.query.prepared = false
		qb.query.async = false
		qb.query.retries = 0

		queryBuilderPool.Put(qb)
	}
}

// QueryBuilder provides a fluent API for building and executing queries
type QueryBuilder struct {
	manager     *Manager
	query       Query
	ctx         context.Context
	useContexts bool // Tracks whether to use contexts for this query
}

// WithContext adds a context to the query and enables context usage for this specific query.
// The context will be used for timeouts, cancellation, and deadlines during query execution.
// Note: Batch operations ignore query-level contexts and use the manager's internal context.
func (qb *QueryBuilder) WithContext(ctx context.Context) *QueryBuilder {
	qb.ctx = ctx
	qb.useContexts = true
	return qb
}

// Prepared marks the query to use a prepared statement for execution.
// Prepared statements are cached and reused, reducing parsing overhead for repeated queries.
// Most beneficial for queries executed multiple times with different parameters.
func (qb *QueryBuilder) Prepared() *QueryBuilder {
	qb.query.prepared = true
	return qb
}

// GenID generates a unique ID for this query
func (qb *QueryBuilder) GenID() *QueryBuilder {
	qb.query.id = time.Now().UnixNano()
	return qb
}

// Write executes the query directly on the writer connection, bypassing the worker queue.
// This provides immediate execution and error feedback but may block the caller.
// Returns a QueryResult containing SQL result, error, duration, and query ID.
func (qb *QueryBuilder) Write() (*QueryResult, error) {
	if err := qb.validate(); err != nil {
		ReleaseQueryBuilder(qb)
		return nil, err
	}

	if qb.manager.writer == nil {
		ReleaseQueryBuilder(qb)
		return nil, ErrWriterDisabled
	}

	qb.GenID()
	start := time.Now()
	result := &QueryResult{id: qb.query.id}

	// Handle prepared statement execution
	if qb.query.prepared && qb.manager.writeStmtCache != nil {
		stmt, cacheErr := qb.manager.writeStmtCache.Get(qb.manager.writer, qb.query.SQL)
		if cacheErr != nil {
			result.err = cacheErr
		} else {
			if qb.useContexts && qb.ctx != nil {
				result.SQLResult, result.err = stmt.ExecContext(qb.ctx, qb.query.Args...)
			} else {
				result.SQLResult, result.err = stmt.Exec(qb.query.Args...)
			}
		}
	} else {
		if qb.useContexts && qb.ctx != nil {
			result.SQLResult, result.err = qb.manager.writer.ExecContext(qb.ctx, qb.query.SQL, qb.query.Args...)
		} else {
			result.SQLResult, result.err = qb.manager.writer.Exec(qb.query.SQL, qb.query.Args...)
		}
	}

	result.duration = time.Since(start)

	// Record metrics
	if qb.manager.serialiser != nil && qb.manager.serialiser.metrics != nil && qb.manager.options.EnableMetrics {
		qb.manager.serialiser.metrics.recordDirectWrite(result.duration, result.err != nil)
	}

	ReleaseQueryBuilder(qb)
	return result, result.err
}

// Async submits the query to the worker pool for background execution.
// Returns immediately with a job ID that can be used to check for errors later.
// Failed async queries are automatically added to the error queue for inspection or retry.
func (qb *QueryBuilder) Async() (int64, error) {
	if err := qb.validate(); err != nil {
		ReleaseQueryBuilder(qb)
		return 0, err
	}

	if qb.manager.writer == nil || qb.manager.serialiser == nil {
		ReleaseQueryBuilder(qb)
		return 0, ErrWriterDisabled
	}

	// Check if worker is running
	if !qb.manager.serialiser.workerRunning.Load() {
		ReleaseQueryBuilder(qb)
		return 0, ErrWorkerNotRunning
	}

	qb.GenID()
	id := qb.query.ID()
	qb.query.async = true

	// Prepare the query for worker
	queryForWorker := qb.query
	if len(qb.query.Args) > 0 {
		queryForWorker.Args = make([]any, len(qb.query.Args))
		copy(queryForWorker.Args, qb.query.Args)
	}

	// Determine context
	var ctx context.Context
	if qb.useContexts {
		if qb.ctx != nil {
			ctx = qb.ctx
		} else if qb.manager.ctx != nil {
			ctx = qb.manager.ctx
		}
	} else {
		ctx = nil
	}

	var submitErr error
	if qb.useContexts && qb.ctx != nil {
		_, submitErr = qb.manager.serialiser.SubmitNoWait(ctx, NewQueryJob(queryForWorker))
	} else {
		_, submitErr = qb.manager.serialiser.SubmitNoWaitNoContext(NewQueryJob(queryForWorker))
	}

	ReleaseQueryBuilder(qb)
	return id, submitErr
}

// Execute submits the query to the worker pool and waits for completion.
// Provides queued execution with immediate error feedback. Query will be serialised
// with other operations but the caller will block until completion.
func (qb *QueryBuilder) Execute() (*QueryResult, error) {
	if err := qb.validate(); err != nil {
		ReleaseQueryBuilder(qb)
		return nil, err
	}

	if qb.manager.writer == nil || qb.manager.serialiser == nil {
		ReleaseQueryBuilder(qb)
		return nil, ErrWriterDisabled
	}

	// Check if worker is running
	if !qb.manager.serialiser.workerRunning.Load() {
		ReleaseQueryBuilder(qb)
		return nil, ErrWorkerNotRunning
	}

	qb.GenID()

	queryForWorker := qb.query
	if len(qb.query.Args) > 0 {
		queryForWorker.Args = make([]any, len(qb.query.Args))
		copy(queryForWorker.Args, qb.query.Args)
	}

	// Determine context
	var ctx context.Context
	if qb.useContexts {
		if qb.ctx != nil {
			ctx = qb.ctx
		} else if qb.manager.ctx != nil {
			ctx = qb.manager.ctx
		} else {
			ctx = nil
		}
	}

	var result JobResult
	var err error

	if ctx != nil {
		result, err = qb.manager.serialiser.SubmitWait(ctx, NewQueryJob(queryForWorker))
	} else {
		result, err = qb.manager.serialiser.SubmitWaitNoContext(NewQueryJob(queryForWorker))
	}

	if err != nil {
		ReleaseQueryBuilder(qb)
		return nil, err
	}

	if result.Type != ResultTypeQuery {
		ReleaseQueryBuilder(qb)
		return nil, ErrInvalidResult
	}

	ReleaseQueryBuilder(qb)
	return &result.QueryResult, result.QueryResult.err
}

// Batch adds the query to a batch for deferred execution
//
// IMPORTANT: Batch operations use the manager's internal context, NOT the context
// set via WithContext(). This means:
//   - Query-level contexts (from WithContext()) are ignored for batch operations
//   - All queries in a batch share the same manager-level context
//   - Timeouts and cancellation apply to the entire batch, not individual queries
//
// If you need query-specific context control, use Execute() or Async() instead.
func (qb *QueryBuilder) Batch() (int64, error) {
	if err := qb.validate(); err != nil {
		ReleaseQueryBuilder(qb)
		return 0, err
	}

	if qb.manager.writer == nil || qb.manager.serialiser == nil || qb.manager.batcher == nil {
		ReleaseQueryBuilder(qb)
		return 0, ErrWriterDisabled
	}

	qb.GenID()
	id := qb.query.ID()

	// Copy the query for batching
	queryForBatch := qb.query
	if len(qb.query.Args) > 0 {
		queryForBatch.Args = make([]any, len(qb.query.Args))
		copy(queryForBatch.Args, qb.query.Args)
	}
	queryForBatch.async = true

	// Add to batch - batcher will use its own internal context
	// Note: QueryBuilder context is deliberately ignored for batch operations
	qb.manager.batcher.Add(NewQueryJob(queryForBatch))

	ReleaseQueryBuilder(qb)
	return id, nil
}

// Read executes a read operation on the reader connection pool and returns multiple rows.
// Uses concurrent reader connections for better read performance. Remember to call
// rows.Close() when finished to prevent connection leaks.
func (qb *QueryBuilder) Read() (*sql.Rows, error) {
	if qb.manager.reader == nil {
		ReleaseQueryBuilder(qb)
		return nil, ErrReaderDisabled
	}

	if qb.query.SQL == "" {
		ReleaseQueryBuilder(qb)
		return nil, ErrInvalidQuery
	}

	var rows *sql.Rows
	var err error

	// Execute read query
	if qb.query.prepared && qb.manager.readStmtCache != nil {
		stmt, stmtErr := qb.manager.readStmtCache.Get(qb.manager.reader, qb.query.SQL)
		if stmtErr != nil {
			ReleaseQueryBuilder(qb)
			return nil, stmtErr
		}

		if qb.useContexts && qb.ctx != nil {
			rows, err = stmt.QueryContext(qb.ctx, qb.query.Args...)
		} else {
			rows, err = stmt.Query(qb.query.Args...)
		}
	} else {
		if qb.useContexts && qb.ctx != nil {
			rows, err = qb.manager.reader.QueryContext(qb.ctx, qb.query.SQL, qb.query.Args...)
		} else {
			rows, err = qb.manager.reader.Query(qb.query.SQL, qb.query.Args...)
		}
	}

	ReleaseQueryBuilder(qb)
	return rows, err
}

// ReadRow executes a read operation on the reader connection pool and returns a single row.
// Convenient for queries expected to return exactly one row. Use row.Scan() to extract
// values. sql.ErrNoRows is returned when no rows match the query.
func (qb *QueryBuilder) ReadRow() (*sql.Row, error) {
	if qb.manager.reader == nil {
		ReleaseQueryBuilder(qb)
		return nil, ErrReaderDisabled
	}

	if qb.query.SQL == "" {
		ReleaseQueryBuilder(qb)
		return nil, ErrInvalidQuery
	}

	var row *sql.Row
	var err error

	// Execute read query
	if qb.query.prepared && qb.manager.readStmtCache != nil {
		stmt, stmtErr := qb.manager.readStmtCache.Get(qb.manager.reader, qb.query.SQL)
		if stmtErr != nil {
			ReleaseQueryBuilder(qb)
			return nil, stmtErr
		}

		if qb.useContexts && qb.ctx != nil {
			row = stmt.QueryRowContext(qb.ctx, qb.query.Args...)
		} else {
			row = stmt.QueryRow(qb.query.Args...)
		}
	} else {
		if qb.useContexts && qb.ctx != nil {
			row = qb.manager.reader.QueryRowContext(qb.ctx, qb.query.SQL, qb.query.Args...)
		} else {
			row = qb.manager.reader.QueryRow(qb.query.SQL, qb.query.Args...)
		}
	}

	ReleaseQueryBuilder(qb)
	return row, err // Note: sql.Row.Scan() is where row-level errors are handled
}

// Release manually returns the QueryBuilder to the object pool for reuse.
// Only call this if you don't execute the query (Write/Async/Execute/Read/ReadRow).
// All execution methods automatically release the QueryBuilder when complete.
func (qb *QueryBuilder) Release() {
	ReleaseQueryBuilder(qb)
}

func (qb *QueryBuilder) validate() error {
	if qb.query.SQL == "" {
		return ErrInvalidQuery
	}
	return nil
}
