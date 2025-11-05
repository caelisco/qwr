package qwr

import (
	"context"
	"database/sql"
	"time"
)

// Query represents a database query operation
type Query struct {
	SQL      string
	Args     []any
	id       int64
	prepared bool
	async    bool // Indicates if this query was submitted via Async() and should use error queue on failure
	retries  int  // Number of retry attempts this query has undergone
}

// ExecuteWithContext runs the query against the database with context
func (q Query) ExecuteWithContext(ctx context.Context, db *sql.DB) JobResult {
	start := time.Now()
	result := &QueryResult{
		id: q.id,
	}

	var sqlResult sql.Result
	var err error

	// Direct execution without prepared statements
	if ctx != nil {
		sqlResult, err = db.ExecContext(ctx, q.SQL, q.Args...)
	} else {
		sqlResult, err = db.Exec(q.SQL, q.Args...)
	}

	result.SQLResult = sqlResult
	result.err = err
	result.duration = time.Since(start)
	return NewQueryResult(*result)
}

// ID returns the unique identifier for this query
func (q Query) ID() int64 {
	return q.id
}
