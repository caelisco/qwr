package qwr

import (
	"database/sql"
	"time"
)

// QueryResult represents the outcome of a query execution
type QueryResult struct {
	SQLResult sql.Result
	err       error
	id        int64
	duration  time.Duration
}

// GetID returns the ID of the query that produced this result
func (r *QueryResult) ID() int64 {
	return r.id
}

// GetError returns any error that occurred during execution
func (r *QueryResult) Error() error {
	return r.err
}

// GetDuration returns how long the query took to execute
func (r *QueryResult) Duration() time.Duration {
	return r.duration
}
