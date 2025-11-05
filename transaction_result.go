package qwr

import "time"

// TransactionResult represents the outcome of a transaction execution
type TransactionResult struct {
	Results  []*QueryResult
	err      error
	id       int64
	duration time.Duration
}

// GetID returns the ID of the transaction that produced this result
func (r *TransactionResult) ID() int64 {
	return r.id
}

// GetError returns any error that occurred during execution
func (r *TransactionResult) Error() error {
	return r.err
}

// GetDuration returns how long the transaction took to execute
func (r *TransactionResult) Duration() time.Duration {
	return r.duration
}
