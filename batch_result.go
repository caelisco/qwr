package qwr

import "time"

// BatchResult represents the outcome of a batch execution
type BatchResult struct {
	Results  []JobResult
	err      error
	id       int64
	duration time.Duration
}

// GetID returns the ID of the batch that produced this result
func (r *BatchResult) ID() int64 {
	return r.id
}

// GetError returns any error that occurred during execution
func (r *BatchResult) Error() error {
	return r.err
}

// GetDuration returns how long the batch took to execute
func (r *BatchResult) Duration() time.Duration {
	return r.duration
}
