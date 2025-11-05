package qwr

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"time"
)

// WriteSerialiser manages a single worker that processes database jobs
type WriteSerialiser struct {
	db            *sql.DB
	queue         chan workItem
	workerRunning atomic.Bool
	workerWg      sync.WaitGroup
	metrics       *Metrics
	writeCache    *StmtCache
	mu            sync.RWMutex
	shutdownOnce  sync.Once

	// Callback for handling async errors
	onAsyncError func(Query, error, time.Duration)

	// Store timeout values directly
	jobTimeout         time.Duration
	transactionTimeout time.Duration
	queueSubmitTimeout time.Duration
}

type workItem struct {
	job        Job
	ctx        context.Context
	queuedAt   time.Time
	resultChan chan JobResult
}

// NewWorkerPool creates a new worker pool for database jobs
func NewWorkerPool(db *sql.DB, queueDepth int, metrics *Metrics, writeCache *StmtCache, options Options) *WriteSerialiser {
	wp := &WriteSerialiser{
		db:                 db,
		queue:              make(chan workItem, queueDepth),
		metrics:            metrics,
		writeCache:         writeCache,
		jobTimeout:         options.JobTimeout,
		transactionTimeout: options.TransactionTimeout,
		queueSubmitTimeout: options.QueueSubmitTimeout,
	}

	wp.workerRunning.Store(true)
	return wp
}

// SetAsyncErrorHandler sets the callback for handling async errors
func (wp *WriteSerialiser) SetAsyncErrorHandler(handler func(Query, error, time.Duration)) {
	wp.onAsyncError = handler
}

// Start begins the worker processing loop
func (wp *WriteSerialiser) Start(ctx context.Context) {
	wp.workerWg.Add(1)
	go func() {
		defer wp.workerWg.Done()
		wp.worker(ctx)
	}()
}

// Stop shuts down the worker pool
func (wp *WriteSerialiser) Stop() error {
	var err error

	wp.shutdownOnce.Do(func() {
		// Signal shutdown to prevent new submissions
		wp.workerRunning.Store(false)

		// Close the queue to signal worker to stop after processing remaining items
		wp.mu.Lock()
		close(wp.queue)
		wp.mu.Unlock()

		// Wait for worker to finish processing all remaining items and exit
		wp.workerWg.Wait()
	})

	return err
}

// SubmitWait submits a job to the queue and waits for its result
func (wp *WriteSerialiser) SubmitWait(ctx context.Context, job Job) (JobResult, error) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if !wp.workerRunning.Load() {
		return JobResult{}, ErrWorkerNotRunning
	}

	resultChan := make(chan JobResult, 1)
	item := workItem{
		job:        job,
		ctx:        ctx,
		queuedAt:   time.Now(),
		resultChan: resultChan,
	}

	select {
	case wp.queue <- item:
		if wp.metrics != nil {
			wp.metrics.recordJobQueued()
		}
	case <-ctx.Done():
		close(resultChan) // Clean up channel since job was never submitted
		return JobResult{}, ctx.Err()
	}

	select {
	case result := <-resultChan:
		return result, result.Error()
	case <-ctx.Done():
		return JobResult{}, ctx.Err()
	}
}

// SubmitNoWait submits a job to the queue without waiting
func (wp *WriteSerialiser) SubmitNoWait(ctx context.Context, job Job) (int64, error) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if !wp.workerRunning.Load() {
		return 0, ErrWorkerNotRunning
	}

	item := workItem{
		job:        job,
		ctx:        ctx,
		queuedAt:   time.Now(),
		resultChan: nil,
	}

	select {
	case wp.queue <- item:
		if wp.metrics != nil {
			wp.metrics.recordJobQueued()
		}
		return job.ID(), nil
	case <-ctx.Done():
		return job.ID(), ctx.Err()
	}
}

// SubmitWaitNoContext submits a job without using any context
func (wp *WriteSerialiser) SubmitWaitNoContext(job Job) (JobResult, error) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if !wp.workerRunning.Load() {
		return JobResult{}, ErrWorkerNotRunning
	}

	resultChan := make(chan JobResult, 1)
	item := workItem{
		job:        job,
		ctx:        nil,
		queuedAt:   time.Now(),
		resultChan: resultChan,
	}

	// Submit to queue with timeout to prevent deadlock when queue is full
	select {
	case wp.queue <- item:
		if wp.metrics != nil {
			wp.metrics.recordJobQueued()
		}
	case <-time.After(wp.queueSubmitTimeout):
		close(resultChan) // Clean up channel since job was never submitted
		return JobResult{}, ErrQueueTimeout
	}

	result := <-resultChan
	return result, result.Error()
}

// SubmitNoWaitNoContext submits a job without using any context
func (wp *WriteSerialiser) SubmitNoWaitNoContext(job Job) (int64, error) {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if !wp.workerRunning.Load() {
		return 0, ErrWorkerNotRunning
	}

	item := workItem{
		job:        job,
		ctx:        nil,
		queuedAt:   time.Now(),
		resultChan: nil,
	}

	// Submit to queue with timeout to prevent deadlock when queue is full
	select {
	case wp.queue <- item:
		if wp.metrics != nil {
			wp.metrics.recordJobQueued()
		}
		return job.ID(), nil
	case <-time.After(wp.queueSubmitTimeout):
		return 0, ErrQueueTimeout
	}
}

func (wp *WriteSerialiser) worker(ctx context.Context) {
	for {
		select {
		case item, ok := <-wp.queue:
			if !ok {
				// Channel closed - exit worker
				return
			}
			wp.processJob(item)

		case <-ctx.Done():
			return
		}
	}
}

// processJob handles the execution of a single job
func (wp *WriteSerialiser) processJob(item workItem) {
	// Calculate queue wait time and record job start
	queueWaitTime := time.Since(item.queuedAt)

	jobStart := time.Now()
	var jobResult JobResult
	var jobType string

	// Execute jobs directly without wrapper methods
	switch item.job.Type {
	case JobTypeQuery:
		jobType = "query"
		jobResult = wp.executeQuery(item.ctx, item.job.Query)

	case JobTypeBatch:
		jobType = "batch"
		jobResult = item.job.BatchJob.ExecuteWithContext(item.ctx, wp.db)

	case JobTypeTransaction:
		jobType = "transaction"
		jobResult = item.job.Transaction.ExecuteWithContext(item.ctx, wp.db)

	default:
		jobType = "unknown"
		jobResult = NewQueryResult(QueryResult{
			id:  item.job.ID(),
			err: ErrInvalidQuery,
		})
	}

	execTime := time.Since(jobStart)
	failed := jobResult.Error() != nil

	// Record metrics in single batched call
	if wp.metrics != nil {
		wp.metrics.recordJobExecution(queueWaitTime, execTime, failed, jobType)
	}

	// Handle failed async jobs using callback to Manager
	if failed && item.resultChan == nil && wp.onAsyncError != nil {
		if item.job.Type == JobTypeQuery && item.job.Query.async {
			wp.onAsyncError(item.job.Query, jobResult.Error(), jobResult.Duration())
		}
	}

	// Deliver result if someone is waiting
	if item.resultChan != nil {
		select {
		case item.resultChan <- jobResult:
		default:
			// Nobody listening - still send to buffered channel and close
			item.resultChan <- jobResult
		}
		close(item.resultChan)
	}
}

// executeQuery handles Query execution with cache access
func (wp *WriteSerialiser) executeQuery(ctx context.Context, query Query) JobResult {
	start := time.Now()
	result := &QueryResult{
		id: query.id,
	}

	var sqlResult sql.Result
	var err error

	// Handle prepared statement execution with cache
	if query.prepared && wp.writeCache != nil {
		stmt, stmtErr := wp.writeCache.Get(wp.db, query.SQL)
		if stmtErr != nil {
			result.err = stmtErr
			result.duration = time.Since(start)
			return NewQueryResult(*result)
		}

		if ctx != nil {
			jobCtx, cancel := context.WithTimeout(ctx, wp.jobTimeout)
			defer cancel()
			sqlResult, err = stmt.ExecContext(jobCtx, query.Args...)
		} else {
			sqlResult, err = stmt.Exec(query.Args...)
		}
	} else {
		// Direct execution
		if ctx != nil {
			jobCtx, cancel := context.WithTimeout(ctx, wp.jobTimeout)
			defer cancel()
			sqlResult, err = wp.db.ExecContext(jobCtx, query.SQL, query.Args...)
		} else {
			sqlResult, err = wp.db.Exec(query.SQL, query.Args...)
		}
	}

	result.SQLResult = sqlResult
	result.err = err
	result.duration = time.Since(start)
	return NewQueryResult(*result)
}
