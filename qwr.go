package qwr

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/caelisco/qwr/profile"
	_ "modernc.org/sqlite"
)

// Manager handles serialised database operations
type Manager struct {
	reader         *sql.DB
	writer         *sql.DB
	serialiser     *WriteSerialiser
	batcher        *BatchCollector
	errorQueue     *ErrorQueue
	options        Options
	ctx            context.Context // User-facing context (can be nil)
	internalCtx    context.Context // Internal context for batching (never nil)
	readerProfile  *profile.Profile
	writerProfile  *profile.Profile
	readStmtCache  *StmtCache
	writeStmtCache *StmtCache
	path           string // Database path for logging context
}

// Database extracts the database filename for logging context
func (m *Manager) Database() string {
	if m.path == "" {
		return ""
	}
	return filepath.Base(m.path)
}

// Query creates a new query with the given SQL and arguments
func (m *Manager) Query(sql string, args ...any) *QueryBuilder {
	qb := GetQueryBuilder()
	qb.manager = m
	qb.query.SQL = sql
	qb.useContexts = m.options.UseContexts

	// Ensure Args slice has enough capacity
	if cap(qb.query.Args) < len(args) {
		qb.query.Args = make([]any, len(args))
	} else {
		qb.query.Args = qb.query.Args[:len(args)]
	}
	copy(qb.query.Args, args)

	return qb
}

// Batch adds a job to be executed as part of a batch
func (m *Manager) Batch(job Job) error {
	if m.writer == nil || m.serialiser == nil || m.batcher == nil {
		slog.Error("Writer is disabled", "db", m.Database())
		return ErrWriterDisabled
	}

	slog.Debug("Adding job to batch", "id", job.ID(), "db", m.Database())
	m.batcher.Add(job)
	return nil
}

// handleAsyncError processes async job errors - called by WriteSerialiser
func (m *Manager) handleAsyncError(query Query, err error, duration time.Duration) {
	if m.errorQueue == nil {
		return
	}

	qwrErr := ClassifyError(err, "async_query")
	jobErr := JobError{
		Query:     query,
		err:       err,
		timestamp: time.Now(),
		duration:  duration,
		errType:   qwrErr,
	}

	// Calculate next retry time if it's retriable
	if qwrErr.IsRetriable() {
		jobErr.CalculateNextRetry(m.options.BaseRetryDelay)
	}

	m.errorQueue.Store(jobErr)
}

// startRetryProcessor begins processing retries in the background
func (m *Manager) startRetryProcessor() {
	if m.errorQueue == nil || m.serialiser == nil || m.ctx == nil {
		return
	}

	go func() {
		ticker := time.NewTicker(m.options.RetryInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				m.processRetries()
			case <-m.ctx.Done():
				return
			}
		}
	}()
}

// processRetries handles retry logic - calls Query directly instead of through JobError wrapper
func (m *Manager) processRetries() {
	if m.errorQueue == nil || m.serialiser == nil {
		return
	}

	readyJobs := m.errorQueue.GetReadyForRetry(time.Now(), m.options.MaxRetries)
	if len(readyJobs) == 0 {
		return
	}

	for _, jobErr := range readyJobs {
		retryQuery := jobErr.CreateRetryQuery()
		retryQuery.async = true

		ctx, cancel := context.WithTimeout(context.Background(), m.options.RetrySubmitTimeout)
		_, err := m.serialiser.SubmitNoWait(ctx, NewQueryJob(retryQuery))
		cancel()

		if err != nil {
			// Failed to resubmit
			if retryQuery.retries >= m.options.MaxRetries {
				m.errorQueue.Remove(jobErr.Query.ID())
				m.errorQueue.PersistError(jobErr, "max_retries_exceeded")
			} else {
				// Update retry count and next retry time
				updatedJobErr := jobErr
				updatedJobErr.Query.retries = retryQuery.retries
				updatedJobErr.CalculateNextRetry(m.options.BaseRetryDelay)
				m.errorQueue.Store(updatedJobErr)
			}
		} else {
			// Successfully resubmitted
			m.errorQueue.Remove(jobErr.Query.ID())
			// Record the retry in metrics
			if m.serialiser != nil && m.serialiser.metrics != nil {
				m.serialiser.metrics.recordErrorRetried()
			}
			slog.Debug("Successfully resubmitted job for retry",
				"jobID", jobErr.Query.ID(),
				"attempt", retryQuery.retries,
				"db", m.Database())
		}
	}
}

// Close closes all database connections and stops the worker pool
func (m *Manager) Close() error {
	var errs []error

	// Stop batch collector
	if m.batcher != nil {
		slog.Info("Closing batch collector", "db", m.Database())
		m.batcher.Close()
	}

	// Stop worker pool
	if m.serialiser != nil {
		slog.Info("Stopping worker pool", "db", m.Database())
		if err := m.serialiser.Stop(); err != nil {
			slog.Error("Error stopping worker pool", "error", err, "db", m.Database())
			errs = append(errs, err)
		}
	}

	// Close error queue
	if m.errorQueue != nil {
		slog.Info("Closing error queue", "db", m.Database())
		m.errorQueue.Close()
	}

	// Close statement caches
	if m.readStmtCache != nil {
		slog.Info("Closing reader statement cache", "db", m.Database())
		m.readStmtCache.Close()
	}

	if m.writeStmtCache != nil {
		slog.Info("Closing writer statement cache", "db", m.Database())
		m.writeStmtCache.Close()
	}

	// Close writer
	if m.writer != nil {
		slog.Info("Closing writer connection", "db", m.Database())
		if err := m.writer.Close(); err != nil {
			slog.Error("Error closing writer", "error", err, "db", m.Database())
			errs = append(errs, err)
		}
	}

	// Close reader
	if m.reader != nil {
		slog.Info("Closing reader connection", "db", m.Database())
		if err := m.reader.Close(); err != nil {
			slog.Error("Error closing reader", "error", err, "db", m.Database())
			errs = append(errs, err)
		}
	}

	slog.Info("QWR manager closed", "db", m.Database())

	if len(errs) > 0 {
		return errs[0] // Return first error for now
	}
	return nil
}

// GetMetrics returns the current write performance metrics
func (m *Manager) GetMetrics() MetricsSnapshot {
	if m.serialiser != nil && m.serialiser.metrics != nil {
		return m.serialiser.metrics.Snapshot()
	}
	return MetricsSnapshot{} // Empty snapshot if no metrics
}

// GetCacheMetrics returns the current statement cache metrics
func (m *Manager) GetCacheMetrics() map[string]StmtCacheStats {
	metrics := make(map[string]StmtCacheStats)

	if m.serialiser != nil && m.serialiser.metrics != nil {
		metrics["reader"] = m.serialiser.metrics.ReaderStmtMetrics.Stats()
		metrics["writer"] = m.serialiser.metrics.WriterStmtMetrics.Stats()
	}

	return metrics
}

// GetDetailedCacheMetrics returns enhanced cache metrics with detailed information
func (m *Manager) GetDetailedCacheMetrics() map[string]interface{} {
	result := make(map[string]interface{})

	if m.serialiser != nil && m.serialiser.metrics != nil {
		result["reader"] = m.serialiser.metrics.ReaderStmtMetrics.Stats()
		result["writer"] = m.serialiser.metrics.WriterStmtMetrics.Stats()
	}

	return result
}

// GetErrorQueueStats returns basic error queue statistics
func (m *Manager) GetErrorQueueStats() ErrorQueueStats {
	if m.errorQueue == nil {
		return ErrorQueueStats{}
	}

	currentSize := m.errorQueue.Count()

	return ErrorQueueStats{
		CurrentSize: currentSize,
	}
}

// ResetMetrics resets the write performance metrics
func (m *Manager) ResetMetrics() {
	if m.serialiser != nil && m.serialiser.metrics != nil {
		slog.Info("Resetting write metrics", "db", m.Database())
		m.serialiser.metrics.Reset()
	}
}

// ResetCaches resets the statement caches
func (m *Manager) ResetCaches() {
	if m.readStmtCache != nil && m.serialiser != nil && m.serialiser.metrics != nil {
		slog.Info("Resetting reader cache", "db", m.Database())
		m.readStmtCache.Close()
		m.readStmtCache = NewStmtCache(
			&m.serialiser.metrics.ReaderStmtMetrics,
			m.options,
		)
	}

	if m.writeStmtCache != nil && m.serialiser != nil && m.serialiser.metrics != nil {
		slog.Info("Resetting writer cache", "db", m.Database())
		m.writeStmtCache.Close()
		m.writeStmtCache = NewStmtCache(
			&m.serialiser.metrics.WriterStmtMetrics,
			m.options,
		)
	}
}

// ResetCacheDetailedMetrics resets only the detailed cache metrics
func (m *Manager) ResetCacheDetailedMetrics() {
	if m.readStmtCache != nil && m.serialiser != nil && m.serialiser.metrics != nil {
		slog.Info("Resetting reader cache detailed metrics", "db", m.Database())
		m.serialiser.metrics.ReaderStmtMetrics.mu.Lock()
		m.serialiser.metrics.ReaderStmtMetrics.hitsByQuery = make(map[string]int64)
		m.serialiser.metrics.ReaderStmtMetrics.slowQueries = m.serialiser.metrics.ReaderStmtMetrics.slowQueries[:0]
		m.serialiser.metrics.ReaderStmtMetrics.lastReset = time.Now()
		m.serialiser.metrics.ReaderStmtMetrics.mu.Unlock()
	}

	if m.writeStmtCache != nil && m.serialiser != nil && m.serialiser.metrics != nil {
		slog.Info("Resetting writer cache detailed metrics", "db", m.Database())
		m.serialiser.metrics.WriterStmtMetrics.mu.Lock()
		m.serialiser.metrics.WriterStmtMetrics.hitsByQuery = make(map[string]int64)
		m.serialiser.metrics.WriterStmtMetrics.slowQueries = m.serialiser.metrics.WriterStmtMetrics.slowQueries[:0]
		m.serialiser.metrics.WriterStmtMetrics.lastReset = time.Now()
		m.serialiser.metrics.WriterStmtMetrics.mu.Unlock()
	}
}

// WaitForIdle waits until all operations are processed
func (m *Manager) WaitForIdle(ctx context.Context) error {
	slog.Info("Waiting for all operations to complete", "db", m.Database())

	// Start with shorter intervals, back off as we wait longer
	intervals := []time.Duration{
		50 * time.Millisecond,  // First few checks
		100 * time.Millisecond, // Short-term waiting
		250 * time.Millisecond, // Medium-term waiting
		500 * time.Millisecond, // Long-term waiting
	}

	intervalIndex := 0
	checkCount := 0

	for {
		select {
		case <-ctx.Done():
			slog.Warn("Wait for idle cancelled by context", "error", ctx.Err(), "db", m.Database())
			return ctx.Err()
		default:
			// Check if queue is empty and batch collector is idle
			metrics := m.GetMetrics()
			batchSize := 0
			if m.batcher != nil {
				m.batcher.mutex.Lock()
				batchSize = len(m.batcher.queries)
				m.batcher.mutex.Unlock()
			}

			if metrics.CurrentQueueLen == 0 && batchSize == 0 {
				slog.Info("All operations completed", "db", m.Database())
				return nil
			}

			// Progressive backoff
			currentInterval := intervals[intervalIndex]
			if checkCount > 10 && intervalIndex < len(intervals)-1 {
				intervalIndex++
				checkCount = 0
			}

			time.Sleep(currentInterval)
			checkCount++
		}
	}
}

// GetReaderProfile returns the current reader profile
func (m *Manager) GetReaderProfile() *profile.Profile {
	return m.readerProfile
}

// GetWriterProfile returns the current writer profile
func (m *Manager) GetWriterProfile() *profile.Profile {
	return m.writerProfile
}

// GetErrors returns all errors in the error queue
func (m *Manager) GetErrors() []JobError {
	if m.errorQueue == nil {
		return nil
	}
	return m.errorQueue.GetAll()
}

// GetErrorByID retrieves a specific error from the queue
func (m *Manager) GetErrorByID(jobID int64) (JobError, bool) {
	if m.errorQueue == nil {
		return JobError{}, false
	}
	return m.errorQueue.Get(jobID)
}

// RemoveError removes a specific error from the queue
func (m *Manager) RemoveError(jobID int64) bool {
	if m.errorQueue == nil {
		return false
	}
	return m.errorQueue.Remove(jobID)
}

// ClearErrors removes all errors from the queue
func (m *Manager) ClearErrors() {
	if m.errorQueue != nil {
		m.errorQueue.Clear()
	}
}

// RetryJob manually retries a failed job by its ID
func (m *Manager) RetryJob(jobID int64) error {
	if m.errorQueue == nil {
		return ErrErrorQueueDisabled
	}

	jobError, found := m.errorQueue.Get(jobID)
	if !found {
		return ErrJobNotFound
	}

	retryQuery := jobError.CreateRetryQuery()
	retryQuery.async = true

	ctx, cancel := context.WithTimeout(context.Background(), m.options.RetrySubmitTimeout)
	defer cancel()

	_, err := m.serialiser.SubmitNoWait(ctx, NewQueryJob(retryQuery))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRetrySubmissionFailed, err)
	}

	// Remove from error queue since we successfully resubmitted
	m.errorQueue.Remove(jobID)
	return nil
}

// RunVacuum performs a VACUUM operation (full database rebuild)
func (m *Manager) RunVacuum() error {
	if m.writer == nil {
		return ErrWriterDisabled
	}
	slog.Info("Running VACUUM", "db", m.Database())
	_, err := m.writer.Exec("VACUUM")
	return err
}

// RunIncrementalVacuum performs incremental vacuum if enabled
func (m *Manager) RunIncrementalVacuum(pages int) error {
	if m.writer == nil {
		return ErrWriterDisabled
	}
	if pages < 0 {
		return fmt.Errorf("invalid pages value: must be non-negative, got %d", pages)
	}
	slog.Info("Running incremental VACUUM", "pages", pages, "db", m.Database())
	_, err := m.writer.Exec("PRAGMA incremental_vacuum(?)", pages)
	return err
}

// RunWalCheckpoint triggers a WAL checkpoint
func (m *Manager) RunWalCheckpoint(mode string) error {
	if m.writer == nil {
		return ErrWriterDisabled
	}
	validModes := map[string]bool{
		"PASSIVE":  true,
		"FULL":     true,
		"RESTART":  true,
		"TRUNCATE": true,
	}
	if !validModes[mode] {
		return fmt.Errorf("invalid checkpoint mode: %q (valid: PASSIVE, FULL, RESTART, TRUNCATE)", mode)
	}
	slog.Info("Running WAL checkpoint", "mode", mode, "db", m.Database())
	_, err := m.writer.Exec(fmt.Sprintf("PRAGMA wal_checkpoint(%s)", mode))
	return err
}

// SetSecureDelete enables or disables secure_delete
func (m *Manager) SetSecureDelete(enabled bool) error {
	if m.writer == nil {
		return ErrWriterDisabled
	}
	val := "OFF"
	if enabled {
		val = "ON"
	}
	slog.Info("Setting secure_delete", "enabled", enabled, "db", m.Database())
	_, err := m.writer.Exec(fmt.Sprintf("PRAGMA secure_delete = %s", val))
	return err
}

// GetJobStatus checks if a job failed by looking in the error queue
func (m *Manager) GetJobStatus(jobID int64) (string, error) {
	if m.errorQueue == nil {
		return "", ErrErrorQueueDisabled
	}

	if _, found := m.errorQueue.Get(jobID); found {
		return "failed", nil
	}

	return "unknown", nil
}
