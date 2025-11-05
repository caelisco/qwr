package qwr

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// BatchCollector manages automatic batching of database jobs for asynchronous execution.
type BatchCollector struct {
	queries []Job          // Current batch of jobs awaiting execution
	timer   *time.Timer      // Timer for timeout-based batch flushing
	mutex   sync.Mutex       // Protects batch state during concurrent access
	ws      *WriteSerialiser // Worker pool for async job execution
	options Options          // Configuration options for batch behavior
	dbPath  string           // Database path for logging context
	ctx     context.Context  // Context for all batch operations (never nil)
}

// NewBatchCollector creates a new batch collector with pre-allocated capacity and context.
func NewBatchCollector(ctx context.Context, ws *WriteSerialiser, options Options, dbPath string) *BatchCollector {
	if ctx == nil {
		panic("BatchCollector requires a non-nil context")
	}

	return &BatchCollector{
		queries: make([]Job, 0, options.BatchSize),
		ws:      ws,
		options: options,
		dbPath:  dbPath,
		ctx:     ctx,
	}
}

// dbName extracts the database filename for logging context
func (bc *BatchCollector) dbName() string {
	if bc.dbPath == "" {
		return ""
	}
	return filepath.Base(bc.dbPath)
}

// Add adds a job to the current batch for eventual execution using the collector's context
func (bc *BatchCollector) Add(job Job) {
	// Mark query as async if it's a Query type
	if job.Type == JobTypeQuery {
		job.Query.async = true
	}

	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	// Start timer if this is the first item
	if len(bc.queries) == 0 {
		bc.startBatchTimer()
	}

	// Add to batch
	bc.queries = append(bc.queries, job)
	currentSize := len(bc.queries)

	slog.Debug("Added query to batch", "current_size", currentSize, "max_size", bc.options.BatchSize, "db", bc.dbName())

	// Check if we need to flush due to size
	if currentSize >= bc.options.BatchSize {
		bc.flushBatch("size_limit")
	}
}

// startBatchTimer starts the timeout timer for batch flushing
func (bc *BatchCollector) startBatchTimer() {
	slog.Debug("Starting batch timer", "timeout", bc.options.BatchTimeout, "db", bc.dbName())

	bc.timer = time.AfterFunc(bc.options.BatchTimeout, func() {
		bc.mutex.Lock()
		defer bc.mutex.Unlock()

		if len(bc.queries) > 0 {
			bc.flushBatch("timeout")
		}
		bc.timer = nil
	})
}

// flushBatch processes and submits the current batch (must be called with mutex held)
func (bc *BatchCollector) flushBatch(reason string) {
	if len(bc.queries) == 0 {
		return
	}

	batchSize := len(bc.queries)
	batchJob := BatchJob{
		Queries: make([]Job, len(bc.queries)),
		id:      time.Now().UnixNano(),
	}
	copy(batchJob.Queries, bc.queries)

	slog.Info("Flushing batch", "reason", reason, "size", batchSize, "batch_id", batchJob.id, "db", bc.dbName())

	// Process batch (inline inserts if enabled)
	if bc.options.InlineInserts && batchSize > 1 {
		if combined, ok := bc.inlineInserts(batchJob.Queries); ok {
			slog.Info("Combined queries into batches",
				"original_count", len(batchJob.Queries),
				"combined_count", len(combined),
				"batch_id", batchJob.id,
				"db", bc.dbName())
			batchJob.Queries = combined
		}
	}

	// Reset batch state
	bc.queries = bc.queries[:0]
	if bc.timer != nil {
		bc.timer.Stop()
		bc.timer = nil
	}

	// Submit to worker queue
	bc.submitBatch(batchJob)
}

// submitBatch sends the batch to the worker queue
func (bc *BatchCollector) submitBatch(batchJob BatchJob) {
	if !bc.ws.workerRunning.Load() {
		slog.Error("Worker not running, cannot submit batch", "batch_id", batchJob.id, "db", bc.dbName())
		return
	}

	item := workItem{
		job:      Job{Type: JobTypeBatch, BatchJob: batchJob},
		ctx:      bc.ctx,
		queuedAt: time.Now(),
	}

	select {
	case bc.ws.queue <- item:
		if bc.ws.metrics != nil {
			bc.ws.metrics.recordJobQueued()
		}
		slog.Debug("Submitted batch job", "query_count", len(batchJob.Queries), "batch_id", batchJob.id, "db", bc.dbName())
	default:
		slog.Error("Failed to submit batch - queue full", "batch_id", batchJob.id, "db", bc.dbName())
	}
}

// Close flushes any pending batch and stops the timer
func (bc *BatchCollector) Close() {
	bc.mutex.Lock()
	defer bc.mutex.Unlock()

	slog.Info("Closing batch collector", "db", bc.dbName())

	if bc.timer != nil {
		bc.timer.Stop()
		bc.timer = nil
	}

	if len(bc.queries) > 0 {
		batchJob := BatchJob{
			Queries: make([]Job, len(bc.queries)),
			id:      time.Now().UnixNano(),
		}
		copy(batchJob.Queries, bc.queries)

		slog.Info("Flushing final batch before closing", "size", len(bc.queries), "batch_id", batchJob.id, "db", bc.dbName())

		// Process final batch
		if bc.options.InlineInserts && len(bc.queries) > 1 {
			if combined, ok := bc.inlineInserts(batchJob.Queries); ok {
				slog.Info("Combined queries into batches",
					"original_count", len(batchJob.Queries),
					"combined_count", len(combined),
					"batch_id", batchJob.id,
					"db", bc.dbName())
				batchJob.Queries = combined
			}
		}

		bc.queries = bc.queries[:0]

		// Synchronous submission for close
		if bc.ws.workerRunning.Load() {
			item := workItem{
				job:        Job{Type: JobTypeBatch, BatchJob: batchJob},
				ctx:        bc.ctx,
				queuedAt:   time.Now(),
				resultChan: make(chan JobResult, 1),
			}

			select {
			case bc.ws.queue <- item:
				if bc.ws.metrics != nil {
					bc.ws.metrics.recordJobQueued()
				}
				<-item.resultChan // Wait for completion during close
			default:
				slog.Error("Failed to submit final batch - queue full", "batch_id", batchJob.id, "db", bc.dbName())
			}
		}
	}
}

// inlineInserts attempts to combine multiple INSERT statements
func (bc *BatchCollector) inlineInserts(items []Job) ([]Job, bool) {
	if len(items) < 2 {
		return items, false
	}

	// Group queries by SQL to combine similar INSERTs
	queryGroups := make(map[string][]Query)
	nonQueryJobs := make([]Job, 0)
	var sqlKeys []string

	// Separate queries from other jobs and group by SQL
	for _, item := range items {
		if item.Type == JobTypeQuery {
			sql := item.Query.SQL
			if _, exists := queryGroups[sql]; !exists {
				sqlKeys = append(sqlKeys, sql)
			}
			queryGroups[sql] = append(queryGroups[sql], item.Query)
		} else {
			nonQueryJobs = append(nonQueryJobs, item)
		}
	}

	if len(queryGroups) == 0 {
		return items, false
	}

	combinedJobs := make([]Job, 0, len(sqlKeys)+len(nonQueryJobs))
	anyCombined := false

	// Process each SQL group
	for _, sql := range sqlKeys {
		group := queryGroups[sql]
		if len(group) == 1 {
			combinedJobs = append(combinedJobs, Job{Type: JobTypeQuery, Query: group[0]})
		} else {
			// Try to combine this group
			if combinedSQL, combinedArgs, ok := bc.combineInsertGroup(group); ok {
				combinedQuery := Query{
					SQL:   combinedSQL,
					Args:  combinedArgs,
					id:    time.Now().UnixNano(),
					async: true,
				}
				combinedJobs = append(combinedJobs, Job{Type: JobTypeQuery, Query: combinedQuery})
				anyCombined = true
			} else {
				for _, q := range group {
					combinedJobs = append(combinedJobs, Job{Type: JobTypeQuery, Query: q})
				}
			}
		}
	}

	combinedJobs = append(combinedJobs, nonQueryJobs...)
	return combinedJobs, anyCombined
}

// combineInsertGroup attempts to combine a group of queries with identical SQL
func (bc *BatchCollector) combineInsertGroup(queries []Query) (string, []any, bool) {
	if len(queries) < 2 {
		return "", nil, false
	}

	firstQuery := queries[0]

	// Check if it's an INSERT
	isInsert := strings.HasPrefix(firstQuery.SQL, "INSERT") || strings.HasPrefix(firstQuery.SQL, "insert")
	if !isInsert {
		return "", nil, false
	}

	// Find the last opening parenthesis
	lastOpenParen := strings.LastIndex(firstQuery.SQL, "(")
	if lastOpenParen == -1 {
		return "", nil, false
	}

	placeholderPattern := firstQuery.SQL[lastOpenParen:]

	// Build the combined query
	var b strings.Builder
	b.WriteString(firstQuery.SQL)

	repeatingPattern := ", " + placeholderPattern
	for i := 1; i < len(queries); i++ {
		b.WriteString(repeatingPattern)
	}

	// Flatten all arguments
	argCount := len(firstQuery.Args)
	args := make([]any, 0, len(queries)*argCount)
	for _, query := range queries {
		args = append(args, query.Args...)
	}

	return b.String(), args, true
}
