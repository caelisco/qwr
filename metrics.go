package qwr

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracks performance of the qwr system
type Metrics struct {
	// Core counters (atomics for lock-free updates from worker)
	jobsProcessed  atomic.Int64
	jobsFailed     atomic.Int64
	totalQueueTime atomic.Int64 // nanoseconds
	totalExecTime  atomic.Int64 // nanoseconds

	// Job type counters
	queriesProcessed      atomic.Int64
	transactionsProcessed atomic.Int64
	batchesProcessed      atomic.Int64

	// Direct write counters (for QueryWrite() bypassing worker pool)
	directWritesProcessed atomic.Int64
	directWritesFailed    atomic.Int64
	totalDirectWriteTime  atomic.Int64 // nanoseconds

	// Error queue counters
	totalErrors   atomic.Int64
	retriedErrors atomic.Int64
	droppedErrors atomic.Int64

	// Current state
	currentQueueLen atomic.Int32
	startTime       time.Time

	// Enhanced cache metrics with detailed tracking
	ReaderStmtMetrics StmtCacheMetrics
	WriterStmtMetrics StmtCacheMetrics

	// RWMutex for safe concurrent reads of snapshot data
	mu sync.RWMutex
}

// MetricsSnapshot provides a point-in-time view
type MetricsSnapshot struct {
	JobsProcessed         int64
	JobsFailed            int64
	QueriesProcessed      int64
	TransactionsProcessed int64
	BatchesProcessed      int64
	DirectWritesProcessed int64
	DirectWritesFailed    int64
	CurrentQueueLen       int32
	ProcessingRate        float64 // jobs/second
	ErrorRate             float64 // percentage
	AvgQueueWaitTime      time.Duration
	AvgProcessingTime     time.Duration
	AvgDirectWriteTime    time.Duration
	Uptime                time.Duration

	// Error queue stats
	TotalErrors   int64
	RetriedErrors int64
	DroppedErrors int64

	// Cache stats (non-atomic snapshots)
	ReaderStmtStats StmtCacheStats
	WriterStmtStats StmtCacheStats
}

// StmtCacheMetrics holds enhanced atomic counters for thread-safe cache metrics
type StmtCacheMetrics struct {
	Size       atomic.Int32 // Current cache size
	MaxSize    int64        // Maximum cache size
	Hits       atomic.Int64 // Cache hits
	Misses     atomic.Int64 // Cache misses
	Evictions  atomic.Int64 // Number of evictions
	Collisions atomic.Int64 // Hash collisions (legacy, may not be used)

	// Enhanced metrics
	totalPrepTime atomic.Int64 // nanoseconds
	prepCount     atomic.Int64 // for averaging
	prepErrors    atomic.Int64

	// Detailed metrics (with mutex protection)
	mu          sync.RWMutex
	hitsByQuery map[string]int64
	slowQueries []SlowQuery
	lastReset   time.Time
}

// StmtCacheStats provides a snapshot of cache statistics (non-atomic)
type StmtCacheStats struct {
	Size       int
	MaxSize    int64
	Hits       int64
	Misses     int64
	Evictions  int64
	Collisions int64
	HitRatio   float64

	// Enhanced stats
	AvgPrepTimeMs     float64
	PrepErrors        int64
	SlowQueriesCount  int
	UptimeSeconds     float64
	TopQueries        map[string]int64
	RecentSlowQueries []SlowQuery
}

// ErrorQueueStats provides basic error queue metrics
type ErrorQueueStats struct {
	CurrentSize    int
	TotalErrors    int64
	RetriedErrors  int64
	DroppedErrors  int64
	PendingRetries int
}

func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

// NewStmtCacheMetrics creates initialized statement cache metrics
func NewStmtCacheMetrics() *StmtCacheMetrics {
	return &StmtCacheMetrics{
		hitsByQuery: make(map[string]int64),
		lastReset:   time.Now(),
	}
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	processed := m.jobsProcessed.Load()
	failed := m.jobsFailed.Load()
	directWrites := m.directWritesProcessed.Load()
	directFailed := m.directWritesFailed.Load()
	elapsed := time.Since(m.startTime).Seconds()

	var processingRate, errorRate float64
	var avgQueueWait, avgProcessing, avgDirectWrite time.Duration

	// Calculate averages for worker pool operations
	if processed > 0 {
		if elapsed > 0 {
			processingRate = float64(processed) / elapsed
		}
		errorRate = float64(failed) / float64(processed)
		avgQueueWait = time.Duration(m.totalQueueTime.Load() / processed)
		avgProcessing = time.Duration(m.totalExecTime.Load() / processed)
	}

	// Calculate average for direct writes
	if directWrites > 0 {
		avgDirectWrite = time.Duration(m.totalDirectWriteTime.Load() / directWrites)
	}

	return MetricsSnapshot{
		JobsProcessed:         processed,
		JobsFailed:            failed,
		QueriesProcessed:      m.queriesProcessed.Load(),
		TransactionsProcessed: m.transactionsProcessed.Load(),
		BatchesProcessed:      m.batchesProcessed.Load(),
		DirectWritesProcessed: directWrites,
		DirectWritesFailed:    directFailed,
		CurrentQueueLen:       m.currentQueueLen.Load(),
		ProcessingRate:        processingRate,
		ErrorRate:             errorRate,
		AvgQueueWaitTime:      avgQueueWait,
		AvgProcessingTime:     avgProcessing,
		AvgDirectWriteTime:    avgDirectWrite,
		Uptime:                time.Since(m.startTime),
		TotalErrors:           m.totalErrors.Load(),
		RetriedErrors:         m.retriedErrors.Load(),
		DroppedErrors:         m.droppedErrors.Load(),
		ReaderStmtStats:       m.ReaderStmtMetrics.Stats(),
		WriterStmtStats:       m.WriterStmtMetrics.Stats(),
	}
}

func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobsProcessed.Store(0)
	m.jobsFailed.Store(0)
	m.totalQueueTime.Store(0)
	m.totalExecTime.Store(0)
	m.queriesProcessed.Store(0)
	m.transactionsProcessed.Store(0)
	m.batchesProcessed.Store(0)
	m.directWritesProcessed.Store(0)
	m.directWritesFailed.Store(0)
	m.totalDirectWriteTime.Store(0)
	m.totalErrors.Store(0)
	m.retriedErrors.Store(0)
	m.droppedErrors.Store(0)

	// Reset cache metrics
	m.ReaderStmtMetrics.Size.Store(0)
	m.ReaderStmtMetrics.Hits.Store(0)
	m.ReaderStmtMetrics.Misses.Store(0)
	m.ReaderStmtMetrics.Evictions.Store(0)
	m.ReaderStmtMetrics.Collisions.Store(0)

	m.WriterStmtMetrics.Size.Store(0)
	m.WriterStmtMetrics.Hits.Store(0)
	m.WriterStmtMetrics.Misses.Store(0)
	m.WriterStmtMetrics.Evictions.Store(0)
	m.WriterStmtMetrics.Collisions.Store(0)

	m.startTime = time.Now()
}

// recordJobQueued records when a job is added to the queue
func (m *Metrics) recordJobQueued() {
	m.currentQueueLen.Add(1)
}

// recordJobExecution handles all the metrics updates for a completed job in one call
// This batches multiple atomic operations that were previously separate
func (m *Metrics) recordJobExecution(queueWaitTime, execTime time.Duration, failed bool, jobType string) {
	// Update core metrics
	m.jobsProcessed.Add(1)
	m.totalQueueTime.Add(int64(queueWaitTime))
	m.totalExecTime.Add(int64(execTime))
	m.currentQueueLen.Add(-1)

	// Track job types
	switch jobType {
	case "query":
		m.queriesProcessed.Add(1)
	case "transaction":
		m.transactionsProcessed.Add(1)
	case "batch":
		m.batchesProcessed.Add(1)
	}

	if failed {
		m.jobsFailed.Add(1)
	}
}

// recordDirectWrite tracks direct write operations (QueryWrite() method)
func (m *Metrics) recordDirectWrite(execTime time.Duration, failed bool) {
	m.directWritesProcessed.Add(1)
	m.totalDirectWriteTime.Add(int64(execTime))

	if failed {
		m.directWritesFailed.Add(1)
	}
}

// Error queue methods
func (m *Metrics) recordErrorAdded() {
	m.totalErrors.Add(1)
}

func (m *Metrics) recordErrorRetried() {
	m.retriedErrors.Add(1)
}

func (m *Metrics) recordErrorDropped() {
	m.droppedErrors.Add(1)
}

// StmtCacheMetrics methods for thread-safe updates
func (s *StmtCacheMetrics) recordCacheHit() {
	s.Hits.Add(1)
}

func (s *StmtCacheMetrics) recordCacheMiss() {
	s.Misses.Add(1)
}

func (s *StmtCacheMetrics) recordPrepTime(duration time.Duration) {
	s.totalPrepTime.Add(int64(duration))
	s.prepCount.Add(1)
}

func (s *StmtCacheMetrics) recordPrepError() {
	s.prepErrors.Add(1)
}

func (s *StmtCacheMetrics) recordSlowQuery(query string, duration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Keep only last 100 slow queries to prevent memory growth
	if len(s.slowQueries) >= 100 {
		s.slowQueries = s.slowQueries[1:]
	}

	s.slowQueries = append(s.slowQueries, SlowQuery{
		Query:    query,
		Duration: duration,
		When:     time.Now(),
	})
}

// Stats method converts atomic metrics to snapshot stats
func (s *StmtCacheMetrics) Stats() StmtCacheStats {
	hits := s.Hits.Load()
	misses := s.Misses.Load()
	evictions := s.Evictions.Load()
	collisions := s.Collisions.Load()
	size := s.Size.Load()

	var hitRatio float64
	total := hits + misses
	if total > 0 {
		hitRatio = float64(hits) / float64(total)
	}

	var avgPrepTime float64
	if prepCount := s.prepCount.Load(); prepCount > 0 {
		totalTime := s.totalPrepTime.Load()
		avgPrepTime = float64(totalTime/prepCount) / 1e6 // Convert to milliseconds
	}

	prepErrors := s.prepErrors.Load()

	s.mu.RLock()
	slowQueriesCount := len(s.slowQueries)
	uptime := time.Since(s.lastReset).Seconds()

	// Copy top queries map
	topQueries := make(map[string]int64)
	for query, hits := range s.hitsByQuery {
		topQueries[query] = hits
	}

	// Copy recent slow queries
	recentSlowQueries := make([]SlowQuery, len(s.slowQueries))
	copy(recentSlowQueries, s.slowQueries)
	s.mu.RUnlock()

	return StmtCacheStats{
		Size:              int(size),
		MaxSize:           s.MaxSize,
		Hits:              hits,
		Misses:            misses,
		Evictions:         evictions,
		Collisions:        collisions,
		HitRatio:          hitRatio,
		AvgPrepTimeMs:     avgPrepTime,
		PrepErrors:        prepErrors,
		SlowQueriesCount:  slowQueriesCount,
		UptimeSeconds:     uptime,
		TopQueries:        topQueries,
		RecentSlowQueries: recentSlowQueries,
	}
}
