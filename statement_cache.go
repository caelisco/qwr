package qwr

import (
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// SlowQuery represents a slow statement preparation for metrics tracking
type SlowQuery struct {
	Query    string        // The SQL query that was slow to prepare
	Duration time.Duration // How long the preparation took
	When     time.Time     // When the slow preparation occurred
}

// StmtCache is a high-performance prepared statement cache using sync.Map
// for optimal concurrent access. Stores SQL prepared statements keyed by
// their query string.
type StmtCache struct {
	statements sync.Map          // map[string]*cacheItem
	metrics    *StmtCacheMetrics // Metrics collection
	options    Options           // Configuration options
	closed     atomic.Bool       // Flag to prevent use after close
}

// cacheItem holds a prepared statement with metadata
type cacheItem struct {
	stmt     *sql.Stmt
	lastUsed time.Time
}

// NewStmtCache creates a new prepared statement cache
func NewStmtCache(metrics *StmtCacheMetrics, options Options) *StmtCache {
	cache := &StmtCache{
		metrics: metrics,
		options: options,
	}

	if metrics != nil {
		metrics.MaxSize = -1 // Unlimited
	}

	return cache
}

// Get retrieves a prepared statement from cache or prepares it if not found
func (c *StmtCache) Get(db *sql.DB, query string) (*sql.Stmt, error) {
	// Check if cache is closed
	if c.closed.Load() {
		return nil, ErrCacheClosed
	}

	// Fast path: check if exists
	if value, exists := c.statements.Load(query); exists {
		item, ok := value.(*cacheItem)
		if !ok {
			// Cache corruption - treat as cache miss and clean up
			c.statements.Delete(query)
			if c.metrics != nil {
				c.metrics.recordCacheMiss()
			}
		} else {
			if c.metrics != nil {
				c.metrics.recordCacheHit()
			}
			item.lastUsed = time.Now()
			return item.stmt, nil
		}
	}

	// Cache miss - prepare the statement
	if c.metrics != nil {
		c.metrics.recordCacheMiss()
	}

	start := time.Now()
	stmt, err := db.Prepare(query)
	prepDuration := time.Since(start)

	if err != nil {
		if c.metrics != nil {
			c.metrics.recordPrepError()
		}
		return nil, err
	}

	if c.metrics != nil {
		c.metrics.recordPrepTime(prepDuration)

		if prepDuration > c.options.StmtCacheSlowThreshold && c.shouldSample() {
			c.recordSlowQuery(query, prepDuration)
		}
	}

	// Create cache item
	item := &cacheItem{
		stmt:     stmt,
		lastUsed: time.Now(),
	}

	// Check if another goroutine added it while we were preparing
	if existing, loaded := c.statements.LoadOrStore(query, item); loaded {
		stmt.Close() // Close our statement, use existing one
		existingItem, ok := existing.(*cacheItem)
		if !ok {
			// Cache corruption - return error
			return nil, errors.New("qwr: statement cache corruption detected")
		}
		return existingItem.stmt, nil
	}

	// We stored our item successfully
	if c.metrics != nil {
		c.metrics.Size.Add(1)
	}
	return stmt, nil
}

// Close gracefully shuts down the cache
func (c *StmtCache) Close() {
	// Set closed flag first to prevent new Get() calls from succeeding
	c.closed.Store(true)

	c.statements.Range(func(key, value interface{}) bool {
		item, ok := value.(*cacheItem)
		if ok {
			item.stmt.Close()
		}
		return true
	})
	c.statements = sync.Map{}
	if c.metrics != nil {
		c.metrics.Size.Store(0)
	}
}

// shouldSample determines if this operation should be sampled for detailed metrics
func (c *StmtCache) shouldSample() bool {
	if c.metrics == nil {
		return false
	}
	return c.metrics.Hits.Load()%c.options.StmtCacheSampleRate == 0
}

// recordSlowQuery records a slow statement preparation
func (c *StmtCache) recordSlowQuery(query string, duration time.Duration) {
	if c.metrics != nil {
		c.metrics.recordSlowQuery(query, duration)
	}
}
