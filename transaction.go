package qwr

import (
	"context"
	"database/sql"
	"time"
)

// Transaction represents multiple SQL statements to execute in a transaction
type Transaction struct {
	queries []Query
	id      int64
	ctx     context.Context
	manager *Manager
}

// Transaction creates a new transaction
func (m *Manager) Transaction(capacity ...int) *Transaction {
	cap := 2 // default capacity
	if len(capacity) > 0 && capacity[0] > 0 {
		cap = capacity[0]
	}

	return &Transaction{
		queries: make([]Query, 0, cap),
		id:      time.Now().UnixNano(),
		ctx:     nil,
		manager: m,
	}
}

// WithContext adds context to the transaction
func (t *Transaction) WithContext(ctx context.Context) *Transaction {
	t.ctx = ctx
	return t
}

// Add adds a query to the transaction
func (t *Transaction) Add(sql string, args ...any) *Transaction {
	query := Query{
		SQL:      sql,
		Args:     append(make([]any, 0, len(args)), args...),
		id:       time.Now().UnixNano(),
		prepared: false,
	}
	t.queries = append(t.queries, query)
	return t
}

// AddPrepared adds a prepared query to the transaction
func (t *Transaction) AddPrepared(sql string, args ...any) *Transaction {
	query := Query{
		SQL:      sql,
		Args:     append(make([]any, 0, len(args)), args...),
		id:       time.Now().UnixNano(),
		prepared: true,
	}
	t.queries = append(t.queries, query)
	return t
}

// Write executes the transaction directly
func (t *Transaction) Write() (*TransactionResult, error) {
	if t.manager.writer == nil {
		return nil, ErrWriterDisabled
	}

	start := time.Now()
	result := t.ExecuteWithContext(t.ctx, t.manager.writer)

	if result.Type != ResultTypeTransaction {
		return nil, ErrInvalidResult
	}

	execTime := time.Since(start)
	failed := result.TransactionResult.err != nil

	// Record metrics as direct write since it bypasses worker pool
	if t.manager.serialiser != nil && t.manager.serialiser.metrics != nil && t.manager.options.EnableMetrics {
		t.manager.serialiser.metrics.recordDirectWrite(execTime, failed)
	}

	return &result.TransactionResult, result.TransactionResult.err
}

// Exec runs the transaction through the worker pool
func (t *Transaction) Exec() (*TransactionResult, error) {
	ctx := t.ctx
	if ctx == nil {
		if t.manager.ctx != nil {
			ctx = t.manager.ctx
		} else {
			ctx = nil
		}
	}

	var result JobResult
	var err error

	if ctx != nil {
		result, err = t.manager.serialiser.SubmitWait(ctx, NewTransactionJob(*t))
	} else {
		result, err = t.manager.serialiser.SubmitWaitNoContext(NewTransactionJob(*t))
	}

	if err != nil {
		return nil, err
	}

	if result.Type != ResultTypeTransaction {
		return nil, ErrInvalidResult
	}
	return &result.TransactionResult, result.TransactionResult.err
}

// ID returns the transaction ID
func (t *Transaction) ID() int64 {
	return t.id
}

// ExecuteWithContext implements JobID interface
func (t *Transaction) ExecuteWithContext(ctx context.Context, db *sql.DB) JobResult {
	if ctx != nil {
		return t.executeWithContext(ctx, db)
	} else {
		return t.execute(db)
	}
}

// execute handles transaction execution without context
func (t *Transaction) execute(db *sql.DB) JobResult {
	start := time.Now()
	result := &TransactionResult{
		id:      t.id,
		Results: make([]*QueryResult, len(t.queries)),
	}

	if len(t.queries) == 0 {
		result.err = ErrInvalidQuery
		result.duration = time.Since(start)
		return NewTransactionResult(*result)
	}

	// Pre-fetch prepared statements from cache before starting transaction
	preparedStmts := make(map[string]*sql.Stmt)
	for _, query := range t.queries {
		if query.prepared && t.manager.writeStmtCache != nil {
			if _, exists := preparedStmts[query.SQL]; !exists {
				if stmt, err := t.manager.writeStmtCache.Get(db, query.SQL); err == nil {
					preparedStmts[query.SQL] = stmt
				}
			}
		}
	}

	tx, err := db.Begin()
	if err != nil {
		result.err = err
		result.duration = time.Since(start)
		return NewTransactionResult(*result)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	// Execute each query in the transaction
	for i, query := range t.queries {
		queryStart := time.Now()
		queryResult := &QueryResult{id: query.ID()}

		var sqlResult sql.Result

		if query.prepared {
			if stmt, exists := preparedStmts[query.SQL]; exists {
				txStmt := tx.Stmt(stmt)
				sqlResult, err = txStmt.Exec(query.Args...)
				txStmt.Close()
			} else {
				sqlResult, err = tx.Exec(query.SQL, query.Args...)
			}
		} else {
			sqlResult, err = tx.Exec(query.SQL, query.Args...)
		}

		if err != nil {
			tx.Rollback()
			queryResult.err = err
			result.Results[i] = queryResult
			result.err = err
			result.duration = time.Since(start)
			return NewTransactionResult(*result)
		}

		queryResult.SQLResult = sqlResult
		queryResult.duration = time.Since(queryStart)
		result.Results[i] = queryResult
	}

	if err := tx.Commit(); err != nil {
		result.err = err
	}

	result.duration = time.Since(start)
	return NewTransactionResult(*result)
}

// executeWithContext handles transaction execution with context
func (t *Transaction) executeWithContext(ctx context.Context, db *sql.DB) JobResult {
	start := time.Now()
	result := &TransactionResult{
		id:      t.id,
		Results: make([]*QueryResult, len(t.queries)),
	}

	if len(t.queries) == 0 {
		result.err = ErrInvalidQuery
		result.duration = time.Since(start)
		return NewTransactionResult(*result)
	}

	// Create transaction context with timeout
	txCtx, cancel := context.WithTimeout(ctx, t.manager.options.TransactionTimeout)
	defer cancel()

	// Pre-fetch prepared statements from cache before starting transaction
	preparedStmts := make(map[string]*sql.Stmt)
	for _, query := range t.queries {
		if query.prepared && t.manager.writeStmtCache != nil {
			if _, exists := preparedStmts[query.SQL]; !exists {
				if stmt, err := t.manager.writeStmtCache.Get(db, query.SQL); err == nil {
					preparedStmts[query.SQL] = stmt
				}
			}
		}
	}

	tx, err := db.BeginTx(txCtx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
	})

	if err != nil {
		result.err = err
		result.duration = time.Since(start)
		return NewTransactionResult(*result)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	// Execute each query in the transaction
	for i, query := range t.queries {
		queryStart := time.Now()
		queryResult := &QueryResult{id: query.ID()}

		var sqlResult sql.Result

		if query.prepared {
			if stmt, exists := preparedStmts[query.SQL]; exists {
				txStmt := tx.StmtContext(txCtx, stmt)
				sqlResult, err = txStmt.ExecContext(txCtx, query.Args...)
				txStmt.Close()
			} else {
				sqlResult, err = tx.ExecContext(txCtx, query.SQL, query.Args...)
			}
		} else {
			sqlResult, err = tx.ExecContext(txCtx, query.SQL, query.Args...)
		}

		if err != nil {
			tx.Rollback()
			queryResult.err = err
			result.Results[i] = queryResult
			result.err = err
			result.duration = time.Since(start)
			return NewTransactionResult(*result)
		}

		queryResult.SQLResult = sqlResult
		queryResult.duration = time.Since(queryStart)
		result.Results[i] = queryResult
	}

	if err := tx.Commit(); err != nil {
		result.err = err
	}

	result.duration = time.Since(start)
	return NewTransactionResult(*result)
}
