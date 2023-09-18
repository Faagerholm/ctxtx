package ctxtx

//go:generate mockgen -source=$GOFILE -destination=mock/mock_$GOFILE

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// CtxTransactionProvider defines functions for interacting with ctxtx transactions in business logic code.
//
// It can be used standalone or embedded in a repository interface, if that repository is supposed to offer public transaction
// management to its clients.
//
// If different objects expose CtxTransactionProvider, they are expected to interoperate (a transaction created by
// one of them should be visible to and usable by the others) as long as they use the same underlying transaction system.
//
// Note: the clients of CtxTransactionProvider are not expected to access the transaction directly, as they are expected
// to be agnostic to the exact implementation of transactions being used. They only see a modified context that can be
// then passed to other functions (e.g. repository functions) that can leverage the transaction contained within.
//
// Typical usage:
//
//	err := provider.DoInReadWriteCtxTransaction(ctx, func(ctx context.Context) error {
//		 err := someRepository.FirstOperationInTransaction(ctx)
//		 if err != nil {
//		     return err
//		 }
//
//		 err = otherRepository.SecondOperationInTransaction(ctx)
//		 if err != nil {
//		     return err
//		 }
//
//		 return nil
//	})
//
//	if err != nil {
//	    // handle error
//	}
//
// These two operations would be executed in one transaction. If a new transaction had to be created it
// it's rolled back or committed as needed before DoInReadOnlyCtxTransaction returns. If the original
// context already carried a transaction, that transaction is left to its creator to rollback or commit
// as needed.
type CtxTransactionProvider interface {
	// DoInReadOnlyCtxTransaction executes the given payload function with a context that contains a transaction.
	//
	// If the given context already contains a transaction, that transaction is used instead of creating a new one.
	// In such a case that transaction will _not_ be committed / rolled back by this function (this is left to
	// whoever created it). The pre-existing transaction can be either read-write or read-only.
	//
	// If the given context does not contain a transaction, a new one will be created. If the payload function returns an
	// error, that transaction will be rolled back before DoInReadOnlyCtxTransaction exits. Otherwise it will be
	// committed.
	//
	// If the payload function returns an error DoInReadOnlyCtxTransaction will return this error as is. Additionally
	// DoInReadOnlyCtxTransaction may return own errors if handling the transaction fails.
	//
	// If you need more control over the transaction consider using BeginCtxTransaction, UseCtxTransaction, CommitCtxTransaction and
	// RollbackCtxTransaction.
	DoInReadOnlyCtxTransaction(ctx context.Context, payload func(context.Context) error) error

	// DoInReadWriteCtxTransaction behaves like DoInReadOnlyCtxTransaction, except that the transaction is read-write.
	//
	// An error is returned if the given context already contains a read-only transaction.
	DoInReadWriteCtxTransaction(ctx context.Context, payload func(context.Context) error) error

	// BeginCtxTransaction starts a new transaction in given mode and puts it in the returned context.
	//
	// Compared with DoInReadOnlyCtxTransaction and DoInReadWriteCtxTransaction the functions BeginCtxTransaction,
	// UseCtxTransaction, CommitCtxTransaction and RollbackCtxTransaction afford more control over the transactions.
	// They might be useful e.g. if readOnly needs to be a parameter or e.g. if the transaction needs to be committed
	// on certain types of errors, or conversely rolled back even if the returned result is not an error.
	//
	// An error is returned if another transaction already exists in the given context.
	BeginCtxTransaction(ctx context.Context, readOnly bool) (context.Context, error)

	// UseCtxTransaction makes sure there is a suitable transaction in the given context, starting a new one in
	// given mode if necessary.
	//
	// If a new transaction was created, true is returned in shouldRollbackOrCommit. Clients can use this to decide whether
	// they should take responsibility for rolling back or committing this transaction.
	//
	// An error is returned if readOnly is false and the given context already contains a read-only transaction.
	UseCtxTransaction(ctx context.Context, readOnly bool) (txCtx context.Context, shouldRollbackOrCommit bool, err error)

	// CommitCtxTransaction commits the context transaction and returns a context without a transaction.
	// An error is returned if there is no transaction in the given context.
	CommitCtxTransaction(ctx context.Context) (context.Context, error)

	// RollbackCtxTransaction rolls back the context transaction and returns a context without a transaction.
	// An error is returned if there is no transaction in the given context.
	RollbackCtxTransaction(ctx context.Context) (context.Context, error)
}

// SqlxCtxTransactionSupport can be embedded in an object to provide CtxTransactionProvider
// functionality. It also provides additional utility functions to the object itself.
//
// Typical usage:
//
//	type SomeRepository struct {
//		 // initialise this in the constructor
//		 *SqlxCtxTransactionSupport
//	}
//
//	func (r SomeRepository) RepositoryFunction(ctx context.Context) {
//		 err := r.DoInReadWriteSqlxTransaction(ctx, func(_ context.Context, tx *sqlx.Tx) error {
//			 err := tx.GetContext(...)
//			 if err != nil {
//		 		 return err
//			 }
//
//			 _, err = tx.ExecContext(...)
//			 if err != nil {
//				 return err
//			 }
//
//			 return nil
//		 })
//		 if err != nil {
//			 // handle error
//		 }
//	}
//
// These two operations would be executed in one transaction. If a new transaction had to be created it
// it's rolled back or committed as needed before DoInReadWriteSqlxTransaction returns. If the original
// context already carried a transaction, that transaction is left to its creator to rollback or commit
// as needed.
//
// Note: DoInReadWriteSqlxTransaction (defined by SqlxCtxTransactionSupport) and DoInReadWriteCtxTransaction
// (defined by CtxTransactionProvider) can be easily confused. Their function is very similar - both start
// a new transaction if needed or reuse one already existing in the context and both handle rollback / commit
// automatically. However their intended usage is different. DoInReadWriteSqlxTransaction is intended for use
// _inside_ the repository code that knows that the repository uses sqlx.Tx transactions.
// DoInReadWriteCtxTransaction is intended to be used by the business logic code that might be combining several
// repository calls in a single transaction. It is not exposed to any specific transaction implementation.
//
// In a repository properly implementing the ctxtx contract _every_ method should execute its logic
// inside either DoInReadOnlySqlxTransaction or DoInReadWriteSqlxTransaction, even if it contains just
// a single database operation. This is because the caller might want to combine this repository function
// call with other repository function calls to form a larger transactional unit. Code that uses sqlx.DB directly
// or creates its own transaction would not function correctly in such a context.
type SqlxCtxTransactionSupport struct {
	db *sqlx.DB
}

// NewSqlxCtxTransactionSupport creates a new SqlxCtxTransactionSupport for a given sqlx.DB connection.
func NewSqlxCtxTransactionSupport(db *sqlx.DB) *SqlxCtxTransactionSupport {
	return &SqlxCtxTransactionSupport{db}
}

type sqlxTransactionCtxMarker struct{}

var sqlxTransactionCtxMarkerKey = &sqlxTransactionCtxMarker{}

// DoInReadOnlyCtxTransaction implements CtxTransactionProvider method - see interface.
func (s *SqlxCtxTransactionSupport) DoInReadOnlyCtxTransaction(ctx context.Context, payload func(context.Context) error) error {
	return s.doInCtxTransaction(ctx, true, payload)
}

// DoInReadWriteCtxTransaction implements CtxTransactionProvider method - see interface.
func (s *SqlxCtxTransactionSupport) DoInReadWriteCtxTransaction(ctx context.Context, payload func(context.Context) error) error {
	return s.doInCtxTransaction(ctx, false, payload)
}

// BeginCtxTransaction implements CtxTransactionProvider method - see interface.
func (s *SqlxCtxTransactionSupport) BeginCtxTransaction(ctx context.Context, readOnly bool) (context.Context, error) {
	previousT := s.getTransaction(ctx)
	if previousT != nil {
		return ctx, fmt.Errorf("the context already contains a transaction")
	}

	t, err := s.beginTransaction(ctx, readOnly)
	if err != nil {
		return ctx, err
	}

	return context.WithValue(ctx, sqlxTransactionCtxMarkerKey, t), nil
}

// UseCtxTransaction implements CtxTransactionProvider method - see interface.
func (s *SqlxCtxTransactionSupport) UseCtxTransaction(ctx context.Context, readOnly bool) (context.Context, bool, error) {
	txCtx, t, err := s.useCtxTransaction(ctx, readOnly)
	if err != nil {
		return ctx, false, fmt.Errorf("failed to acquire transaction: %w", err)
	}

	return txCtx, t.shouldRollbackOrCommit, nil
}

// CommitCtxTransaction implements CtxTransactionProvider method - see interface.
func (s *SqlxCtxTransactionSupport) CommitCtxTransaction(ctx context.Context) (context.Context, error) {
	t := s.getTransaction(ctx)
	if t != nil {
		err := t.commit()
		if err != nil {
			return ctx, err
		}

		return context.WithValue(ctx, sqlxTransactionCtxMarkerKey, nil), nil
	}

	return ctx, fmt.Errorf("no transaction in the context")
}

// RollbackCtxTransaction implements CtxTransactionProvider method - see interface.
func (s *SqlxCtxTransactionSupport) RollbackCtxTransaction(ctx context.Context) (context.Context, error) {
	t := s.getTransaction(ctx)
	if t != nil {
		err := t.rollback()
		if err != nil {
			return ctx, err
		}

		return context.WithValue(ctx, sqlxTransactionCtxMarkerKey, nil), nil
	}

	return ctx, fmt.Errorf("no transaction in the context")
}

// DoInReadOnlySqlxTransaction is a convenience function that behaves like DoInReadOnlyCtxTransaction but the payload function exposes
// an *sqlx.Tx in addition to a context. The caller can safely ignore the context passed to the payload function unless there is
// a need to nest these calls.
func (s *SqlxCtxTransactionSupport) DoInReadOnlySqlxTransaction(ctx context.Context, payload func(context.Context, *sqlx.Tx) error) error {
	return s.doInTransaction(ctx, true, payload)
}

// DoInReadWriteSqlxTransaction is a convenience function that behaves like DoInReadWriteCtxTransaction but the payload function exposes
// an *sqlx.Tx in addition to a context. The caller can safely ignore the context passed to the payload function unless there is
// // a need to nest these calls.
func (s *SqlxCtxTransactionSupport) DoInReadWriteSqlxTransaction(ctx context.Context, payload func(context.Context, *sqlx.Tx) error) error {
	return s.doInTransaction(ctx, false, payload)
}

// HasTransaction returns true if the context contains a compatible transaction.
func (s *SqlxCtxTransactionSupport) HasTransaction(ctx context.Context) bool {
	return s.getTransaction(ctx) != nil
}

// IsTransactionReadOnly returns true, true if the context contains a compatible transaction which is readonly. False, true is returned
// if the transaction is read-write. False, false is returned if there is not transaction at all.
func (s *SqlxCtxTransactionSupport) IsTransactionReadOnly(ctx context.Context) (readOnly bool, ok bool) {
	t := s.getTransaction(ctx)
	if t == nil {
		return false, false
	}

	return t.readOnly, true
}

func (s *SqlxCtxTransactionSupport) getTransaction(ctx context.Context) *sqlxTransaction {
	t, ok := ctx.Value(sqlxTransactionCtxMarkerKey).(*sqlxTransaction)
	if ok {
		return t
	}

	return nil
}

func (s *SqlxCtxTransactionSupport) getCorrectModeTransaction(ctx context.Context, readOnly bool) (*sqlxTransaction, error) {
	t, ok := ctx.Value(sqlxTransactionCtxMarkerKey).(*sqlxTransaction)
	if ok {
		// if read-write transaction is request but a read-only transaction found
		if !readOnly && t.readOnly {
			return nil, fmt.Errorf("context transaction is read-only and cannot be used in read-write mode - elevate the context transaction to read-write mode")
		}

		return t, nil
	}

	return nil, nil
}

func (s *SqlxCtxTransactionSupport) useCtxTransaction(ctx context.Context, readOnly bool) (context.Context, *sqlxTransaction, error) {
	t, err := s.getCorrectModeTransaction(ctx, readOnly)
	if err != nil {
		return nil, nil, err
	}

	if t != nil {
		if t.shouldRollbackOrCommit {
			// repackage this existing transaction as one not to be rolled back or committed at the end
			t = t.toExternal()
			return context.WithValue(ctx, sqlxTransactionCtxMarkerKey, t), t, nil
		}

		// same context is ok, transaction will not be rolled back or committed at the end
		return ctx, t, nil
	}

	// no transaction in context, begin a new one
	t, err = s.beginTransaction(ctx, readOnly)
	if err != nil {
		return nil, nil, err
	}

	return context.WithValue(ctx, sqlxTransactionCtxMarkerKey, t), t, nil
}

func (s *SqlxCtxTransactionSupport) beginTransaction(ctx context.Context, readOnly bool) (*sqlxTransaction, error) {
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  readOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	t := &sqlxTransaction{
		tx:                     tx,
		readOnly:               readOnly,
		shouldRollbackOrCommit: true,
	}

	return t, nil
}

func (s *SqlxCtxTransactionSupport) doInCtxTransaction(ctx context.Context, readOnly bool, payload func(context.Context) error) error {
	txCtx, t, err := s.useCtxTransaction(ctx, readOnly)
	if err != nil {
		return err
	}

	return t.doInTransaction(txCtx, func(_ context.Context, _ *sqlx.Tx) error {
		// use the context from useCtxTransaction in this case
		return payload(txCtx)
	})
}

func (s *SqlxCtxTransactionSupport) doInTransaction(ctx context.Context, readOnly bool, payload func(context.Context, *sqlx.Tx) error) error {
	ctxTx, t, err := s.useCtxTransaction(ctx, readOnly)
	if err != nil {
		return fmt.Errorf("failed to acquire transaction: %w", err)
	}

	return t.doInTransaction(ctxTx, payload)
}

type sqlxTransaction struct {
	tx                     *sqlx.Tx
	readOnly               bool
	shouldRollbackOrCommit bool
}

func (t *sqlxTransaction) toExternal() *sqlxTransaction {
	return &sqlxTransaction{
		t.tx,
		t.readOnly,
		false,
	}
}

func (t *sqlxTransaction) commit() error {
	if t.shouldRollbackOrCommit {
		return t.tx.Commit()
	}

	return nil
}

func (t *sqlxTransaction) rollback() error {
	if t.shouldRollbackOrCommit {
		return t.tx.Rollback()
	}

	return nil
}

func (t *sqlxTransaction) doInTransaction(ctx context.Context, payload func(context.Context, *sqlx.Tx) error) error {
	rollback := func(err error) error {
		errRollback := t.rollback()
		if errRollback != nil {
			if !isIgnoredRollbackError(errRollback) {
				if err != nil {
					// wrap the original error, include the rollback error in the message
					return fmt.Errorf("failed to rollback transaction with error [%s] after error: %w", errRollback.Error(), err)
				}

				// don't wrap the rollback error so that this case is consistent with the above case
				return fmt.Errorf("failed to rollback transaction with error [%s]", errRollback.Error())
			}
		}

		// always return the original error
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			// ignore the rollback error in this case, as there's nothing to do with it unless we depend on a logger
			//   or modify the original panic payload
			_ = rollback(nil)
			// re-panic with the same payload
			panic(r)
		}
	}()

	err := payload(ctx, t.tx)
	if err != nil {
		return rollback(err)
	}

	err = t.commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return err
}

func isIgnoredRollbackError(err error) bool {
	// we ignore rollback errors if the connection is broken or the transaction already closed
	//   in either case the transaction should be already rolled back unless an explicit commit was made
	return errors.Is(err, sql.ErrTxDone) || errors.Is(err, sql.ErrConnDone)
}

// NopCtxTransactionProvider is a dummy implementation of CtxTransactionProvider that can be used in tests if it's not
// desirable to test whether the provider was invoked and not convenient to use a mock for the provider.
//
// The NOP provider ignores calls to BeginCtxTransaction, UseCtxTransaction,
// CommitCtxTransaction and RollbackCtxTransaction and in case of calls to DoInReadOnlyCtxTransaction and DoInReadWriteCtxTransaction
// it just executes the payload function with the same context as it was provided with.
//
// Note that the NOP provider ignores some subtle aspects of the CtxTransactionProvider contract, e.g. it does not return
// an error if a read-only transactions would have to be promoted to a read-write one. In many cases these departures are irrelevant to
// the test, but there might be exceptions.
type NopCtxTransactionProvider struct{}

// DoInReadOnlyCtxTransaction provides a dummy implementation of the CtxTransactionProvider method - see interface.
func (n NopCtxTransactionProvider) DoInReadOnlyCtxTransaction(ctx context.Context, payload func(context.Context) error) error {
	return payload(ctx)
}

// DoInReadWriteCtxTransaction provides a dummy implementation of the CtxTransactionProvider method - see interface.
func (n NopCtxTransactionProvider) DoInReadWriteCtxTransaction(ctx context.Context, payload func(context.Context) error) error {
	return payload(ctx)
}

// BeginCtxTransaction provides a dummy implementation of the CtxTransactionProvider method - see interface.
func (n NopCtxTransactionProvider) BeginCtxTransaction(ctx context.Context, _ bool) (context.Context, error) {
	return ctx, nil
}

// UseCtxTransaction provides a dummy implementation of the CtxTransactionProvider method - see interface.
func (n NopCtxTransactionProvider) UseCtxTransaction(ctx context.Context, _ bool) (context.Context, bool, error) {
	return ctx, true, nil
}

// CommitCtxTransaction provides a dummy implementation of the CtxTransactionProvider method - see interface.
func (n NopCtxTransactionProvider) CommitCtxTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

// RollbackCtxTransaction provides a dummy implementation of the CtxTransactionProvider method - see interface.
func (n NopCtxTransactionProvider) RollbackCtxTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
