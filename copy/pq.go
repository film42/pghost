package copy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/film42/pghost/config"
	"github.com/jackc/pgx/v4"
)

type IdRange struct {
	StartAt int64
	EndAt   int64
}

type CopyWithPq struct {
	minId int64
	maxId int64

	Cfg *config.Config
}

type IdRangeSeq interface {
	Next() *IdRange
}

type indexWalkSeq struct {
	minId     int64
	maxId     int64
	currentId int64
	batchSize int
}

func (s *indexWalkSeq) Next() *IdRange {
	if s.currentId >= s.maxId {
		return nil
	}
	endAt := s.currentId + int64(s.batchSize) - 1
	ir := &IdRange{StartAt: s.currentId, EndAt: endAt}
	s.currentId = endAt + 1
	return ir
}

func WalkTableIds(ctx context.Context, txn pgx.Tx, schemaName, tableName string, batchSize int) (IdRangeSeq, error) {
	iws := &indexWalkSeq{batchSize: batchSize}

	// Fetch the minId
	err := txn.QueryRow(ctx, fmt.Sprintf("SELECT MIN(id) FROM %s.%s", schemaName, tableName)).Scan(&iws.minId)
	if err != nil {
		return nil, err
	}
	iws.currentId = iws.minId

	// Fetch the maxId
	err = txn.QueryRow(ctx, fmt.Sprintf("SELECT MAX(id) FROM %s.%s", schemaName, tableName)).Scan(&iws.maxId)
	if err != nil {
		return nil, err
	}

	return iws, nil
}

func (cb *CopyWithPq) CopyOneBatchCustomImpl(ctx context.Context, srcTableColumns []string, idRange *IdRange, transactionSnapshotId string) error {
	srcConn, err := pgx.Connect(ctx, cb.Cfg.SourceConnection)
	if err != nil {
		return err
	}
	defer srcConn.Close(ctx)

	// Load the session to use a repeatable read isolation level.
	if len(transactionSnapshotId) > 0 {
		_, err = srcConn.Exec(ctx, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
		if err != nil {
			return err
		}
	}

	srcConnTxn, err := srcConn.Begin(ctx)
	if err != nil {
		return err
	}
	defer srcConnTxn.Commit(ctx)

	// See if we should load the commit with an existing transaction snapshot id.
	if len(transactionSnapshotId) > 0 {
		_, err = srcConnTxn.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", transactionSnapshotId))
		if err != nil {
			panic(err)
			return err
		}
	}

	destConn, err := pgx.Connect(ctx, cb.Cfg.DestinationConnection)
	if err != nil {
		return err
	}
	defer destConn.Close(ctx)

	columnNames := strings.Join(srcTableColumns, ", ")
	copyToQuery := fmt.Sprintf("COPY (SELECT * FROM %s.%s WHERE id >= %d AND id <= %d) TO STDOUT",
		cb.Cfg.SourceSchemaName, cb.Cfg.SourceTableName, idRange.StartAt, idRange.EndAt)
	copyFromQuery := fmt.Sprintf("COPY %s.%s (%s) FROM STDIN",
		cb.Cfg.DestinationSchemaName, cb.Cfg.DestinationTableName, columnNames)

	// By this point any fancy transaction logic should be applied and we should be
	// good to copy this data.
	cc := &CopyCmd{
		FromConn:  destConn.PgConn(),
		FromQuery: copyFromQuery,

		ToConn:  srcConn.PgConn(),
		ToQuery: copyToQuery,
	}

	return cc.Do(ctx)
}

func getColumnNamesForTable(ctx context.Context, txn pgx.Tx, schemaName, tableName string) ([]string, error) {
	sql := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' and table_name = '%s'",
		schemaName, tableName)

	rows, err := txn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnNames := []string{}
	for rows.Next() {
		var columnName string
		err = rows.Scan(&columnName)
		if err != nil {
			return nil, err
		}
		columnNames = append(columnNames, columnName)
	}

	if len(columnNames) == 0 {
		return nil, errors.New("error: could not find any column on source table")
	}

	return columnNames, nil
}

func isHotStandbyFeedbackError(err error) bool {
	return strings.Contains(err.Error(), "canceling statement due to conflict with recovery")
}

func withHotStandbyRetry(callback func() error) error {
	var err error
	for i := 0; i < 3; i++ {
		err = callback()
		// When everything "just works" we'll break and keep going.
		if err == nil {
			return nil
		}
		// When we detect "conflict with recovery" we'll sleep and retry up to 3 times.
		if isHotStandbyFeedbackError(err) {
			log.Println("Detected hot standby feedback, sleeping 10 second before retrying...")
			time.Sleep(10 * time.Second)
			continue
		}
		// If it's a different error we'll stop now.
		return err
	}

	// This should be the hot standby error after 3 failures.
	return err
}

func (cb *CopyWithPq) saveOrLoadKeysetPaginatedTableIdRange(ctx context.Context, txn pgx.Tx) (IdRangeSeq, error) {
	if len(cb.Cfg.CopyKeysetPaginationCacheFile) == 0 {
		return KeysetPaginateTable(ctx, txn, cb.Cfg.SourceSchemaName, cb.Cfg.SourceTableName, cb.Cfg.CopyBatchSize)
	}

	ks := new(keysetSeq)
	err := ks.Load(cb.Cfg.CopyKeysetPaginationCacheFile)
	if err == nil {
		return ks, nil
	}

	idRangeSet, err := KeysetPaginateTable(ctx, txn, cb.Cfg.SourceSchemaName, cb.Cfg.SourceTableName, cb.Cfg.CopyBatchSize)
	if err != nil {
		return nil, err
	}

	err = idRangeSet.(*keysetSeq).Save(cb.Cfg.CopyKeysetPaginationCacheFile)
	if err != nil {
		return nil, err
	}
	return idRangeSet, nil
}

func (cb *CopyWithPq) DoCopy(ctx context.Context, transactionSnapshotId string) error {
	srcConn, err := pgx.Connect(ctx, cb.Cfg.SourceConnection)
	if err != nil {
		return err
	}
	defer srcConn.Close(ctx)
	srcConnTxn, err := srcConn.Begin(ctx)
	if err != nil {
		return err
	}

	srcTableColumnNames, err := getColumnNamesForTable(ctx, srcConnTxn,
		cb.Cfg.SourceSchemaName, cb.Cfg.SourceTableName)
	if err != nil {
		return err
	}

	var idRangeSeq IdRangeSeq
	if cb.Cfg.CopyUseKeysetPagination {
		idRangeSeq, err = cb.saveOrLoadKeysetPaginatedTableIdRange(ctx, srcConnTxn)
	} else {
		idRangeSeq, err = WalkTableIds(ctx, srcConnTxn,
			cb.Cfg.SourceSchemaName, cb.Cfg.SourceTableName, cb.Cfg.CopyBatchSize)
	}
	// Check error for both IdRangeSeq builders above.
	if err != nil {
		return err
	}

	// See if we should capture a transaction snapshot.
	if cb.Cfg.CopyUseTransactionSnapshot {
		// This is super super important. Without this we'll lose the snapshot after the first
		// copy transactions finish. By holding this reference, we can hack the planet.
		defer srcConnTxn.Commit(ctx)
	} else {
		// Release the transaction since we won't need it going forward. And set snapshot to "".
		transactionSnapshotId = ""
		err = srcConnTxn.Commit(ctx)
		if err != nil {
			return nil
		}
	}

	errorsChan := make(chan error, 100)
	// This needs to be blocking so we can know all work has been handed off.
	pendingWorkChan := make(chan *IdRange)
	wg := sync.WaitGroup{}
	ctx, cancelFunc := context.WithCancel(context.Background())
	done := make(chan bool)

	// Spawn workers.
	for i := 0; i < cb.Cfg.CopyWorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				case <-ctx.Done():
					return
				case nextIdRange := <-pendingWorkChan:
					log.Printf("Starting batch for range: %d through %d", nextIdRange.StartAt, nextIdRange.EndAt)

					err := withHotStandbyRetry(func() error {
						return cb.CopyOneBatchCustomImpl(ctx, srcTableColumnNames, nextIdRange, transactionSnapshotId)
					})
					if err != nil {
						errorsChan <- err
						continue
					}

					log.Printf("Finished batch for range: %d through %d", nextIdRange.StartAt, nextIdRange.EndAt)
				}
			}
		}()
	}

	// Send work with blocking chan.
	for {
		nextIdRange := idRangeSeq.Next()
		if nextIdRange == nil {
			break
		}

		select {
		case pendingWorkChan <- nextIdRange:
		case err := <-errorsChan:
			return err
			cancelFunc()
		}
	}

	close(done)
	wg.Wait()

	// Check for any errors.
	select {
	case err := <-errorsChan:
		return err
	default:
		return nil
	}
}
