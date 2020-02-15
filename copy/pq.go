package copy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"sync"

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

func WalkTableIds(ctx context.Context, conn *pgx.Conn, schemaName, tableName string, batchSize int) (IdRangeSeq, error) {
	iws := &indexWalkSeq{batchSize: batchSize}

	// Fetch the minId
	err := conn.QueryRow(ctx, fmt.Sprintf("SELECT MIN(id) FROM %s.%s", schemaName, tableName)).Scan(&iws.minId)
	if err != nil {
		return nil, err
	}
	iws.currentId = iws.minId

	// Fetch the maxId
	err = conn.QueryRow(ctx, fmt.Sprintf("SELECT MAX(id) FROM %s.%s", schemaName, tableName)).Scan(&iws.maxId)
	if err != nil {
		return nil, err
	}

	return iws, nil
}

func (cb *CopyWithPq) CopyOneBatchCustomImpl(ctx context.Context, idRange *IdRange) error {
	srcConn, err := pgx.Connect(ctx, cb.Cfg.SourceConnection)
	if err != nil {
		return err
	}
	defer srcConn.Close(ctx)

	destConn, err := pgx.Connect(ctx, cb.Cfg.DestinationConnection)
	if err != nil {
		return err
	}
	defer destConn.Close(ctx)

	copyToQuery := fmt.Sprintf("COPY (SELECT * FROM %s.%s WHERE id >= %d AND id <= %d) TO STDOUT WITH BINARY",
		cb.Cfg.SourceSchemaName, cb.Cfg.SourceTableName, idRange.StartAt, idRange.EndAt)
	copyFromQuery := fmt.Sprintf("COPY %s.%s FROM STDIN WITH BINARY",
		cb.Cfg.DestinationSchemaName, cb.Cfg.DestinationTableName)

	cc := &CopyCmd{
		FromConn:  destConn.PgConn(),
		FromQuery: copyFromQuery,

		ToConn:  srcConn.PgConn(),
		ToQuery: copyToQuery,
	}

	return cc.Do(ctx)
}

// TODO: Remove
func (cb *CopyWithPq) CopyOneBatch(ctx context.Context, startingAtId int64) error {
	srcConn, err := pgx.Connect(ctx, cb.Cfg.SourceConnection)
	if err != nil {
		return err
	}
	defer srcConn.Close(ctx)

	destConn, err := pgx.Connect(ctx, cb.Cfg.DestinationConnection)
	if err != nil {
		return err
	}
	defer destConn.Close(ctx)

	copyToQuery := fmt.Sprintf("COPY (SELECT * FROM %s.%s WHERE id >= %d AND id < (%d + %d)) TO STDOUT WITH BINARY",
		cb.Cfg.SourceSchemaName, cb.Cfg.SourceTableName, startingAtId, startingAtId, cb.Cfg.CopyBatchSize)
	copyFromQuery := fmt.Sprintf("COPY %s.%s FROM STDIN WITH BINARY",
		cb.Cfg.DestinationSchemaName, cb.Cfg.DestinationTableName)

	log.Println(copyToQuery)
	log.Println(copyFromQuery)

	rx, wx := io.Pipe()
	defer wx.Close()
	defer rx.Close()
	writer := bufio.NewWriterSize(wx, 65536)
	ctxWithCancel, cancelFunc := context.WithCancel(ctx)

	// Kick off the read side. This will block even on read, so we'll be careful here.
	copyFromChan := make(chan error, 1)
	go func() {
		_, err := destConn.PgConn().CopyFrom(ctxWithCancel, &R{rx}, copyFromQuery)
		if err != nil {
			// Signal the write side needs to end.
			cancelFunc()
		}
		rx.Close()
		copyFromChan <- err
	}()

	_, err = srcConn.PgConn().CopyTo(ctxWithCancel, &W{writer}, copyToQuery)
	if err != nil {
		// Signal to reader that we are done.
		cancelFunc()
		return err
	}
	err = writer.Flush()
	if err != nil {
		cancelFunc()
		return err
	}
	wx.Close()

	return <-copyFromChan
}

func (cb *CopyWithPq) DoCopy(ctx context.Context) error {
	srcConn, err := pgx.Connect(ctx, cb.Cfg.SourceConnection)
	if err != nil {
		return err
	}
	defer srcConn.Close(ctx)

	var idRangeSeq IdRangeSeq
	if cb.Cfg.CopyUseKeysetPagination {
		idRangeSeq, err = KeysetPaginateTable(ctx, srcConn,
			cb.Cfg.SourceSchemaName, cb.Cfg.SourceTableName, cb.Cfg.CopyBatchSize)
	} else {
		idRangeSeq, err = WalkTableIds(ctx, srcConn,
			cb.Cfg.SourceSchemaName, cb.Cfg.SourceTableName, cb.Cfg.CopyBatchSize)
	}
	// Check error for both IdRangeSeq builders above.
	if err != nil {
		return err
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
					// err := cb.CopyOneBatch(ctx, nextId)
					err := cb.CopyOneBatchCustomImpl(ctx, nextIdRange)
					if err != nil {
						errorsChan <- err
						continue
					}

					log.Printf("Copied batch for range: %d through %d", nextIdRange.StartAt, nextIdRange.EndAt)
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
