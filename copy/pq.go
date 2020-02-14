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

type CopyWithPq struct {
	minId int64
	maxId int64

	Cfg *config.Config
}

func (cb *CopyWithPq) CopyOneBatchCustomImpl(ctx context.Context, startingAtId int64) error {
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

	// Fetch the minId
	err = srcConn.QueryRow(ctx, fmt.Sprintf("SELECT MIN(id) FROM %s.%s",
		cb.Cfg.SourceSchemaName, cb.Cfg.SourceTableName)).Scan(&cb.minId)
	if err != nil {
		return err
	}

	// Fetch the maxId
	err = srcConn.QueryRow(ctx, fmt.Sprintf("SELECT MAX(id) FROM %s.%s",
		cb.Cfg.SourceSchemaName, cb.Cfg.SourceTableName)).Scan(&cb.maxId)
	if err != nil {
		return err
	}

	errorsChan := make(chan error, 100)
	// This needs to be blocking so we can know all work has been handed off.
	pendingWorkChan := make(chan int64)
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
				case nextId := <-pendingWorkChan:
					// err := cb.CopyOneBatch(ctx, nextId)
					err := cb.CopyOneBatchCustomImpl(ctx, nextId)
					if err != nil {
						errorsChan <- err
						continue
					}

					log.Printf("Copied batch for range: %d through %d", nextId, nextId+int64(cb.Cfg.CopyBatchSize))
				}
			}
		}()
	}

	// Send work with blocking chan.
	nextId := cb.minId
	for {
		if nextId > cb.maxId {
			break
		}

		select {
		case pendingWorkChan <- nextId:
		case err := <-errorsChan:
			return err
			cancelFunc()
		}

		nextId += int64(cb.Cfg.CopyBatchSize)
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
