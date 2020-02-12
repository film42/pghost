package main

import (
	"fmt"
	"context"
	"io"
	"sync"
	"log"
	"bufio"

	"github.com/jackc/pgx/v4"
	"github.com/kr/pretty"
)

type copyWithPq struct {
	minId            int64
	maxId            int64
	BatchSize        int64
	SourceTable      string
	DestinationTable string
}

func (cb *copyWithPq) CopyOneBatchCustomImpl(ctx context.Context, startingAtId int64) error {
	srcConn, err := pgx.Connect(ctx, "dbname=postgres")
	if err != nil {
		return err
	}
	defer srcConn.Close(ctx)

	destConn, err := pgx.Connect(ctx, "dbname=postgres")
	if err != nil {
		return err
	}
	defer destConn.Close(ctx)

	copyToQuery := fmt.Sprintf("COPY (SELECT * FROM %s WHERE id >= %d AND id < (%d + %d)) TO STDOUT WITH BINARY", cb.SourceTable, startingAtId, startingAtId, cb.BatchSize)
	copyFromQuery := fmt.Sprintf("COPY %s FROM STDIN WITH BINARY", cb.DestinationTable)

	log.Println(copyToQuery)
	log.Println(copyFromQuery)

	cc := &CopyCmd{
		FromConn: destConn.PgConn(),
		FromQuery: copyFromQuery,

		ToConn: srcConn.PgConn(),
		ToQuery: copyToQuery,
	}

	return cc.Do(ctx)
}

func (cb *copyWithPq) CopyOneBatch(ctx context.Context, startingAtId int64) error {
	srcConn, err := pgx.Connect(ctx, "dbname=postgres")
	if err != nil {
		return err
	}
	defer srcConn.Close(ctx)

	destConn, err := pgx.Connect(ctx, "dbname=postgres")
	if err != nil {
		return err
	}
	defer destConn.Close(ctx)

	copyToQuery := fmt.Sprintf("COPY (SELECT * FROM %s WHERE id >= %d AND id < (%d + %d)) TO STDOUT WITH BINARY", cb.SourceTable, startingAtId, startingAtId, cb.BatchSize)
	copyFromQuery := fmt.Sprintf("COPY %s FROM STDIN WITH BINARY", cb.DestinationTable)

	log.Println(copyToQuery)
	log.Println(copyFromQuery)

	rx, wx := io.Pipe()
	defer wx.Close()
	defer rx.Close()
	writer := bufio.NewWriterSize(wx, 65536)
	ctxWithCancel, cancelFunc := context.WithCancel(ctx)

	// Kick off the read side. This will block even on read, so we'll be careful here.
	copyFromChan :=  make(chan error, 1)
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

func (cb *copyWithPq) CopyUsingPq(workers int) error {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "dbname=postgres host=/var/run/postgresql")
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Fetch the minId
	err = conn.QueryRow(ctx, fmt.Sprintf("SELECT MIN(id) FROM %s", cb.SourceTable)).Scan(&cb.minId)
	if err != nil {
		return err
	}

	// Fetch the maxId
	err = conn.QueryRow(ctx, fmt.Sprintf("SELECT MAX(id) FROM %s", cb.SourceTable)).Scan(&cb.maxId)
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
	for i := 0; i < workers; i++ {
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
						log.Println("WARNING!!! nextId:", nextId, "Found err while copying:", err)
						errorsChan <- err
					}
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
			panic(err)
			cancelFunc()
		}

		nextId += cb.BatchSize
	}

	close(done)
	wg.Wait()

	pretty.Println(cb)

	return nil
}
