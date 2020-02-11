package main

import (
	"context"
	"io"
	"sync"
	"fmt"
	"bufio"

	// "github.com/jackc/pgconn/internal/ctxwatch"
	"github.com/jackc/pgio"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
)

type CopyCmd struct {
	FromConn *pgconn.PgConn
	FromQuery string

	ToConn *pgconn.PgConn
	ToQuery string
}

func (c *CopyCmd) Do(ctx context.Context) error {
	parentContext := ctx
	ctx, cancelFunc := context.WithCancel(parentContext)
	defer cancelFunc()

	wg := sync.WaitGroup{}
	wg.Add(2)

	reader, writer := io.Pipe()
	errors := make(chan error, 2)

	go func() {
		copyToErr := c.copyTo(ctx, writer)
		errors <- copyToErr
		writer.Close()
		fmt.Println("(closd write half) Err from copyTo:", copyToErr)
		// cancelFunc()
		wg.Done()
	}()

	go func() {
		errors <- c.copyFrom(ctx, reader)
		cancelFunc()
		reader.Close()
		wg.Done()
	}()

	wg.Wait()

	// There should be two "errors" waiting (might be nil error)
	for i := 0; i < 2; i++ {
		select {
		case <-parentContext.Done():
			return parentContext.Err()
		case err := <- errors:
			if err != nil && err != context.Canceled {
				return err
			}
		default:
			// This shouldn't ever happen.
		}
	}

	return nil
}

func (c *CopyCmd) copyTo(ctx context.Context, writer io.Writer) error {
	pgConn := c.ToConn
	sql := c.ToQuery

	// Can't do this.
	// if err := pgConn.lock(); err != nil {
	// 	return nil, err
	// }

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// pgConn.contextWatcher.Watch(ctx)
	// defer pgConn.contextWatcher.Unwatch()

	// Send copy to command
	buf := make([]byte, 0, 4096)
	buf = (&pgproto3.Query{String: sql}).Encode(buf)

	n, err := pgConn.Conn().Write(buf)
	if err != nil {
		// TODO: Handle n == 0?
		n = n
		return err
	}

	// Read results
	var commandTag pgconn.CommandTag
	commandTag = commandTag
	var pgErr error
	for {
		msg, err := pgConn.ReceiveMessage(ctx)
		if err != nil {
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyDone:
		case *pgproto3.CopyData:
			_, err := writer.Write(msg.Data)
			if err != nil {
				return err
			}
		case *pgproto3.ReadyForQuery:
			return /*commandTag,*/ pgErr
		case *pgproto3.CommandComplete:
			commandTag = pgconn.CommandTag(msg.CommandTag)
		case *pgproto3.ErrorResponse:
			pgErr = pgconn.ErrorResponseToPgError(msg)
		}
	}
}

func (c *CopyCmd) copyFrom(ctx context.Context, reader io.Reader) error {
	parentContext := ctx
	ctx, cancelFunc := context.WithCancel(parentContext)
	pgConn := c.FromConn
	sql := c.FromQuery

	// if err := pgConn.lock(); err != nil {
	// 	return nil, err
	// }
	// defer pgConn.unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Send copy to command
	buf := make([]byte, 0, 4096)
	buf = (&pgproto3.Query{String: sql}).Encode(buf)

	n, err := pgConn.Conn().Write(buf)
	if err != nil {
		// TODO: Handle n == 0
		n = n
		return err
	}

	// Read until copy in response or error.
	var commandTag pgconn.CommandTag
	commandTag = commandTag
	var pgErr error
	pendingCopyInResponse := true
	for pendingCopyInResponse {
		msg, err := pgConn.ReceiveMessage(ctx)
		if err != nil {
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyInResponse:
			pendingCopyInResponse = false
		case *pgproto3.ErrorResponse:
			pgErr = pgconn.ErrorResponseToPgError(msg)
		case *pgproto3.ReadyForQuery:
			return /*commandTag,*/ pgErr
		}
	}

	// Send copy data
	// abortCopyChan := make(chan struct{})
	// copyErrChan := make(chan error, 1)
	// signalMessageChan := pgConn.signalMessage()

// 	go func() {
// 		buf := make([]byte, 0, 65536)
// 		buf = append(buf, 'd')
// 		sp := len(buf)

// 		_, err := io.Copy(pgConn.Conn(), reader)
// 		if err != nil {
// 			panic(err)
// 			copyErrChan <- err
// 		}
// 		return

	// yolo := pgio.SetInt32
	// yolo = yolo

	{
		// This is critical. This keeps things moving really fast.
		// The difference is ~50% vs ~170% cpu because of small
		// writes to the pg tcp conn.
		connBuf  := bufio.NewWriterSize(pgConn.Conn(), 8192)
		buf = make([]byte, 0, 8192)
		buf = append(buf, 'd')
		sp := len(buf)
		for {
			n, readErr := reader.Read(buf[5:cap(buf)])
			if n > 0 {
				buf = buf[0 : n+5]
				pgio.SetInt32(buf[sp:], int32(n+4))

				// The problem is that this doesn't batch so we're doing 19 byte writes forever.
				_, writeErr := connBuf.Write(buf)
				if writeErr != nil {
					// Write errors are always fatal, but we can't use asyncClose because we are in a different
					// goroutine.
					// EDIT: I made this a straigt up pgconn close.
					pgConn.Close(ctx)
					// copyErrChan <- writeErr
					// cancelFunc()
					// return writeErr
					err = writeErr
					break
				}
			}
			if readErr == io.EOF {
				err = readErr
				flushErr := connBuf.Flush()
				if flushErr != nil {
					err = flushErr
				}
				break
			}
			if readErr != nil {
				err = readErr
				break
				// copyErrChan <- readErr
				// cancelFunc()
				// return readErr
			}

			//			select {
			//			case <-abortCopyChan:
			//				return
			//			default:
			//			}
		}
	}

	// {
	// 	buf = make([]byte, 8192)
	// 	_, err = io.CopyBuffer(pgConn.Conn(), reader, buf)
	// }


	// So we're flushing everything to the DB, but not getting anything back. Why?

	// sp := len(buf)


	cancelFunc = cancelFunc


	buf = buf[:0]
	if err == io.EOF || pgErr != nil {
		copyDone := &pgproto3.CopyDone{}
		buf = copyDone.Encode(buf)
	} else {
		copyFail := &pgproto3.CopyFail{Message: err.Error()}
		buf = copyFail.Encode(buf)
	}
	_, err = pgConn.Conn().Write(buf)
	if err != nil {
		pgConn.Close(ctx)
		return err
	}

	// var copyErr error
	// for copyErr == nil && pgErr == nil {
	// 	println("done?")
	// 	msg, err := pgConn.ReceiveMessage(parentContext)
	// 	println("Got something...")
	// 	// If the error was conxtext.Canceled, see if
	// 	// something is stored in the copyErrChan from the
	// 	// goroutine above.
	// 	if err == context.Canceled {
	// 		select {
	// 		case copyErr = <- copyErrChan:
	// 			break
	// 		default:
	// 		}
	// 	}
	// 	// Otherwise return the error from Received Message
	// 	if err != nil {
	// 		panic(err)
	// 		pgConn.Close(ctx)
	// 		// TODO: Maybe do more cleanup here or something? Idk.
	// 		return err
	// 	}

	// 	switch msg := msg.(type) {
	// 	case *pgproto3.ErrorResponse:
	// 		pgErr = pgconn.ErrorResponseToPgError(msg)
	// 	}
	// }

	// buf = buf[:0]
	// if copyErr == io.EOF || pgErr != nil {
	// 	copyDone := &pgproto3.CopyDone{}
	// 	buf = copyDone.Encode(buf)
	// } else {
	// 	copyFail := &pgproto3.CopyFail{Message: copyErr.Error()}
	// 	buf = copyFail.Encode(buf)
	// }
	// _, err = pgConn.Conn().Write(buf)
	// if err != nil {
	// 	pgConn.Close(ctx)
	// 	return err
	// }

	// Read results
	for {
		println("tick...")
		msg, err := pgConn.ReceiveMessage(ctx)
		if err != nil {
			pgConn.Close(ctx)
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.ReadyForQuery:
			return /* commandTag, */ pgErr
		case *pgproto3.CommandComplete:
			commandTag = pgconn.CommandTag(msg.CommandTag)
		case *pgproto3.ErrorResponse:
			pgErr = pgconn.ErrorResponseToPgError(msg)
		}
	}
}
