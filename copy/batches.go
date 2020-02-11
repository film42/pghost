package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

type W struct {
	io.Writer
}

func (t *W) Write(b []byte) (int, error) {
	n, err := t.Writer.Write(b)
	// fmt.Println("Wrote n bytes:", n)
	return n, err
}

type R struct {
	io.Reader
}

func (t *R) Read(b []byte) (int, error) {
	n, err := t.Reader.Read(b)
	// fmt.Println("Read n bytes:", n)
	return n, err
}

// The idea here is that we can scan through an index in the most cheap and naive way
// possible: Walk the ID as a simulated keyset pagination scan.
// Start with the min(id) and walk by 1,000,000 to max(id).
// From this point, the replication merge tool should take over.
// Will this work? Idk. Let's find out.
func copyIdRange(srcTable, destTable string, minId, batchSize int64) error {
	// Create the COPY TO.
	copyToQuery := fmt.Sprintf("COPY (SELECT * FROM %s WHERE id >= %d AND id < (%d + %d)) TO STDOUT WITH BINARY", srcTable, minId, minId, batchSize)
	log.Println(copyToQuery)
	copyToCmd := exec.Command("psql", "-U", "postgres", "-c", copyToQuery)
	copyToCmd.Stderr = os.Stderr

	// Create the COPY FROM.
	copyFromQuery := fmt.Sprintf("COPY %s FROM STDIN WITH BINARY", destTable)
	log.Println(copyFromQuery)
	copyFromCmd := exec.Command("psql", "-U", "postgres", "-c", copyFromQuery)
	copyFromCmd.Stderr = os.Stderr

	// Wire the COPY TO to point at the COPY FROM.
	copyToCmdStdout, err := copyToCmd.StdoutPipe()
	if err != nil {
		return err
	}
	// defer copyToCmdStdout.Close()
	copyFromCmd.Stdin = copyToCmdStdout

	// Run the processes to completion.
	if err = copyFromCmd.Start(); err != nil {
		return err
	}
	if err = copyToCmd.Start(); err != nil {
		return err
	}

	if err = copyToCmd.Wait(); err != nil {
		return err
	}
	if err = copyFromCmd.Wait(); err != nil {
		return err
	}
	return nil
}

type BatchCopyBoy struct {
	minId            int64
	maxId            int64
	SourceTable      string
	DestinationTable string
}

func (cb *BatchCopyBoy) discoverIdRange() error {
	cmd := exec.Command("psql", "-U", "postgres", "-Atc", fmt.Sprintf("SELECT MIN(id) FROM %s", cb.SourceTable))
	n, err := cmd.Output()
	if err != nil {
		return err
	}
	min, err := strconv.ParseInt(strings.TrimSpace(string(n)), 0, 64)
	if err != nil {
		return err
	}
	cb.minId = min

	cmd = exec.Command("psql", "-U", "postgres", "-Atc", fmt.Sprintf("SELECT MAX(id) FROM %s", cb.SourceTable))
	n, err = cmd.Output()
	if err != nil {
		return err
	}
	max, err := strconv.ParseInt(strings.TrimSpace(string(n)), 0, 64)
	if err != nil {
		return err
	}
	cb.maxId = max
	return nil
}

func (cb *BatchCopyBoy) CopyInBatches(batchSize int64, workers int) error {
	err := cb.discoverIdRange()
	if err != nil {
		return err
	}

	// DEBUG: Print range
	log.Println("Min:", cb.minId, "Max:", cb.maxId)
	errorsChan := make(chan error, 100)
	// This needs to be blocking so we can know all work has been handed off.
	pendingWorkChan := make(chan int64)
	wg := sync.WaitGroup{}
	ctx, cancelFunc := context.WithCancel(context.Background())

	// Spawn workers.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			for {
				select {
				case <-ctx.Done():
					wg.Done()
					return
				case nextId := <-pendingWorkChan:
					err := copyIdRange(cb.SourceTable, cb.DestinationTable, nextId, batchSize)
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
			cancelFunc()
			return err
		}
		nextId += batchSize
	}

	// Await.
	cancelFunc()
	wg.Wait()
	return nil
}
