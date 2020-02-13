package main

import (
	"context"
	"log"
	"time"

	"github.com/film42/pghost/copy"
	"github.com/film42/pghost/pglogrepl"
	"github.com/film42/pghost/replication"
	"github.com/jackc/pgx/v4"
)

func main() {
	// Setup replication connection.
	ctx := context.Background()
	replicationConn, err := pgx.Connect(ctx, "dbname=postgres replication=database")
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}
	defer replicationConn.Close(ctx)

	// Setup queryable connection.
	queryConn, err := pgx.Connect(ctx, "dbname=postgres")
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}
	defer queryConn.Close(ctx)

	// TODO: Turn these into parameters.
	checkpointLSN := pglogrepl.LSN(0)
	publicationName := "pub_on_yolos"
	replicationSlotName := "yolo12300000xyz"

	// Create a replication slot to catch all future changes.
	lr := replication.NewLogicalReplicator(replicationConn.PgConn())
	// For testing we'll use a temporary slot.
	err = lr.CreateReplicationSlot(ctx, replicationSlotName, true)
	if err != nil {
		log.Println("Ignoring error from trying to create the replication slot:", err)
	}

	err = doSomeWork(ctx, queryConn)
	if err != nil {
		log.Fatalln("Could not create some pending work in the replication slot:", err)
	}

	cpq := &copy.CopyWithPq{
		SourceTable:      "yolos",
		DestinationTable: "yolos2",
		BatchSize:        100000,
	}
	err = cpq.CopyUsingPq(10)
	if err != nil {
		log.Fatalln("Could not copy table:", err)
	}

	// Get the most recent xlogpos as a checkpoint.
	checkpointLSN, err = lr.CurrentXLogPos(ctx)
	if err != nil {
		log.Fatalln("Could not fetch current xlog pos:", err)
	}

	// Go!
	log.Println("CheckpointLSN:", checkpointLSN)
	err = lr.ReplicateUpToCheckpoint(ctx, replicationSlotName, checkpointLSN, publicationName)
	if err != nil {
		log.Fatalln("Error replicating to the checkpoint LSN:", err)
	}

	log.Println("Replication completed, you may now switch to standard logical replication.")
}

func doSomeWork(ctx context.Context, conn *pgx.Conn) error {
	for i := 0; i < 5; i++ {
		rows, err := conn.Query(ctx, "insert into yolos select max(id)+1 from yolos;")
		if err != nil {
			return err
		}
		rows.Values() // Load
		rows.Close()
	}
	time.Sleep(time.Second * 5)
	return nil
}
