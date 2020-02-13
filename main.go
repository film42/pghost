package main

import (
	"context"
	"log"
	"time"

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
	publicationName := "my_pub"
	replicationSlotName := "yolo12300000xyz"

	// Replicate any missing changes.
	lr := replication.NewLogicalReplicator(replicationConn.PgConn())
	err = lr.CreateReplicationSlot(ctx, replicationSlotName, false)
	if err != nil {
		log.Println("Ignoring error from trying to create the replication slot:", err)
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
}

func doSomeWork(ctx context.Context, conn *pgx.Conn) error {
	for i := 0; i < 5; i++ {
		rows, err := conn.Query(ctx, "update yolos4 set date = now() where id = (select min(id) from yolos4);")
		if err != nil {
			return err
		}
		rows.Values() // Load
		rows.Close()
	}
	time.Sleep(5)
	return nil
}
