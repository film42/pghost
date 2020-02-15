package main

import (
	"context"
	"log"
	"strings"

	"github.com/film42/pghost/config"
	"github.com/film42/pghost/copy"
	"github.com/film42/pghost/replication"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/cobra"
)

func main() {
	replicateCmd := &cobra.Command{
		Use:   "replicate [config]",
		Short: "Replicate one table to another using logical replication with batched copy",
		Args:  cobra.MinimumNArgs(1),
		Run:   doReplication,
	}

	rootCmd := &cobra.Command{Use: "pghost"}
	rootCmd.AddCommand(replicateCmd)
	rootCmd.Execute()
}

func doReplication(cmd *cobra.Command, args []string) {
	cfg, err := config.ParseConfig(args[0])
	if err != nil {
		log.Fatalln("Error parsing config:", err)
	}

	// Setup replication connection.
	ctx := context.Background()
	replicationConn, err := pgx.Connect(ctx, cfg.SourceConnectionForReplication)
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}
	defer replicationConn.Close(ctx)

	// Setup queryable connection.
	queryConn, err := pgx.Connect(ctx, cfg.DestinationConnection)
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}
	defer queryConn.Close(ctx)

	// Create a replication slot to catch all future changes.
	lr := replication.NewLogicalReplicator(cfg, replicationConn.PgConn(), func(ctx context.Context, statements []string) error {
		// An applier will be handed sql statements that need to be applied to the target
		// database. The Logical Replicator will use this to determine if the results were
		// successfully applied or not.
		log.Println("STATEMENTS:", statements)
		// TODO: Clean up all this error handling.
		_, err := queryConn.Exec(ctx, strings.Join(statements, " "))
		return err
	})

	lr.AddRelationMapping(&replication.RelationMapping{
		SourceNamespace:      cfg.SourceSchemaName,
		SourceName:           cfg.SourceTableName,
		DestinationNamespace: cfg.DestinationSchemaName,
		DestinationName:      cfg.DestinationTableName,
	})

	// Check if we should skip the creation (existing replication slot)
	if !cfg.ReplicationSlotSkipCreate {
		err = lr.CreateReplicationSlot(ctx, cfg.ReplicationSlotName, cfg.ReplicationSlotIsTemporary)
		if err != nil {
			log.Fatalln("Could not create the replication slot:", err)
		}
	}

	log.Println("Starting COPY...")
	cpq := &copy.CopyWithPq{Cfg: cfg}
	err = cpq.DoCopy(ctx)
	if err != nil {
		log.Fatalln("COPY process failed:", err)
	}
	log.Println("COPY Finished!")

	// DEBUG: Generate some work so we can be sure we always replicate after COPY.
	err = doSomeWork(ctx, queryConn)
	if err != nil {
		log.Fatalln("Could not create some pending work in the replication slot:", err)
	}

	// Get the most recent xlogpos as a checkpoint.
	checkpointLSN, err := lr.CurrentXLogPos(ctx)
	if err != nil {
		log.Fatalln("Could not fetch current xlog pos:", err)
	}

	// Replicate!
	log.Println("CheckpointLSN:", checkpointLSN)
	err = lr.ReplicateUpToCheckpoint(ctx, cfg.ReplicationSlotName, checkpointLSN, cfg.PublicationName)
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
	return nil
}
