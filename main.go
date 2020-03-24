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
	copyCmd := &cobra.Command{
		Use:   "copy [config]",
		Short: "Parallel copy one table to another (does not use logical replication)",
		Args:  cobra.MinimumNArgs(1),
		Run:   doCopy,
	}

	replicateCmd := &cobra.Command{
		Use:   "replicate [config]",
		Short: "Replicate one table to another using logical replication with batched copy",
		Args:  cobra.MinimumNArgs(1),
		Run:   doReplication,
	}
	replicateCmd.Flags().Bool("internal-do-demo-work", false, "Internal: used by bench testing a test table")

	rootCmd := &cobra.Command{Use: "pghost"}
	rootCmd.AddCommand(copyCmd)
	rootCmd.AddCommand(replicateCmd)
	rootCmd.Execute()
}

func doCopy(cmd *cobra.Command, args []string) {
	cfg, err := config.ParseConfig(args[0])
	if err != nil {
		log.Fatalln("Error parsing config:", err)
	}

	ctx := context.Background()
	log.Printf("Starting parallel COPY: Workers: %d, BatchSize: %d, KeysetPagination: %v, KeysetCacheFile: '%s'",
		cfg.CopyWorkerCount, cfg.CopyBatchSize, cfg.CopyUseKeysetPagination, cfg.CopyKeysetPaginationCacheFile)
	cpq := &copy.CopyWithPq{Cfg: cfg}
	// We pass an empty string here to not use a transaction snapshot for copying.
	// NOTE: We still can do this if we want but I see no need at the moment.
	err = cpq.DoCopy(ctx, "")
	if err != nil {
		log.Fatalln("COPY process failed:", err)
	}
	log.Println("COPY Finished!")
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

	log.Printf("Creating replication slot: %s, is_temporary: %v",
		cfg.ReplicationSlotName, cfg.ReplicationSlotIsTemporary)
	result, err := lr.CreateReplicationSlot(ctx, cfg.ReplicationSlotName, cfg.ReplicationSlotIsTemporary)
	if err != nil {
		log.Fatalln("Could not create the replication slot:", err)
	}

	if cfg.CopyUseSourceConnectionAsHotStandby {
		log.Println("Waiting for source connection (hot standby) to reach LSN:", result.ConsistentPoint)
		err = replication.WaitForHotStandbyToArriveAtLSN(ctx, cfg, result.ConsistentPoint)
		if err != nil {
			log.Fatalln("Could not wait for hot standby to catch up:", err)
		}
		log.Println("Hot standby is reached consistency point:", result.ConsistentPoint)
	}

	log.Printf("Starting parallel COPY: Workers: %d, BatchSize: %d, KeysetPagination: %v, KeysetCacheFile: '%s'",
		cfg.CopyWorkerCount, cfg.CopyBatchSize, cfg.CopyUseKeysetPagination, cfg.CopyKeysetPaginationCacheFile)
	cpq := &copy.CopyWithPq{Cfg: cfg}
	err = cpq.DoCopy(ctx, result.SnapshotName)
	if err != nil {
		log.Fatalln("COPY process failed:", err)
	}
	log.Println("COPY Finished!")

	// DEBUG: Generate some work so we can be sure we always replicate after COPY.
	shouldDoDemoWork, _ := cmd.Flags().GetBool("internal-do-demo-work")
	if shouldDoDemoWork {
		err = interalDoDemoWork(ctx, cfg)
		if err != nil {
			log.Fatalln("Internal: Could not run demo work due to error:", err)
		}
	}

	// We don't need to use hand-rolled replication when we use a transaction snapshot
	// since it's basically the same thing as regular log repl but with parallel copy.
	if !cfg.CopyUseTransactionSnapshot {
		// Get the most recent xlogpos as a checkpoint.
		checkpointLSN, err := lr.CurrentXLogPos(ctx)
		if err != nil {
			log.Fatalln("Could not fetch current xlog pos:", err)
		}

		// Replicate!
		log.Println("Starting replication with upserts up to checkpoint LSN:", checkpointLSN)
		err = lr.ReplicateUpToCheckpoint(ctx, cfg.ReplicationSlotName, checkpointLSN, cfg.PublicationName)
		if err != nil {
			log.Fatalln("Error replicating to the checkpoint LSN:", err)
		}

		log.Println("Replication completed, you may now switch to standard logical replication.")
	}

	// Check to see if we should create a subscription in the destination db.
	if cfg.SubscriptionCreateAfterCheckpoint {
		err = replication.CreateSubscription(ctx, cfg)
		if err != nil {
			log.Fatalln("Error createing subscription:", err)
		}
		log.Println("Subscription created on destination db named:", cfg.SubscriptionName)
	}

	log.Println("All done. Have a great day!")
}

func interalDoDemoWork(ctx context.Context, cfg *config.Config) error {
	// Demo work conn.
	demoWorkConn, err := pgx.Connect(ctx, cfg.SourceConnection)
	if err != nil {
		return err
	}
	defer demoWorkConn.Close(ctx)

	for i := 0; i < 5; i++ {
		rows, err := demoWorkConn.Query(ctx, "insert into yolos select max(id)+1 from yolos;")
		if err != nil {
			return err
		}
		rows.Values() // Load
		rows.Close()
	}
	return nil
}
