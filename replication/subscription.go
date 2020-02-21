package replication

import (
	"context"
	"errors"
	"fmt"
	"github.com/film42/pghost/config"
	"github.com/jackc/pgx/v4"
)

func CreateSubscription(ctx context.Context, cfg *config.Config) error {
	// If the source and dest table don't match, logical replication can't start.
	if cfg.SourceTableName != cfg.DestinationTableName ||
		cfg.SourceSchemaName != cfg.DestinationSchemaName {

		return errors.New("error: tables must match name in order to start a subscription")
	}

	// We need a valid publication, connection, and slot_name.
	sql := fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s WITH (copy_data = false, create_slot = false, slot_name = '%s')",
		cfg.SubscriptionName,
		cfg.SourceConnection,
		cfg.PublicationName,
		cfg.ReplicationSlotName,
	)

	destConn, err := pgx.Connect(ctx, cfg.DestinationConnection)
	if err != nil {
		return err
	}
	defer destConn.Close(ctx)

	_, err = destConn.Exec(ctx, sql)
	return err
}
