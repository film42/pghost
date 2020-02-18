package replication

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"

	"github.com/film42/pghost/config"
	"github.com/film42/pghost/pglogrepl"
	"github.com/film42/pghost/pgoutput"
)

type SqlApplierFunc func(ctx context.Context, sqlStatements []string) error

type LogicalReplicator struct {
	Conn       *pgconn.PgConn
	Processor  *PgOutputUtil
	SqlApplier SqlApplierFunc
	Cfg        *config.Config
}

func NewLogicalReplicator(cfg *config.Config, conn *pgconn.PgConn, sqlApplier SqlApplierFunc) *LogicalReplicator {
	return &LogicalReplicator{
		Cfg:        cfg,
		Conn:       conn,
		Processor:  NewPgOutputUtil(),
		SqlApplier: sqlApplier,
	}
}

func (lr *LogicalReplicator) AddRelationMapping(relMap *RelationMapping) {
	lr.Processor.AddRelationMapping(relMap)
}

func (lr *LogicalReplicator) CurrentXLogPos(ctx context.Context) (pglogrepl.LSN, error) {
	isr, err := lr.IdentifySystem(ctx)
	if err != nil {
		return 0, err
	}
	return isr.XLogPos, nil
}

func (lr *LogicalReplicator) IdentifySystem(ctx context.Context) (*pglogrepl.IdentifySystemResult, error) {
	isr, err := pglogrepl.IdentifySystem(ctx, lr.Conn)
	if err != nil {
		return nil, err
	}
	return &isr, nil
}

func (lr *LogicalReplicator) CreateReplicationSlot(ctx context.Context, name string, temporary bool) (*pglogrepl.CreateReplicationSlotResult, error) {
	result, err := pglogrepl.CreateReplicationSlot(ctx, lr.Conn, name, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: temporary})
	return result, err
}

func (lr *LogicalReplicator) ReplicateUpToCheckpoint(ctx context.Context, name string, checkpointLSN pglogrepl.LSN, publication string) error {
	// NOTE: We always set the lastAckedLSN to 0 because this tells postgres
	// to start from the restart_lsn which is what we want. We only store an
	// LSN after a commit anyways, so this should be fine.
	lastAckedLSN := pglogrepl.LSN(0)

	err := pglogrepl.StartReplication(ctx, lr.Conn, name, pglogrepl.LSN(0),
		pglogrepl.StartReplicationOptions{}, fmt.Sprintf("(proto_version '1', publication_names '%s')", publication))
	if err != nil {
		return err
	}

	// Preload the statements to be applied.
	statements := make([]string, 0, 1024)

	rcvTimeoutDuration := time.Second * 10
	lastStandbyStatusUpdateWasAt := time.Now()

	// Replicate until we reach the provided checkpoint.
	// NOTE: This is best effor, so it's ok if we go beyond the checkpoint LSN a little bit
	// since the main goal is to transition to the builtin wal sender worker.
	for lastAckedLSN < checkpointLSN || lr.Cfg.ReplicationContinueAfterCheckpoint {
		var sendStandbyStatusUpdate bool

		// NOTE: We should tick at least once every 10 seconds as a heart beat.
		rcvCtx, cancelFunc := context.WithTimeout(ctx, rcvTimeoutDuration)

		// Check if we've been in a busy loop and should send a status update now.
		if lastStandbyStatusUpdateWasAt.Add(rcvTimeoutDuration).Before(time.Now()) {
			sendStandbyStatusUpdate = true
			cancelFunc()
		}

		// Receive the mesasge
		msg, err := lr.Conn.ReceiveMessage(rcvCtx)
		cancelFunc()
		switch {
		case pgconn.Timeout(err):
			// Send a status update if we've been idle too long.
			sendStandbyStatusUpdate = true
		case err != nil:
			return err
		}

		// Handle the message
		switch msg := msg.(type) {
		case nil:
			// Timeout triggered, continue so we can send a heartbeat status update.
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return err
				}
				sendStandbyStatusUpdate = pkm.ReplyRequested
				// Update the latest ack LSN if it's older than the server wal end.
				if pkm.ServerWALEnd > lastAckedLSN {
					lastAckedLSN = pkm.ServerWALEnd
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return err
				}

				walMsg, err := pgoutput.Parse(xld.WALData)
				if err != nil {
					return err
				}

				switch v := walMsg.(type) {
				case *pgoutput.Relation:
					lr.Processor.CacheRelation(v)
				case *pgoutput.Begin:
					// Reset statement list.
					statements = statements[:0]
					sql, err := lr.Processor.BeginToSql(v)
					if err != nil {
						return err
					}
					statements = append(statements, sql)
				case *pgoutput.Commit:
					sql, err := lr.Processor.CommitToSql(v)
					lastAckedLSN = pglogrepl.LSN(v.TransactionLSN)
					if err != nil {
						return err
					}
					sendStandbyStatusUpdate = true
					statements = append(statements, sql)
				case *pgoutput.Delete:
					sql, err := lr.Processor.DeleteToSql(v)
					if err != nil {
						return err
					}
					statements = append(statements, sql)
				case *pgoutput.Insert:
					sql, err := lr.Processor.InsertToSql(v)
					if err != nil {
						return err
					}
					statements = append(statements, sql)
				case *pgoutput.Update:
					sql, err := lr.Processor.UpdateToSql(v)
					if err != nil {
						return err
					}
					statements = append(statements, sql)
				default:
					err = errors.New(fmt.Sprintf("error: received unknown wal message: %+v"))
				}

				// Check for all errors in the walMsg switch above.
				if err != nil {
					return err
				}

			default:
				return errors.New(fmt.Sprintf("error: received unknown replication message type: %v", msg.Data[0]))
			}
		default:
			return errors.New(fmt.Sprintf("error: did not received copy data, received: %+v", msg))
		}

		// Only apply statements if we have more than BEGIN; COMMIT;
		if len(statements) > 2 {
			err = lr.SqlApplier(ctx, statements)
			if err != nil {
				return err
			}
		}

		if sendStandbyStatusUpdate {
			// Only ACK if the sql applier finished OK.
			err = pglogrepl.SendStandbyStatusUpdate(ctx, lr.Conn,
				pglogrepl.StandbyStatusUpdate{
					WALApplyPosition: lastAckedLSN,
					WALFlushPosition: lastAckedLSN,
					WALWritePosition: lastAckedLSN,
				})
			if err != nil {
				return err
			}

			// Update our internal timer.
			lastStandbyStatusUpdateWasAt = time.Now()
			log.Println("LSN Status Update:", lastAckedLSN)
		}
	}

	// If we've gotten to this point then we know that we have handled all changes up to the checkpoint LSN.
	// In other words, we are done.
	return nil
}
