package replication

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"

	"github.com/film42/pghost/pglogrepl"
	"github.com/film42/pghost/pgoutput"
)

type LogicalReplicator struct {
	Conn    *pgconn.PgConn
	Handler *PgOutputUtil
}

func NewLogicalReplicator(conn *pgconn.PgConn) *LogicalReplicator {
	return &LogicalReplicator{
		Conn:    conn,
		Handler: NewPgOutputUtil(),
	}
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

func (lr *LogicalReplicator) CreateReplicationSlot(ctx context.Context, name string, temporary bool) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, lr.Conn, name, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: temporary})
	return err
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

	var hadWritesSinceLastCommit bool

	// Replicate until we reach the provided checkpoint.
	// NOTE: This is best effor, so it's ok if we go beyond the checkpoint LSN a little bit
	// since the main goal is to transition to the builtin wal sender worker.
	for lastAckedLSN < checkpointLSN {
		var shouldAckKnownLSN bool

		// NOTE: We should tick at least once every 10 seconds as a heart beat.
		rcvCtx, cancelFunc := context.WithTimeout(ctx, time.Second*10)

		// Receive the mesasge
		msg, err := lr.Conn.ReceiveMessage(rcvCtx)
		cancelFunc()
		switch {
		case pgconn.Timeout(err):
			// Time to send a heartbeat.
			shouldAckKnownLSN = true
		case err != nil:
			return err
		}

		// Handle the message
		switch msg := msg.(type) {
		case nil:
			// timeout triggered, continue...
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return err
				}
				// Update the latest ack LSN if it's older than the server wal end.
				if pkm.ServerWALEnd > lastAckedLSN {
					lastAckedLSN = pkm.ServerWALEnd
				}
				if pkm.ReplyRequested {
					// Signal we should ACK our completed LSN.
					shouldAckKnownLSN = true
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
					lr.Handler.CacheRelation(v)
				case *pgoutput.Begin:
					hadWritesSinceLastCommit = false
					err = lr.Handler.HandleBegin(v)
				case *pgoutput.Commit:
					err = lr.Handler.HandleCommit(v)
					// Signal that an ACK should be sent back to the DB.
					// Only do it if there were writes. This cuts down on the millions of txns that happen.
					shouldAckKnownLSN = hadWritesSinceLastCommit
					lastAckedLSN = pglogrepl.LSN(v.TransactionLSN)
				case *pgoutput.Delete:
					hadWritesSinceLastCommit = true
					err = lr.Handler.HandleDelete(v)
				case *pgoutput.Insert:
					hadWritesSinceLastCommit = true
					err = lr.Handler.HandleInsert(v)
				case *pgoutput.Update:
					hadWritesSinceLastCommit = true
					err = lr.Handler.HandleUpdate(v)
					// log.Println(err)
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

		// ACK if requested
		shouldAckKnownLSN = shouldAckKnownLSN
		if shouldAckKnownLSN {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, lr.Conn,
				pglogrepl.StandbyStatusUpdate{
					WALApplyPosition: lastAckedLSN,
					WALFlushPosition: lastAckedLSN,
					WALWritePosition: lastAckedLSN,
				})
			if err != nil {
				return err
			}
			log.Println("LSN Status Update:", lastAckedLSN)
		}
	}

	fmt.Println("done")

	// If we've gotten to this point then we know that we have handled all changes up to the checkpoint LSN.
	// In other words, we are done.
	return nil
}
