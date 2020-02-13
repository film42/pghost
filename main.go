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

// func main() {
// 	conn, err := pgconn.Connect(context.Background(), "dbname=postgres replication=database")
// 	if err != nil {
// 		log.Fatalln("failed to connect to PostgreSQL server:", err)
// 	}
// 	defer conn.Close(context.Background())

// 	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
// 	if err != nil {
// 		log.Fatalln("IdentifySystem failed:", err)
// 	}
// 	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

// 	slotName := "yolo12300000xyz"

// 	// _, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: false})
// 	// if err != nil {
// 	// 	log.Fatalln("CreateReplicationSlot failed:", err)
// 	// }
// 	// log.Println("Created temporary replication slot:", slotName)

// 	// time.Sleep(time.Second * 5)

// 	// NOTE: We should probably always default to start at LSN(0) since that will always go back to the
// 	// oldest restart_lsn which is probably what we want.
// 	err = pglogrepl.StartReplication(context.Background(), conn, slotName, pglogrepl.LSN(0), pglogrepl.StartReplicationOptions{}, "(proto_version '1', publication_names 'my_pub')")
// 	if err != nil {
// 		log.Fatalln("StartReplication failed:", err)
// 	}
// 	log.Println("Logical replication started on slot", slotName)

// 	clientXLogPos := sysident.XLogPos
// 	standbyMessageTimeout := time.Second * 10
// 	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

// 	util := NewPgOutputUtil()

// 	for {
// 		if time.Now().After(nextStandbyMessageDeadline) {
// 			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
// 			if err != nil {
// 				log.Fatalln("SendStandbyStatusUpdate failed:", err)
// 			}
// 			log.Println("Sent Standby status message")
// 			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
// 		}

// 		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
// 		msg, err := conn.ReceiveMessage(ctx)
// 		cancel()
// 		if err != nil {
// 			if pgconn.Timeout(err) {
// 				continue
// 			}
// 			log.Fatalln("ReceiveMessage failed:", err)
// 		}

// 		switch msg := msg.(type) {
// 		case *pgproto3.CopyData:
// 			switch msg.Data[0] {
// 			case pglogrepl.PrimaryKeepaliveMessageByteID:
// 				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
// 				if err != nil {
// 					log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
// 				}
// 				// log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

// 				if pkm.ReplyRequested {
// 					nextStandbyMessageDeadline = time.Time{}
// 				}

// 			case pglogrepl.XLogDataByteID:
// 				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
// 				if err != nil {
// 					log.Fatalln("ParseXLogData failed:", err)
// 				}

// 				r, err := pgoutput.Parse(xld.WALData)
// 				if err != nil {
// 					log.Println("ERROR GETTING THE WAL DATA PARSED:", err)
// 				}
// 				// log.Println(xld.WALStart, xld.ServerWALEnd, xld.ServerTime, pretty.Sprint(r))

// 				// Debug.
// 				switch v := r.(type) {
// 				case *pgoutput.Relation:
// 					// log.Println(pretty.Sprint(v))
// 					util.CacheRelation(v)
// 				case *pgoutput.Begin:
// 					err = util.HandleBegin(v)
// 				case *pgoutput.Commit:
// 					err = util.HandleCommit(v)
// 					// Suppose we have written updates to the DB.
// 					err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(v.LSN)})
// 					if err != nil {
// 						log.Println("ERROR SENDING ACK:", pglogrepl.LSN(v.LSN))
// 					} else {
// 						log.Println("ACK DELIVERED:", pglogrepl.LSN(v.LSN))
// 					}
// 				case *pgoutput.Delete:
// 					err = util.HandleDelete(v)
// 				case *pgoutput.Insert:
// 					err = util.HandleInsert(v)
// 				case *pgoutput.Update:
// 					err = util.HandleUpdate(v)
// 					// log.Println(err)
// 				default:
// 					f := pretty.Sprint(v)
// 					f = f
// 					log.Println("Not supported:", f)
// 				}

// 				// log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", tryParse(xld.WALData))

// 				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
// 			default:
// 				log.Println("Something else:", msg.Data[0])
// 			}
// 		default:
// 			log.Printf("ERROR: Received unexpected message: %#v\n", msg)
// 		}

// 	}
// }
