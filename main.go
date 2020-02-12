package main

import (
	"context"
	"log"
	"time"

	// "encoding/binary"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/kr/pretty"

	"github.com/film42/pghost/pglogrepl"
	"github.com/film42/pghost/pgoutput"
)

func main() {
	conn, err := pgconn.Connect(context.Background(), "dbname=postgres replication=database")
	if err != nil {
		log.Fatalln("failed to connect to PostgreSQL server:", err)
	}
	defer conn.Close(context.Background())

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	slotName := "yolo12300000xyz"

	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	if err != nil {
		log.Fatalln("CreateReplicationSlot failed:", err)
	}
	log.Println("Created temporary replication slot:", slotName)

	time.Sleep(time.Second * 5)

	err = pglogrepl.StartReplication(context.Background(), conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{}, "(proto_version '1', publication_names 'my_pub')")
	if err != nil {
		log.Fatalln("StartReplication failed:", err)
	}
	log.Println("Logical replication started on slot", slotName)

	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	util := NewPgOutputUtil()

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			// log.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalln("ReceiveMessage failed:", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
				}
				// log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Fatalln("ParseXLogData failed:", err)
				}

				r, err := pgoutput.Parse(xld.WALData)
				if err != nil {
					// log.Println("ERROR GETTING THE WAL DATA PARSED:", err)
				}
				// log.Println(xld.WALStart, xld.ServerWALEnd, xld.ServerTime, pretty.Sprint(r))

				// Debug.
				switch v := r.(type) {
				case *pgoutput.Relation:
					// log.Println(pretty.Sprint(v))
					util.CacheRelation(v)
				case *pgoutput.Delete:
					err = util.HandleDelete(v)
				case *pgoutput.Insert:
					err = util.HandleInsert(v)
				case *pgoutput.Update:
					err = util.HandleUpdate(v)
					// log.Println(err)
				default:
					f := pretty.Sprint(v)
					f = f
					// log.Println("Not supported:", f)
				}

				// log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", tryParse(xld.WALData))

				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			default:
				log.Println("Something else:", msg.Data[0])
			}
		default:
			log.Printf("ERROR: Received unexpected message: %#v\n", msg)
		}

	}
}
