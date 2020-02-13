package main

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

/////////////////////////////////////

// func tryParse(data []byte) string {
// 	return "DONE"
// 	log.Println("FIRST BYTE:", data[0], "STR:", string(data[0]))

// 	switch data[0] {
// 	case 'B':
// 		log.Println("BEGIN")
// 		log.Println("Final LSN of txn:", pglogrepl.LSN(binary.BigEndian.Uint64(data[1:9])).String())
// 	case 'R':
// 		cur := 1
// 		log.Println("RELATION")
// 		log.Println("Rel ID:", binary.BigEndian.Uint32(data[1:5]))
// 		str, n := parseString(data[5:])
// 		cur = n + 5
// 		log.Println("Namespace:", str)
// 		str, n = parseString(data[cur:])
// 		cur += n
// 		log.Println("Namespace:", str)
// 		log.Println("relreplident:", string(uint8(data[cur])))
// 		cur += 1
// 		numCols := binary.BigEndian.Uint16(data[cur : cur+2])
// 		cur += 2
// 		log.Println("Num cols:", numCols)

// 		for i := uint16(0); i < numCols; i++ {
// 			log.Println("== Column", i+1, "==")
// 			log.Println("Col part of key:", uint8(data[cur]))
// 			cur++
// 			str, n = parseString(data[cur:])
// 			cur += n
// 			log.Println("Col name:", str)
// 			log.Println("Col type ID:", binary.BigEndian.Uint32(data[cur:cur+5]))
// 			cur += 5
// 			log.Println("atttypmod:", binary.BigEndian.Uint32(data[cur:cur+5]))
// 			cur += 5
// 		}

// 	case 'I':
// 		log.Println("INSERT")
// 		log.Println("Rel ID:", binary.BigEndian.Uint32(data[1:5]))
// 		log.Println("New tuple?:", string(data[5]))

// 		cur := 6
// 		numCols := binary.BigEndian.Uint16(data[cur : cur+2])
// 		cur += 2

// 		for i := uint16(0); i < numCols; i++ {
// 			log.Println("== TupleData: == ")

// 			switch data[cur] {
// 			case 'n':
// 			case 'u':
// 			case 't':
// 				cur++
// 				vsize := binary.BigEndian.Uint32(data[cur : cur+4])
// 				cur += 4
// 				log.Println("  Tup Size:", vsize)
// 				b := data[cur : cur+int(vsize)]
// 				log.Println("  Tup Data:", b, "str:", string(b))
// 				cur += int(vsize)
// 			}

// 		}

// 	case 'C':
// 		log.Println("COMMIT")
// 		log.Println("LSN of commit:", pglogrepl.LSN(binary.BigEndian.Uint64(data[2:10])).String())
// 		log.Println("LSN of txn:", pglogrepl.LSN(binary.BigEndian.Uint64(data[10:18])).String())
// 	}

// 	return string(data)
// }

// func parseString(data []byte) (string, int) {
// 	s := 0
// 	for {
// 		if s > len(data) {
// 			return "", -1
// 		}
// 		if data[s] == byte(0) {
// 			return string(data[:s]), s + 1
// 		}
// 		s++
// 	}
// }
