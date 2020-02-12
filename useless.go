package main

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
