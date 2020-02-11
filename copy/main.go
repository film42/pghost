package main

import (
	"fmt"
	"os"
	"log"
	"runtime/pprof"
)

func main() {
	f, err := os.Create("/tmp/cpu_prof_8.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	cb := &BatchCopyBoy{
		SourceTable:      "yolos3",
		DestinationTable: "yolos2",
	}
	cb = cb
	// ./copy  2.34s user 1.38s system 19% cpu 19.259 total
	// ./copy  2.63s user 1.49s system 14% cpu 29.401 total
	// ./copy  2.23s user 1.37s system 17% cpu 20.983 total
	// ./copy  2.24s user 1.35s system 13% cpu 26.438 total
	// ./copy  2.12s user 1.27s system 19% cpu 17.268 total
	// fmt.Println(cb.CopyInBatches(100000, 10))

	// ./copy  2.23s user 1.73s system 11% cpu 34.379 total
	// fmt.Println(cb.CopyInBatches(100000, 2))

	// ./copy  1.10s user 1.00s system 4% cpu 45.838 total
	// fmt.Println(cb.CopyInBatches(100000000, 1))

	// fmt.Println(cb.CopyInBatches(100000, 1))


	// 10 workers.
	// &main.copyWithPq{minId:1, maxId:10000406, BatchSize:100000, SourceTable:"yolos", DestinationTable:"yolos2"}
    // ./copy  326.65s user 22.58s system 508% cpu 1:08.70 total
	// Without the busy "default:" loop killing cpu:
	// &main.copyWithPq{minId:1, maxId:10000406, BatchSize:100000, SourceTable:"yolos", DestinationTable:"yolos2"}
	// ./copy  30.83s user 23.68s system 193% cpu 28.156 total
	// With abortCopyChan select removed
	// ./copy  26.56s user 19.58s system 191% cpu 24.081 total
	// ./copy  30.07s user 1.11s system 145% cpu 21.426 total
	cpq := &copyWithPq{
		SourceTable: "yolos",
		DestinationTable: "yolos2",
		BatchSize: 100000,
	}
	cpq = cpq

	// Even this is using 270% to copy one worker. Why? That's insane.
	fmt.Println(cpq.CopyUsingPq(10))
	// fmt.Println(cpq.CopyUsingPq(1))

	fmt.Println("Done!")
}
