package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	zydecodb "github.com/dataparade/zydecodb/clients/go"
)

// LogEvent represents an append-only log entry
type LogEvent struct {
	Level     string `json:"level"`
	Message   string `json:"message"`
	Service   string `json:"service"`
	Timestamp int64  `json:"timestamp"`
	TraceID   string `json:"trace_id"`
}

var (
	levels   = []string{"INFO", "WARN", "ERROR", "DEBUG"}
	services = []string{"auth", "billing", "api", "worker"}
)

func main() {
	workers := flag.Int("workers", 10, "number of concurrent writers")
	duration := flag.Duration("duration", 10*time.Second, "how long to run")
	relaxed := flag.Bool("relaxed", false, "use relaxed durability (skip fsync)")
	flag.Parse()

	addr := "127.0.0.1:9470"
	db := zydecodb.NewClient(addr, zydecodb.WithMaxRetries(20))
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("ZydecoDB not reachable at %s: %v", addr, err)
	}
	cancel()

	logs := db.Collection("logs")
	ctx = context.Background()

	// Ensure indexes for querying later if needed
	_, err := logs.CreateIndex(ctx, []string{"service"}, false)
	if err != nil {
		log.Fatalf("Failed to create index: %v", err)
	}
	_, err = logs.CreateIndex(ctx, []string{"level"}, false)
	if err != nil {
		log.Fatalf("Failed to create index: %v", err)
	}

	fmt.Printf("Starting write-heavy test: %d workers for %v (relaxed=%v)\n", *workers, *duration, *relaxed)

	var (
		totalWrites uint64
		totalErrors uint64
		wg          sync.WaitGroup
	)

	start := time.Now()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Give each worker its own random source
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			timeout := time.After(*duration)
			for {
				select {
				case <-timeout:
					return
				default:
					doc := zydecodb.Document{
						"level":     levels[r.Intn(len(levels))],
						"message":   fmt.Sprintf("Event occurred in system %d", r.Intn(1000)),
						"service":   services[r.Intn(len(services))],
						"timestamp": time.Now().UnixMilli(),
						"trace_id":  fmt.Sprintf("trace-%d-%d", workerID, r.Intn(1000000)),
					}

					// 5s timeout per write
					wCtx, wCancel := context.WithTimeout(ctx, 5*time.Second)
					_, err := logs.InsertOne(wCtx, doc, *relaxed, 0)
					wCancel()

					if err != nil {
						atomic.AddUint64(&totalErrors, 1)
					} else {
						atomic.AddUint64(&totalWrites, 1)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	writes := atomic.LoadUint64(&totalWrites)
	errs := atomic.LoadUint64(&totalErrors)

	fmt.Printf("\n--- Results ---\n")
	fmt.Printf("Duration:    %v\n", elapsed)
	fmt.Printf("Total writes:%d\n", writes)
	fmt.Printf("Errors:      %d\n", errs)
	fmt.Printf("Throughput:  %.2f writes/sec\n", float64(writes)/elapsed.Seconds())

	// Check how many actually made it
	count, err := logs.CountDocuments(ctx, nil)
	if err != nil {
		log.Printf("Failed to count docs: %v", err)
	} else {
		fmt.Printf("Doc count:   %d\n", count)
	}
}
