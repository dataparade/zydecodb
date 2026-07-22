// Quickstart: connect, index, insert, query, update, and delete documents.
//
// Run against a local server (default 127.0.0.1:9470):
//
//	go run ./examples/quickstart
//
// Override the address / API key with ZYDECODB_ADDR and ZYDECODB_API_KEY.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	zydecodb "github.com/dataparade/zydecodb/clients/go"
)

func main() {
	addr := envOr("ZYDECODB_ADDR", "127.0.0.1:9470")
	var opts []zydecodb.Option
	if key := os.Getenv("ZYDECODB_API_KEY"); key != "" {
		opts = append(opts, zydecodb.WithAPIKey(key))
	}

	client := zydecodb.NewClient(addr, opts...)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx); err != nil {
		log.Fatalf("server not reachable at %s: %v", addr, err)
	}

	users := client.Collection(fmt.Sprintf("quickstart_%d", time.Now().Unix()))

	if _, err := users.CreateIndex(ctx, []string{"age"}, false); err != nil {
		log.Fatalf("create index: %v", err)
	}

	ids, err := users.InsertMany(ctx, []zydecodb.Document{
		{"name": "Ada", "age": 30, "city": "London"},
		{"name": "Bo", "age": 25, "city": "NOLA"},
		{"name": "Cy", "age": 40, "city": "NOLA"},
	})
	if err != nil {
		log.Fatalf("insert: %v", err)
	}
	fmt.Printf("inserted %d users: %v\n", len(ids), ids)

	adults, err := users.Find(ctx, zydecodb.Document{"age": zydecodb.Document{"$gte": 30}},
		zydecodb.QueryOptions{Sort: []zydecodb.SortKey{{Field: "age", Ascending: true}}})
	if err != nil {
		log.Fatalf("find: %v", err)
	}
	fmt.Println("adults (age >= 30):")
	for _, u := range adults {
		fmt.Printf("  %v (%v)\n", u["name"], u["age"])
	}

	res, err := users.UpdateMany(ctx, zydecodb.Document{"city": "NOLA"},
		zydecodb.Document{"$set": zydecodb.Document{"region": "South"}}, false, false)
	if err != nil {
		log.Fatalf("update: %v", err)
	}
	fmt.Printf("tagged %d NOLA users as South\n", res.Modified)

	cities, err := users.Distinct(ctx, "city", nil)
	if err != nil {
		log.Fatalf("distinct: %v", err)
	}
	fmt.Printf("distinct cities: %v\n", cities)

	deleted, err := users.DeleteMany(ctx, zydecodb.Document{"age": zydecodb.Document{"$lt": 28}}, false)
	if err != nil {
		log.Fatalf("delete: %v", err)
	}
	fmt.Printf("deleted %d users under 28\n", deleted)

	total, err := users.CountDocuments(ctx, nil)
	if err != nil {
		log.Fatalf("count: %v", err)
	}
	fmt.Printf("remaining users: %d\n", total)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
