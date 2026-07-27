package zydecodb

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"testing"
	"time"
)

// Integration tests against a live ZydecoDB server. Set ZYDECODB_TEST_HOST /
// ZYDECODB_TEST_PORT (and optionally ZYDECODB_TEST_API_KEY) to point at a
// running server; the suite is skipped when the server is unreachable, so
// `go test ./...` stays green offline.

func testAddr() string {
	host := os.Getenv("ZYDECODB_TEST_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("ZYDECODB_TEST_PORT")
	if port == "" {
		port = "9470"
	}
	return net.JoinHostPort(host, port)
}

func liveClient(t *testing.T) *Client {
	t.Helper()
	addr := testAddr()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Skipf("no ZydecoDB server at %s: %v", addr, err)
	}
	_ = c.Close()
	var opts []Option
	if key := os.Getenv("ZYDECODB_TEST_API_KEY"); key != "" {
		opts = append(opts, WithAPIKey(key))
	}
	return NewClient(addr, opts...)
}

func uniqueCollection() string {
	return fmt.Sprintf("gotest_%d", time.Now().UnixNano())
}

func TestLivePing(t *testing.T) {
	c := liveClient(t)
	defer c.Close()
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestKV(t *testing.T) {
	db := liveClient(t)
	defer db.Close()
	ctx := context.Background()

	key := []byte(fmt.Sprintf("testkv_%d", time.Now().UnixNano()))
	val := []byte("hello kv")

	// Get missing
	res, err := db.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Fatalf("expected nil, got %q", res)
	}

	// Put
	seq, err := db.Put(ctx, key, val, 0)
	if err != nil {
		t.Fatal(err)
	}
	if seq == 0 {
		t.Fatal("expected positive seq")
	}

	// Get
	res, err = db.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(res) != "hello kv" {
		t.Fatalf("got %q, want hello kv", res)
	}

	// Delete
	existed, err := db.Delete(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !existed {
		t.Fatal("expected true")
	}
	existed, err = db.Delete(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if existed {
		t.Fatal("expected false")
	}

	// Put with TTL (expired)
	expired := uint64(time.Now().Add(-time.Hour).UnixMilli())
	_, err = db.Put(ctx, key, val, expired)
	if err != nil {
		t.Fatal(err)
	}
	res, err = db.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Fatalf("expected nil for expired key, got %q", res)
	}
}

func TestLiveInsertFindUpdateDelete(t *testing.T) {
	c := liveClient(t)
	defer c.Close()
	ctx := context.Background()
	coll := c.Collection(uniqueCollection())

	if _, err := coll.CreateIndex(ctx, []string{"age"}, false, 0); err != nil {
		t.Fatalf("create index: %v", err)
	}
	ids, err := coll.InsertMany(ctx, []Document{
		{"name": "Ada", "age": 30, "city": "London"},
		{"name": "Bo", "age": 25, "city": "NOLA"},
		{"name": "Cy", "age": 40, "city": "NOLA"},
	})
	if err != nil {
		t.Fatalf("insert many: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 ids, got %d", len(ids))
	}

	got, err := coll.Find(ctx, Document{"age": Document{"$gte": 30}}, QueryOptions{
		Sort: []SortKey{{Field: "age", Ascending: true}},
	})
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	var names []string
	for _, d := range got {
		names = append(names, d["name"].(string))
	}
	if len(names) != 2 || names[0] != "Ada" || names[1] != "Cy" {
		t.Fatalf("unexpected find result: %v", names)
	}

	res, err := coll.UpdateOne(ctx, Document{"name": "Bo"}, Document{"$inc": Document{"age": 10}}, false, false)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if res.Matched != 1 || res.Modified != 1 {
		t.Fatalf("unexpected update result: %+v", res)
	}

	cnt, err := coll.CountDocuments(ctx, nil)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if cnt != 3 {
		t.Fatalf("expected count 3, got %d", cnt)
	}

	cities, err := coll.Distinct(ctx, "city", nil)
	if err != nil {
		t.Fatalf("distinct: %v", err)
	}
	strs := make([]string, 0, len(cities))
	for _, v := range cities {
		strs = append(strs, v.(string))
	}
	sort.Strings(strs)
	if len(strs) != 2 || strs[0] != "London" || strs[1] != "NOLA" {
		t.Fatalf("unexpected distinct: %v", strs)
	}

	deleted, err := coll.DeleteMany(ctx, Document{"city": "NOLA"}, false)
	if err != nil {
		t.Fatalf("delete many: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deleted, got %d", deleted)
	}
}

func TestLiveUniqueIndexConflict(t *testing.T) {
	c := liveClient(t)
	defer c.Close()
	ctx := context.Background()
	coll := c.Collection(uniqueCollection())

	if _, err := coll.CreateIndex(ctx, []string{"email"}, true, 0); err != nil {
		t.Fatalf("create unique index: %v", err)
	}
	if _, err := coll.InsertOne(ctx, Document{"email": "a@b.com"}, false, 0); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, err := coll.InsertOne(ctx, Document{"email": "a@b.com"}, false, 0)
	if err == nil || !IsConflict(err) {
		t.Fatalf("expected conflict error, got %v", err)
	}
}

func TestLiveUpsertSetOnInsert(t *testing.T) {
	c := liveClient(t)
	defer c.Close()
	ctx := context.Background()
	coll := c.Collection(uniqueCollection())

	miss, err := coll.UpdateOne(ctx,
		Document{"email": "soi@example.com"},
		Document{
			"$set":         Document{"email": "soi@example.com", "n": 1.0},
			"$setOnInsert": Document{"created": true},
		},
		false, true,
	)
	if err != nil {
		t.Fatalf("upsert miss: %v", err)
	}
	if miss.Matched != 0 || miss.Modified != 0 || miss.UpsertedID == "" {
		t.Fatalf("unexpected miss result: %+v", miss)
	}
	doc, err := coll.FindOne(ctx, Document{"email": "soi@example.com"}, QueryOptions{})
	if err != nil {
		t.Fatalf("find after upsert: %v", err)
	}
	if doc["created"] != true || doc["n"].(float64) != 1 {
		t.Fatalf("unexpected doc after insert: %#v", doc)
	}

	hit, err := coll.UpdateOne(ctx,
		Document{"email": "soi@example.com"},
		Document{
			"$set":         Document{"n": 2.0},
			"$setOnInsert": Document{"created": false, "extra": 1.0},
		},
		false, true,
	)
	if err != nil {
		t.Fatalf("upsert hit: %v", err)
	}
	if hit.Matched != 1 || hit.Modified != 1 || hit.UpsertedID != "" {
		t.Fatalf("unexpected hit result: %+v", hit)
	}
	doc, err = coll.FindOne(ctx, Document{"email": "soi@example.com"}, QueryOptions{})
	if err != nil {
		t.Fatalf("find after hit: %v", err)
	}
	if doc["n"].(float64) != 2 || doc["created"] != true {
		t.Fatalf("setOnInsert must be ignored on hit: %#v", doc)
	}
	if _, ok := doc["extra"]; ok {
		t.Fatalf("extra must not appear on hit: %#v", doc)
	}
}

func TestLiveOptimisticConcurrency(t *testing.T) {
	c := liveClient(t)
	defer c.Close()
	ctx := context.Background()
	coll := c.Collection(uniqueCollection())

	id, err := coll.InsertOne(ctx, Document{"n": 1.0}, false, 0)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err := coll.GetWithRevision(ctx, id)
	if err != nil || got.Doc == nil {
		t.Fatalf("get with revision: %v %#v", err, got)
	}
	if got.Revision == 0 {
		t.Fatal("expected non-zero revision")
	}
	newRev, err := coll.ReplaceOneIfMatch(ctx, id, Document{"n": 2.0}, false, got.Revision, 0)
	if err != nil {
		t.Fatalf("replace if match: %v", err)
	}
	if newRev <= got.Revision {
		t.Fatalf("expected newer revision, got %d after %d", newRev, got.Revision)
	}
	_, err = coll.ReplaceOneIfMatch(ctx, id, Document{"n": 3.0}, false, got.Revision, 0)
	if !IsConflict(err) {
		t.Fatalf("expected conflict on stale replace, got %v", err)
	}
	after, err := coll.UpdateByIDIfMatch(ctx, id, Document{"$inc": Document{"n": 1.0}}, false, newRev)
	if err != nil {
		t.Fatalf("update if match: %v", err)
	}
	if after <= newRev {
		t.Fatalf("expected newer revision after update, got %d", after)
	}
}

func TestLiveBoundedTransaction(t *testing.T) {
	c := liveClient(t)
	defer c.Close()
	ctx := context.Background()
	collName := uniqueCollection()
	coll := c.Collection(collName)
	if _, err := coll.InsertOne(ctx, Document{"_seed": true}, false, 0); err != nil {
		t.Fatalf("seed collection: %v", err)
	}

	kvKey := []byte("tx-go-" + collName)
	seq, err := c.WithTransaction(ctx, func(tx *Tx) error {
		if err := tx.Put(ctx, kvKey, []byte("v1"), 0); err != nil {
			return err
		}
		if err := tx.PutDocument(ctx, collName, "u1", Document{"n": 1.0}); err != nil {
			return err
		}
		got, err := tx.Get(ctx, kvKey)
		if err != nil {
			return err
		}
		if string(got) != "v1" {
			t.Fatalf("read-your-writes: got %q", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("transaction: %v", err)
	}
	if seq == 0 {
		t.Fatal("expected non-zero commit seq")
	}
	val, err := c.Get(ctx, kvKey)
	if err != nil || string(val) != "v1" {
		t.Fatalf("committed kv: %v %q", err, val)
	}

	// Rollback path
	tx, err := c.BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Put(ctx, []byte("tx-rollback"), []byte("nope"), 0); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	gone, err := c.Get(ctx, []byte("tx-rollback"))
	if err != nil || gone != nil {
		t.Fatalf("rollback must leave nothing: %v %q", err, gone)
	}
}
