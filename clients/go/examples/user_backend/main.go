// A small HTTP backend (a users API) on top of the ZydecoDB Go client. It shows
// the client driving a real request/response cycle: one shared, pooled Client
// handles concurrent HTTP requests.
//
// Run against a local server (default 127.0.0.1:9470):
//
//	go run ./examples/user_backend
//
// Then:
//
//	curl -s localhost:8080/users -d '{"name":"Ada","email":"ada@x.io","age":30}'
//	curl -s 'localhost:8080/users?min_age=18'
//	curl -s localhost:8080/users/<id>
//	curl -s -X PATCH localhost:8080/users/<id> -d '{"age":31}'
//	curl -s -X DELETE localhost:8080/users/<id>
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	zydecodb "github.com/dataparade/paradekv/clients/go"
)

const collection = "app_users"

type server struct {
	db   *zydecodb.Client
	coll *zydecodb.Collection
}

func main() {
	addr := envOr("ZYDECODB_ADDR", "127.0.0.1:9470")
	var opts []zydecodb.Option
	if key := os.Getenv("ZYDECODB_API_KEY"); key != "" {
		opts = append(opts, zydecodb.WithAPIKey(key))
	}
	db := zydecodb.NewClient(addr, opts...)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("ZydecoDB not reachable at %s: %v", addr, err)
	}
	srv := &server{db: db, coll: db.Collection(collection)}
	// A unique email per user — enforced by the database, not the app.
	if _, err := srv.coll.CreateIndex(ctx, []string{"email"}, true); err != nil {
		log.Fatalf("create index: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/users", srv.handleUsers)
	mux.HandleFunc("/users/", srv.handleUserByID)
	mux.HandleFunc("/login", srv.handleLogin)
	mux.HandleFunc("/me", srv.handleMe)

	listen := envOr("LISTEN_ADDR", ":8080")
	log.Printf("user_backend listening on %s (db %s)", listen, addr)
	httpSrv := &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(httpSrv.ListenAndServe())
}

// POST /users  -> create; GET /users?min_age=N -> list
func (s *server) handleUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.createUser(w, r)
	case http.MethodGet:
		s.listUsers(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// GET/PATCH/DELETE /users/{id}
func (s *server) handleUserByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/users/")
	if id == "" || strings.Contains(id, "/") {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.getUser(w, r, id)
	case http.MethodPatch:
		s.patchUser(w, r, id)
	case http.MethodDelete:
		s.deleteUser(w, r, id)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) createUser(w http.ResponseWriter, r *http.Request) {
	var doc zydecodb.Document
	if err := decodeJSON(r, &doc); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, ok := doc["email"].(string); !ok {
		http.Error(w, "email is required", http.StatusBadRequest)
		return
	}
	id, err := s.coll.InsertOne(r.Context(), doc, false)
	if err != nil {
		if zydecodb.IsConflict(err) {
			http.Error(w, "email already exists", http.StatusConflict)
			return
		}
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

func (s *server) listUsers(w http.ResponseWriter, r *http.Request) {
	filter := zydecodb.Document{}
	if v := r.URL.Query().Get("min_age"); v != "" {
		age, err := strconv.Atoi(v)
		if err != nil {
			http.Error(w, "min_age must be an integer", http.StatusBadRequest)
			return
		}
		filter["age"] = zydecodb.Document{"$gte": age}
	}
	users, err := s.coll.Find(r.Context(), filter, zydecodb.QueryOptions{
		Sort:  []zydecodb.SortKey{{Field: "age", Ascending: true}},
		Limit: 100,
	})
	if err != nil {
		serverError(w, err)
		return
	}
	if users == nil {
		users = []zydecodb.Document{}
	}
	writeJSON(w, http.StatusOK, users)
}

func (s *server) getUser(w http.ResponseWriter, r *http.Request, id string) {
	doc, err := s.coll.Get(r.Context(), id)
	if err != nil {
		serverError(w, err)
		return
	}
	if doc == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

func (s *server) patchUser(w http.ResponseWriter, r *http.Request, id string) {
	var fields zydecodb.Document
	if err := decodeJSON(r, &fields); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	delete(fields, "_id")
	if len(fields) == 0 {
		http.Error(w, "no fields to update", http.StatusBadRequest)
		return
	}
	res, err := s.coll.UpdateOne(r.Context(), zydecodb.Document{"_id": id},
		zydecodb.Document{"$set": fields}, false)
	if err != nil {
		if zydecodb.IsConflict(err) {
			http.Error(w, "email already exists", http.StatusConflict)
			return
		}
		serverError(w, err)
		return
	}
	if res.Matched == 0 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *server) deleteUser(w http.ResponseWriter, r *http.Request, id string) {
	deleted, err := s.coll.DeleteOne(r.Context(), zydecodb.Document{"_id": id}, false)
	if err != nil {
		serverError(w, err)
		return
	}
	if deleted == 0 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var creds struct {
		Email string `json:"email"`
	}
	if err := decodeJSON(r, &creds); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	docs, err := s.coll.Find(r.Context(), zydecodb.Document{"email": creds.Email}, zydecodb.QueryOptions{Limit: 1})
	if err != nil {
		serverError(w, err)
		return
	}
	if len(docs) == 0 {
		http.Error(w, "invalid email", http.StatusUnauthorized)
		return
	}
	id, _ := docs[0]["_id"].(string)

	var b [32]byte
	rand.Read(b[:])
	token := hex.EncodeToString(b[:])
	
	expiresAt := uint64(time.Now().Add(24 * time.Hour).UnixMilli())
	_, err = s.db.Put(r.Context(), []byte("session:"+token), []byte(id), expiresAt)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

func (s *server) handleMe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		http.Error(w, "missing bearer token", http.StatusUnauthorized)
		return
	}
	token := strings.TrimPrefix(auth, "Bearer ")
	
	idBytes, err := s.db.Get(r.Context(), []byte("session:"+token))
	if err != nil {
		serverError(w, err)
		return
	}
	if idBytes == nil {
		http.Error(w, "invalid or expired token", http.StatusUnauthorized)
		return
	}
	
	doc, err := s.coll.Get(r.Context(), string(idBytes))
	if err != nil {
		serverError(w, err)
		return
	}
	if doc == nil {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return errors.New("invalid JSON body")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func serverError(w http.ResponseWriter, err error) {
	log.Printf("error: %v", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
