package zydecodb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// Document is a JSON document: a map keyed by field name. The "_id" field is
// the document's string primary key.
type Document = map[string]any

// Collection is the product surface: a MongoDB-inspired collection of JSON
// documents over the binary client. Filters and updates use the familiar
// $-operators; the server plans the access path and re-checks the full filter.
type Collection struct {
	client *Client
	name   string
}

// Name returns the collection name.
func (c *Collection) Name() string { return c.name }

// CreateIndex creates a secondary index over one or more dotted field paths.
// It returns false if the index already existed.
func (c *Collection) CreateIndex(ctx context.Context, fields []string, unique bool) (bool, error) {
	parts := make([]string, len(fields))
	for i, f := range fields {
		parts[i] = strings.ReplaceAll(f, ".", "_")
	}
	name := "by_" + strings.Join(parts, "_")
	return c.client.DefineIndex(ctx, c.name, name, fields, unique, true)
}

// InsertOne inserts a document, generating "_id" if absent, and returns the id.
// It returns an error (see IsConflict) if a unique index would be violated.
func (c *Collection) InsertOne(ctx context.Context, doc Document, relaxed bool) (string, error) {
	id, _ := doc["_id"].(string)
	if id == "" {
		id = GenerateID()
	}
	cp := make(Document, len(doc)+1)
	for k, v := range doc {
		cp[k] = v
	}
	cp["_id"] = id
	body, err := json.Marshal(cp)
	if err != nil {
		return "", fmt.Errorf("zydecodb: marshal document: %w", err)
	}
	if _, err := c.client.PutDocument(ctx, c.name, id, body, relaxed); err != nil {
		return "", err
	}
	return id, nil
}

// InsertMany inserts each document in order, returning the assigned ids.
func (c *Collection) InsertMany(ctx context.Context, docs []Document) ([]string, error) {
	ids := make([]string, 0, len(docs))
	for _, d := range docs {
		id, err := c.InsertOne(ctx, d, false)
		if err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ReplaceOne inserts or fully replaces the document at docID.
func (c *Collection) ReplaceOne(ctx context.Context, docID string, doc Document, relaxed bool) (uint64, error) {
	cp := make(Document, len(doc)+1)
	for k, v := range doc {
		cp[k] = v
	}
	cp["_id"] = docID
	body, err := json.Marshal(cp)
	if err != nil {
		return 0, fmt.Errorf("zydecodb: marshal document: %w", err)
	}
	return c.client.PutDocument(ctx, c.name, docID, body, relaxed)
}

// UpdateResult summarizes an update operation.
type UpdateResult struct {
	Matched  int64 `json:"matched"`
	Modified int64 `json:"modified"`
}

func (c *Collection) update(ctx context.Context, filter, update Document, multi, relaxed bool) (UpdateResult, error) {
	var res UpdateResult
	fb, err := marshalFilter(filter)
	if err != nil {
		return res, err
	}
	ub, err := json.Marshal(update)
	if err != nil {
		return res, fmt.Errorf("zydecodb: marshal update: %w", err)
	}
	body, err := c.client.Update(ctx, c.name, fb, ub, multi, relaxed)
	if err != nil {
		return res, err
	}
	if err := json.Unmarshal(body, &res); err != nil {
		return res, fmt.Errorf("zydecodb: decode update result: %w", err)
	}
	return res, nil
}

// UpdateOne applies update to the first matching document.
func (c *Collection) UpdateOne(ctx context.Context, filter, update Document, relaxed bool) (UpdateResult, error) {
	return c.update(ctx, filter, update, false, relaxed)
}

// UpdateMany applies update to all matching documents.
func (c *Collection) UpdateMany(ctx context.Context, filter, update Document, relaxed bool) (UpdateResult, error) {
	return c.update(ctx, filter, update, true, relaxed)
}

// DeleteOne deletes the first matching document and returns the deleted count.
func (c *Collection) DeleteOne(ctx context.Context, filter Document, relaxed bool) (int64, error) {
	fb, err := marshalFilter(filter)
	if err != nil {
		return 0, err
	}
	return c.client.DeleteByFilter(ctx, c.name, fb, false, relaxed)
}

// DeleteMany deletes all matching documents and returns the deleted count.
func (c *Collection) DeleteMany(ctx context.Context, filter Document, relaxed bool) (int64, error) {
	fb, err := marshalFilter(filter)
	if err != nil {
		return 0, err
	}
	return c.client.DeleteByFilter(ctx, c.name, fb, true, relaxed)
}

// QueryOptions tunes a Find at the collection level.
type QueryOptions struct {
	Sort     []SortKey
	Include  []string // include only these fields (mutually exclusive with Exclude)
	Exclude  []string // exclude these fields
	Skip     uint32
	Limit    uint32
	PageSize uint32
}

func (q QueryOptions) projection() (Projection, error) {
	if len(q.Include) > 0 && len(q.Exclude) > 0 {
		return Projection{}, fmt.Errorf("zydecodb: projection cannot mix include and exclude fields")
	}
	if len(q.Include) > 0 {
		return Projection{Mode: ProjInclude, Fields: q.Include}, nil
	}
	if len(q.Exclude) > 0 {
		return Projection{Mode: ProjExclude, Fields: q.Exclude}, nil
	}
	return Projection{Mode: ProjNone}, nil
}

// Find returns all matching documents, decoded as Documents.
func (c *Collection) Find(ctx context.Context, filter Document, opts QueryOptions) ([]Document, error) {
	fb, err := marshalFilter(filter)
	if err != nil {
		return nil, err
	}
	proj, err := opts.projection()
	if err != nil {
		return nil, err
	}
	bodies, err := c.client.Find(ctx, c.name, fb, FindOptions{
		Sort:       opts.Sort,
		Projection: proj,
		Skip:       opts.Skip,
		Limit:      opts.Limit,
		PageSize:   opts.PageSize,
	})
	if err != nil {
		return nil, err
	}
	out := make([]Document, 0, len(bodies))
	for _, b := range bodies {
		doc := Document{}
		if len(b) > 0 {
			if err := json.Unmarshal(b, &doc); err != nil {
				return nil, fmt.Errorf("zydecodb: decode document: %w", err)
			}
		}
		out = append(out, doc)
	}
	return out, nil
}

// FindOne returns the first matching document, or (nil, nil) if none match.
func (c *Collection) FindOne(ctx context.Context, filter Document, opts QueryOptions) (Document, error) {
	opts.Limit = 1
	docs, err := c.Find(ctx, filter, opts)
	if err != nil || len(docs) == 0 {
		return nil, err
	}
	return docs[0], nil
}

// Get fetches one document directly by id (fast path), or (nil, nil) if absent.
func (c *Collection) Get(ctx context.Context, docID string) (Document, error) {
	body, err := c.client.GetDocument(ctx, c.name, docID)
	if err != nil || body == nil {
		return nil, err
	}
	doc := Document{}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("zydecodb: decode document: %w", err)
	}
	return doc, nil
}

// CountDocuments returns the number of documents matching filter (nil = all).
func (c *Collection) CountDocuments(ctx context.Context, filter Document) (int64, error) {
	fb, err := marshalFilter(filter)
	if err != nil {
		return 0, err
	}
	return c.client.Count(ctx, c.name, fb)
}

// Distinct returns the distinct values of field across matching documents.
func (c *Collection) Distinct(ctx context.Context, field string, filter Document) ([]any, error) {
	fb, err := marshalFilter(filter)
	if err != nil {
		return nil, err
	}
	return c.client.Distinct(ctx, c.name, field, fb)
}

// marshalFilter serializes a filter, treating a nil/empty filter as "match all"
// (empty bytes on the wire).
func marshalFilter(filter Document) ([]byte, error) {
	if len(filter) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(filter)
	if err != nil {
		return nil, fmt.Errorf("zydecodb: marshal filter: %w", err)
	}
	return b, nil
}
