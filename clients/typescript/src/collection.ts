import type { Client, UpdateResult } from "./client.ts";
import { generateId } from "./client.ts";
import { ZydecoError } from "./errors.ts";
import { Proj, type Projection, type SortKey } from "./protocol.ts";

/** A JSON document. The "_id" field is the document's string primary key. */
export type Document = Record<string, unknown>;

export interface QueryOptions {
  sort?: SortKey[];
  /** Include only these fields (mutually exclusive with `exclude`). */
  include?: string[];
  /** Exclude these fields. */
  exclude?: string[];
  skip?: number;
  limit?: number;
  pageSize?: number;
}

const EMPTY = Buffer.alloc(0);

/**
 * The product surface: a collection of JSON documents over the
 * binary client. Filters and updates use the familiar $-operators; the server
 * plans the access path and re-checks the full filter.
 */
export class Collection {
  private readonly client: Client;
  readonly name: string;

  constructor(client: Client, name: string) {
    this.client = client;
    this.name = name;
  }

  /**
   * Create a secondary index over one or more dotted field paths. Returns false
   * if the index already existed.
   */
  createIndex(fields: string[], unique = false): Promise<boolean> {
    const indexName = "by_" + fields.map((f) => f.replaceAll(".", "_")).join("_");
    return this.client.defineIndex(this.name, indexName, fields, unique);
  }

  /** Insert a document, generating "_id" if absent. Returns the id. */
  async insertOne(document: Document, relaxed = false): Promise<string> {
    const id = typeof document._id === "string" && document._id ? document._id : generateId();
    const doc = { ...document, _id: id };
    await this.client.putDocument(this.name, id, jsonBytes(doc), relaxed);
    return id;
  }

  async insertMany(documents: Document[]): Promise<string[]> {
    const ids: string[] = [];
    for (const d of documents) ids.push(await this.insertOne(d));
    return ids;
  }

  /** Insert or fully replace the document at docId. */
  replaceOne(docId: string, document: Document, relaxed = false): Promise<bigint> {
    const doc = { ...document, _id: docId };
    return this.client.putDocument(this.name, docId, jsonBytes(doc), relaxed);
  }

  updateOne(filter: Document, update: Document, relaxed = false): Promise<UpdateResult> {
    return this.client.update(this.name, filterBytes(filter), jsonBytes(update), false, relaxed);
  }

  updateMany(filter: Document, update: Document, relaxed = false): Promise<UpdateResult> {
    return this.client.update(this.name, filterBytes(filter), jsonBytes(update), true, relaxed);
  }

  deleteOne(filter: Document, relaxed = false): Promise<number> {
    return this.client.deleteByFilter(this.name, filterBytes(filter), false, relaxed);
  }

  deleteMany(filter: Document, relaxed = false): Promise<number> {
    return this.client.deleteByFilter(this.name, filterBytes(filter), true, relaxed);
  }

  /** Return all matching documents, decoded as objects. */
  async find(filter: Document | null, opts: QueryOptions = {}): Promise<Document[]> {
    const bodies = await this.client.find(this.name, filterBytes(filter), {
      sort: opts.sort,
      projection: projection(opts),
      skip: opts.skip,
      limit: opts.limit,
      pageSize: opts.pageSize,
    });
    return bodies.map((b) => (b.length ? (JSON.parse(b.toString("utf8")) as Document) : {}));
  }

  /** Return the first matching document, or null if none match. */
  async findOne(filter: Document | null, opts: QueryOptions = {}): Promise<Document | null> {
    const docs = await this.find(filter, { ...opts, limit: 1 });
    return docs.length ? docs[0]! : null;
  }

  /** Fetch one document directly by id (fast path), or null if absent. */
  async get(docId: string): Promise<Document | null> {
    const body = await this.client.getDocument(this.name, docId);
    return body === null ? null : (JSON.parse(body.toString("utf8")) as Document);
  }

  countDocuments(filter: Document | null = null): Promise<number> {
    return this.client.count(this.name, filterBytes(filter));
  }

  distinct(field: string, filter: Document | null = null): Promise<unknown[]> {
    return this.client.distinct(this.name, field, filterBytes(filter));
  }
}

function projection(opts: QueryOptions): Projection {
  if (opts.include?.length && opts.exclude?.length) {
    throw new ZydecoError("projection cannot mix include and exclude fields");
  }
  if (opts.include?.length) return { mode: Proj.Include, fields: opts.include };
  if (opts.exclude?.length) return { mode: Proj.Exclude, fields: opts.exclude };
  return { mode: Proj.None, fields: [] };
}

function jsonBytes(value: unknown): Buffer {
  return Buffer.from(JSON.stringify(value), "utf8");
}

/** A nil/empty filter is "match all" (empty bytes on the wire). */
function filterBytes(filter: Document | null): Buffer {
  if (!filter || Object.keys(filter).length === 0) return EMPTY;
  return jsonBytes(filter);
}
