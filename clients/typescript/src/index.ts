export { Client, generateId } from "./client.ts";
export type { ClientOptions, FindOptions, UpdateResult } from "./client.ts";
export { Collection } from "./collection.ts";
export type { Document, QueryOptions } from "./collection.ts";
export {
  AuthError,
  ConflictError,
  ConnectionError,
  InvalidRequestError,
  PolicyError,
  ServerBusyError,
  ServerError,
  UnsupportedFormatError,
  ZydecoError,
} from "./errors.ts";
export type { Projection, Row, SortKey } from "./protocol.ts";
