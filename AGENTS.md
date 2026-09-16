ZydecoDB is a document store. Official drivers speak a binary protocol on `127.0.0.1:9470`.
Run `zydecodb --agent` for usage, examples, and hard no's. Then `zydecodb --agent python` (or `go` / `typescript`).
Never expose `:9470`. Use an official driver; do not invent Mongo APIs.
