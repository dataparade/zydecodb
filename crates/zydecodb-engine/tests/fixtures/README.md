# Format-version fixtures

This directory is the engine's format-history archive. When an on-disk format
is bumped (WAL segment, SSTable footer, manifest, or any future addition), the
PR that bumps it MUST land here a binary fixture written by the prior version,
together with a test in `../format_versions.rs` proving the new engine refuses
that fixture with a clean, frozen-substring error.

The current Phase 1 cut generates the v1 shapes inline because the prior
engine versions never shipped externally — there is no historical artifact to
preserve yet. From the first tagged release onward, every format bump adds
exactly one binary file here and one assertion in `format_versions.rs`.

Naming convention: `<surface>_v<N>.<ext>`. Examples:
- `wal_v1_segment.bin`
- `sstable_v1_footer.bin`
- `manifest_v1.bin`

Do not edit existing fixtures. Add new ones; refusal tests are append-only.
