---
'@electric-sql/pglite': minor
---

Add `opfs-image://` — a single-file block-image OPFS filesystem.

The new `opfs-image` backend stores the entire Postgres cluster inside one OPFS file, so it only ever holds a single sync access handle open. This avoids the file-descriptor pressure of the access-handle-pool backend (which keeps ~1000 handles open and can wedge a Chromium renderer, and exceeds Safari's open-handle limit), and because the single handle's I/O is synchronous the VFS needs no `SharedArrayBuffer`, Atomics, or cross-origin isolation (COOP/COEP). Use it with `new PGlite('opfs-image://path')` or `new PGlite({ fs: new OpfsImageFS('path') })`.

Also fixes a latent bug where passing a custom `fs` instance via `new PGlite({ fs })` caused the internal `initdb` sub-instance to reuse and re-initialise that same instance; the sub-instance now runs against an in-memory filesystem.
