// Owns THE single OPFS FileSystemSyncAccessHandle for the whole cluster.
// All access is synchronous (the handle is opened once at init), so there is no
// SharedArrayBuffer, no Atomics, no I/O worker — and no COOP/COEP requirement.

// TypeScript doesn't have a built-in type for FileSystemSyncAccessHandle, and
// `createSyncAccessHandle` is likewise missing from the FileSystemFileHandle
// lib type — hence the local `SAH` interface and the `as any` cast in `open`.
interface SAH {
  close(): void
  flush(): void
  getSize(): number
  read(buffer: ArrayBufferView, opts: { at: number }): number
  truncate(newSize: number): void
  write(buffer: ArrayBufferView, opts: { at: number }): number
}

export class Container {
  #handle: SAH
  // Private, non-shared scratch. The WASM heap is backed by a SharedArrayBuffer
  // under cross-origin isolation, and sync-access-handle read/write want a
  // non-shared view — so we copy through this. Chunking it also lets a single
  // large mmap read flow through a bounded buffer.
  #scratch = new Uint8Array(1 << 20) // 1 MiB

  private constructor(handle: SAH) {
    this.#handle = handle
  }

  static async open(dataDir: string, fileName: string): Promise<Container> {
    const root = await navigator.storage.getDirectory()
    let dir = root
    for (const part of dataDir.split('/').filter(Boolean)) {
      dir = await dir.getDirectoryHandle(part, { create: true })
    }
    const fh = await dir.getFileHandle(fileName, { create: true })
    const handle = (await (fh as any).createSyncAccessHandle()) as SAH
    return new Container(handle)
  }

  /** Read into `into` from container byte offset `at`. Returns bytes read. */
  read(at: number, into: Uint8Array): number {
    let done = 0
    while (done < into.length) {
      const chunk = Math.min(into.length - done, this.#scratch.length)
      const view = this.#scratch.subarray(0, chunk)
      const n = this.#handle.read(view, { at: at + done })
      if (n <= 0) break
      into.set(view.subarray(0, n), done)
      done += n
      if (n < chunk) break
    }
    return done
  }

  /** Write `bytes` at container byte offset `at`. Returns bytes written. */
  write(at: number, bytes: Uint8Array): number {
    let done = 0
    while (done < bytes.length) {
      const chunk = Math.min(bytes.length - done, this.#scratch.length)
      const view = this.#scratch.subarray(0, chunk)
      view.set(bytes.subarray(done, done + chunk))
      const n = this.#handle.write(view, { at: at + done })
      done += n
      if (n < chunk) break
    }
    return done
  }

  /** Grow (or set) the container file size in bytes (OPFS zero-fills growth). */
  truncate(bytes: number): void {
    this.#handle.truncate(bytes)
  }

  getSize(): number {
    return this.#handle.getSize()
  }

  flush(): void {
    this.#handle.flush()
  }

  close(): void {
    this.#handle.close()
  }
}
