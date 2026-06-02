import { BaseFilesystem, ERRNO_CODES, type FsStats } from '../base.js'
import type { PostgresMod } from '../../postgresMod.js'
import type { PGlite } from '../../pglite.js'
import { Container } from './container.js'
import { BlockAllocator } from './allocator.js'
import {
  BLOCK_SIZE,
  MAGIC,
  VERSION,
  META_START_BLOCK,
  META_SLOT_BLOCKS,
  META_SLOT_BYTES,
  META_SLOTS,
  DATA_START_BLOCK,
  INITIAL_DATA_BLOCKS,
  INITIAL_TOTAL_BLOCKS,
  GROW_MIN_BLOCKS,
  SLOT_HEADER_BYTES,
  JOURNAL_START_BLOCK,
  JOURNAL_BYTES,
  REC_HEADER_BYTES,
  encodeSuperblock,
  decodeSuperblock,
  encodeSlotHeader,
  decodeSlotHeader,
  encodeRecHeader,
  decodeRecHeader,
  crc32,
  type SuperBlock,
} from './format.js'

// S_IFDIR | 0o777 and S_IFREG | 0o666 — include permission bits so directory
// access checks (initdb/postgres) succeed.
const INITIAL_MODE = { DIR: 16384 | 0o777, FILE: 32768 | 0o666 }

type NodeType = 'file' | 'directory'
interface BaseNode {
  type: NodeType
  lastModified: number
  mode: number
}
interface Extent {
  start: number // data-region block index
  count: number
}
interface FileNode extends BaseNode {
  type: 'file'
  size: number
  extents: Extent[]
}
interface DirectoryNode extends BaseNode {
  type: 'directory'
  children: { [filename: string]: Node }
}
type Node = FileNode | DirectoryNode

interface State {
  root: DirectoryNode
}

export interface OpfsImageOptions {
  debug?: boolean
}

class FsError extends Error {
  code?: number
  constructor(code: keyof typeof ERRNO_CODES, message: string) {
    super(message)
    this.code = ERRNO_CODES[code]
  }
}

/**
 * Single-file block-image filesystem.
 *
 * The ENTIRE Postgres cluster lives inside ONE OPFS file. Only one
 * FileSystemSyncAccessHandle is ever open, so it never approaches the ~1024 FD
 * limit that wedges Chrome — and because that handle is opened once at init and
 * its read/write/truncate/flush are synchronous, the whole VFS runs
 * synchronously in the postgres worker with NO SharedArrayBuffer, NO Atomics,
 * NO I/O worker, and NO COOP/COEP requirement.
 *
 * Each file maps to a list of extents (block runs) inside the container's data
 * region; the directory tree + extents are the only durable metadata, persisted
 * via a double-buffered checkpoint. The allocator bitmap is rebuilt from extents
 * on recovery.
 */
export class OpfsImageFS extends BaseFilesystem {
  declare readonly dataDir: string

  #container!: Container
  #alloc!: BlockAllocator
  #totalBlocks = INITIAL_TOTAL_BLOCKS
  #seq = 0
  #activeSlot = 1 // so the first checkpoint writes slot 0
  #encoder = new TextEncoder()
  #decoder = new TextDecoder()
  #zeroBlock = new Uint8Array(BLOCK_SIZE)

  state!: State
  // Per-path metadata changes since the last sync (null = deleted). Journaled
  // incrementally instead of rewriting the whole tree.
  #dirty = new Map<string, Node | null>()
  #journalOffset = 0 // next write position within the journal region (bytes)
  // Blocks freed this sync window. They stay marked USED in the allocator (so
  // they can't be reallocated) until the sync that durably records the free —
  // this prevents a crash from cross-linking a surviving file to reused blocks.
  #pendingFree: { start: number; count: number }[] = []
  #handleIdCounter = 0
  #openHandlePaths = new Map<number, string>()
  #openHandleIds = new Map<string, number>()

  constructor(dataDir: string, { debug = false }: OpfsImageOptions = {}) {
    super(dataDir, { debug })
  }

  async init(pg: PGlite, opts: Partial<PostgresMod>) {
    await this.#openContainer()
    return super.init(pg, opts)
  }

  async #openContainer(): Promise<void> {
    this.#container = await Container.open(this.dataDir, 'cluster.img')
    if (this.#container.getSize() === 0) {
      this.#format()
    } else {
      this.#recover()
    }
  }

  // --- container lifecycle / metadata ---

  #superblock(): SuperBlock {
    return {
      magic: MAGIC,
      version: VERSION,
      blockSize: BLOCK_SIZE,
      totalBlocks: this.#totalBlocks,
      metaStartBlock: META_START_BLOCK,
      metaSlotBlocks: META_SLOT_BLOCKS,
      dataStartBlock: DATA_START_BLOCK,
    }
  }

  #writeSuperblock(): void {
    this.#container.write(0, encodeSuperblock(this.#superblock()))
  }

  #format(): void {
    this.#totalBlocks = INITIAL_TOTAL_BLOCKS
    this.#container.truncate(this.#totalBlocks * BLOCK_SIZE)
    this.#writeSuperblock()
    this.#alloc = new BlockAllocator(INITIAL_DATA_BLOCKS)
    this.state = {
      root: {
        type: 'directory',
        lastModified: Date.now(),
        mode: INITIAL_MODE.DIR,
        children: {},
      },
    }
    this.#checkpoint()
  }

  #recover(): void {
    const sbBuf = new Uint8Array(BLOCK_SIZE)
    this.#container.read(0, sbBuf)
    const sb = decodeSuperblock(sbBuf)
    if (sb.magic !== MAGIC) throw new Error('opfs-image: bad container magic')
    if (sb.version !== VERSION) {
      throw new Error(
        `opfs-image: incompatible container version ${sb.version} (expected ${VERSION})`,
      )
    }
    this.#totalBlocks = sb.totalBlocks

    // Pick the metadata slot with the highest valid seq.
    let best: { seq: number; slot: number; json: string } | null = null
    for (let slot = 0; slot < META_SLOTS; slot++) {
      const base = (META_START_BLOCK + slot * META_SLOT_BLOCKS) * BLOCK_SIZE
      const hdrBuf = new Uint8Array(SLOT_HEADER_BYTES)
      this.#container.read(base, hdrBuf)
      const hdr = decodeSlotHeader(hdrBuf)
      if (
        hdr.byteLen <= 0 ||
        hdr.byteLen > META_SLOT_BYTES - SLOT_HEADER_BYTES
      ) {
        continue
      }
      const payload = new Uint8Array(hdr.byteLen)
      this.#container.read(base + SLOT_HEADER_BYTES, payload)
      if (crc32(payload) !== hdr.crc) continue
      if (!best || hdr.seq > best.seq) {
        best = { seq: hdr.seq, slot, json: this.#decoder.decode(payload) }
      }
    }
    if (!best) throw new Error('opfs-image: no valid metadata checkpoint')
    this.state = JSON.parse(best.json) as State
    this.#seq = best.seq
    this.#activeSlot = best.slot

    // Replay journal records belonging to this checkpoint epoch (= its seq),
    // stopping at the first stale epoch or torn/invalid record.
    const recHdr = new Uint8Array(REC_HEADER_BYTES)
    let off = 0
    for (;;) {
      if (off + REC_HEADER_BYTES > JOURNAL_BYTES) break
      this.#container.read(JOURNAL_START_BLOCK * BLOCK_SIZE + off, recHdr)
      const h = decodeRecHeader(recHdr)
      if (h.epoch !== this.#seq) break
      if (h.byteLen <= 0 || off + REC_HEADER_BYTES + h.byteLen > JOURNAL_BYTES)
        break
      const payload = new Uint8Array(h.byteLen)
      this.#container.read(
        JOURNAL_START_BLOCK * BLOCK_SIZE + off + REC_HEADER_BYTES,
        payload,
      )
      if (crc32(payload) !== h.crc) break
      const { p, n } = JSON.parse(this.#decoder.decode(payload))
      this.#applyRecord(p, n)
      off += REC_HEADER_BYTES + h.byteLen
    }
    this.#journalOffset = off

    // Rebuild the allocator bitmap from the tree's extents. Size it to cover
    // both the superblock high-water mark and any extent end (robust if a grow's
    // superblock write was lost before a checkpoint).
    let maxBlock = this.#totalBlocks - DATA_START_BLOCK
    const scan = (node: Node) => {
      if (node.type === 'file') {
        for (const ex of node.extents) {
          maxBlock = Math.max(maxBlock, ex.start + ex.count)
        }
      } else {
        for (const c of Object.values(node.children)) scan(c)
      }
    }
    scan(this.state.root)
    this.#alloc = new BlockAllocator(maxBlock)
    const mark = (node: Node) => {
      if (node.type === 'file') {
        for (const ex of node.extents) this.#alloc.markUsed(ex.start, ex.count)
      } else {
        for (const c of Object.values(node.children)) mark(c)
      }
    }
    mark(this.state.root)
  }

  #checkpoint(): void {
    const blob = this.#encoder.encode(JSON.stringify(this.state))
    if (blob.length > META_SLOT_BYTES - SLOT_HEADER_BYTES) {
      throw new Error(
        'opfs-image: metadata exceeds slot size (prototype limit)',
      )
    }
    const seq = ++this.#seq
    const slot = 1 - this.#activeSlot
    const base = (META_START_BLOCK + slot * META_SLOT_BLOCKS) * BLOCK_SIZE
    this.#container.write(
      base,
      encodeSlotHeader({ seq, byteLen: blob.length, crc: crc32(blob) }),
    )
    this.#container.write(base + SLOT_HEADER_BYTES, blob)
    this.#container.flush()
    this.#activeSlot = slot
    // A full checkpoint captures all metadata, so the journal logically resets;
    // subsequent records carry the new seq as their epoch.
    this.#journalOffset = 0
  }

  // --- journal (incremental metadata WAL) ---

  // Compact on-disk representation of a node for a journal record (directories
  // omit children — those arrive via their own records and are merged on replay).
  #journalRepr(node: Node): any {
    if (node.type === 'file') {
      return {
        t: 'f',
        m: node.mode,
        lm: node.lastModified,
        s: node.size,
        e: node.extents,
      }
    }
    return { t: 'd', m: node.mode, lm: node.lastModified }
  }

  #encodeRecord(path: string, node: Node | null): Uint8Array {
    const payload = this.#encoder.encode(
      JSON.stringify({ p: path, n: node ? this.#journalRepr(node) : null }),
    )
    const hdr = encodeRecHeader({
      epoch: this.#seq,
      byteLen: payload.length,
      crc: crc32(payload),
    })
    const rec = new Uint8Array(hdr.length + payload.length)
    rec.set(hdr, 0)
    rec.set(payload, hdr.length)
    return rec
  }

  #markDirty(path: string, node: Node): void {
    this.#dirty.set(path, node)
  }
  #markDeleted(path: string): void {
    this.#dirty.set(path, null)
  }

  #ensureDir(parts: string[]): DirectoryNode {
    let node = this.state.root
    for (const part of parts) {
      let child = node.children[part]
      if (!child || child.type !== 'directory') {
        child = {
          type: 'directory',
          mode: INITIAL_MODE.DIR,
          lastModified: Date.now(),
          children: {},
        }
        node.children[part] = child
      }
      node = child
    }
    return node
  }

  // Apply one journal record to the in-memory tree during recovery.
  #applyRecord(path: string, repr: any): void {
    const parts = this.#pathParts(path)
    const name = parts.pop()!
    if (repr === null) {
      // Navigate to parent without creating; skip if it's gone.
      let node: DirectoryNode = this.state.root
      for (const part of parts) {
        const c: Node | undefined = node.children[part]
        if (!c || c.type !== 'directory') return
        node = c
      }
      delete node.children[name]
      return
    }
    const parent = this.#ensureDir(parts)
    if (repr.t === 'd') {
      const existing = parent.children[name]
      if (existing && existing.type === 'directory') {
        existing.mode = repr.m
        existing.lastModified = repr.lm
      } else {
        parent.children[name] = {
          type: 'directory',
          mode: repr.m,
          lastModified: repr.lm,
          children: {},
        }
      }
    } else {
      parent.children[name] = {
        type: 'file',
        mode: repr.m,
        lastModified: repr.lm,
        size: repr.s,
        extents: repr.e,
      }
    }
  }

  async syncToFs(_relaxedDurability = false) {
    if (this.#dirty.size > 0) {
      // Data blocks were already written via container.write during
      // write/writeFile. Flush them durable BEFORE committing the metadata that
      // references them.
      this.#container.flush()

      const recs: Uint8Array[] = []
      for (const [path, node] of this.#dirty)
        recs.push(this.#encodeRecord(path, node))
      const total = recs.reduce((n, r) => n + r.length, 0)

      if (this.#journalOffset + total > JOURNAL_BYTES) {
        // Journal would overflow — write a full checkpoint instead (captures all
        // current metadata, including these dirty nodes, and resets the journal).
        this.#checkpoint()
      } else {
        let off = this.#journalOffset
        for (const r of recs) {
          this.#container.write(JOURNAL_START_BLOCK * BLOCK_SIZE + off, r)
          off += r.length
        }
        this.#journalOffset = off
        this.#container.flush()
      }
      this.#dirty.clear()
    } else {
      this.#container.flush()
    }
    // The free is now durably committed (the metadata no longer references the
    // freed blocks) — only now return them to the allocator for reuse.
    this.#applyPendingFrees()
  }

  async initialSyncFs() {}

  async closeFs(): Promise<void> {
    try {
      await this.syncToFs()
    } catch {
      /* ignore */
    }
    this.#container.flush()
    this.#container.close()
    this.pg!.Module.FS.quit()
  }

  // --- block / extent helpers ---

  #capacityBytes(node: FileNode): number {
    let blocks = 0
    for (const ex of node.extents) blocks += ex.count
    return blocks * BLOCK_SIZE
  }

  #containerOffset(dataBlock: number, intra: number): number {
    return (DATA_START_BLOCK + dataBlock) * BLOCK_SIZE + intra
  }

  /** Map a byte position within a file to its container location. */
  #mapPos(
    node: FileNode,
    pos: number,
  ): { dataBlock: number; intra: number; contiguous: number } | null {
    let acc = 0
    for (const ex of node.extents) {
      const exBytes = ex.count * BLOCK_SIZE
      if (pos < acc + exBytes) {
        const within = pos - acc
        return {
          dataBlock: ex.start + Math.floor(within / BLOCK_SIZE),
          intra: within % BLOCK_SIZE,
          contiguous: exBytes - within, // bytes until end of this extent
        }
      }
      acc += exBytes
    }
    return null
  }

  #growContainer(extraDataBlocks: number): void {
    const newDataBlocks = this.#alloc.numBlocks + extraDataBlocks
    this.#totalBlocks = DATA_START_BLOCK + newDataBlocks
    this.#container.truncate(this.#totalBlocks * BLOCK_SIZE)
    this.#alloc.grow(newDataBlocks)
    this.#writeSuperblock()
  }

  /** Ensure a file's extents cover at least `neededBytes` of capacity. */
  #ensureCapacity(node: FileNode, neededBytes: number): void {
    const cap = this.#capacityBytes(node)
    if (cap >= neededBytes) return
    let needBlocks = Math.ceil((neededBytes - cap) / BLOCK_SIZE)

    // Try to extend the last extent in place (keeps extents few).
    if (node.extents.length > 0) {
      const last = node.extents[node.extents.length - 1]
      const after = last.start + last.count
      let ext = 0
      while (ext < needBlocks && this.#alloc.isFree(after + ext, 1)) ext++
      if (ext > 0) {
        this.#alloc.markUsed(after, ext)
        last.count += ext
        needBlocks -= ext
      }
    }
    while (needBlocks > 0) {
      let start = this.#alloc.allocRun(needBlocks)
      while (start < 0) {
        this.#growContainer(Math.max(needBlocks, GROW_MIN_BLOCKS))
        start = this.#alloc.allocRun(needBlocks)
      }
      node.extents.push({ start, count: needBlocks })
      needBlocks = 0
    }
  }

  /** Zero a logical byte range [from, to) of a file in the container. */
  #zeroRange(node: FileNode, from: number, to: number): void {
    let pos = from
    while (pos < to) {
      const m = this.#mapPos(node, pos)!
      const seg = Math.min(to - pos, m.contiguous)
      let written = 0
      while (written < seg) {
        const chunk = Math.min(seg - written, BLOCK_SIZE)
        this.#container.write(
          this.#containerOffset(m.dataBlock, m.intra) + written,
          this.#zeroBlock.subarray(0, chunk),
        )
        written += chunk
      }
      pos += seg
    }
  }

  // Defer a free: keep the blocks marked used until the next durable sync.
  #deferFree(start: number, count: number): void {
    this.#pendingFree.push({ start, count })
  }

  // Called after a sync has durably committed the metadata reflecting the frees;
  // only now are the blocks safe to reallocate.
  #applyPendingFrees(): void {
    for (const f of this.#pendingFree) this.#alloc.freeRun(f.start, f.count)
    this.#pendingFree.length = 0
  }

  #freeExtents(node: FileNode): void {
    for (const ex of node.extents) this.#deferFree(ex.start, ex.count)
    node.extents = []
    node.size = 0
  }

  // --- Filesystem API ---

  chmod(path: string, mode: number): void {
    const node = this.#resolvePath(path)
    node.mode = mode
    this.#markDirty(path, node)
  }

  close(fd: number): void {
    const path = this.#openHandlePaths.get(fd)
    if (path !== undefined) {
      this.#openHandlePaths.delete(fd)
      this.#openHandleIds.delete(path)
    }
  }

  fstat(fd: number): FsStats {
    return this.lstat(this.#getPathFromFd(fd))
  }

  lstat(path: string): FsStats {
    const node = this.#resolvePath(path)
    const size = node.type === 'file' ? node.size : 0
    const blksize = BLOCK_SIZE
    return {
      dev: 0,
      ino: 0,
      mode: node.mode,
      nlink: 1,
      uid: 0,
      gid: 0,
      rdev: 0,
      size,
      blksize,
      blocks: Math.ceil(size / 512),
      atime: node.lastModified,
      mtime: node.lastModified,
      ctime: node.lastModified,
    }
  }

  mkdir(path: string, options?: { recursive?: boolean; mode?: number }): void {
    const parts = this.#pathParts(path)
    const newDirName = parts.pop()!
    let node = this.state.root
    const currentPath: string[] = []
    for (const part of parts) {
      currentPath.push(part)
      if (!Object.prototype.hasOwnProperty.call(node.children, part)) {
        if (options?.recursive) {
          this.mkdir('/' + currentPath.join('/'))
        } else {
          throw new FsError('ENOENT', 'No such file or directory')
        }
      }
      if (node.children[part].type !== 'directory') {
        throw new FsError('ENOTDIR', 'Not a directory')
      }
      node = node.children[part] as DirectoryNode
    }
    if (Object.prototype.hasOwnProperty.call(node.children, newDirName)) {
      throw new FsError('EEXIST', 'File exists')
    }
    node.children[newDirName] = {
      type: 'directory',
      lastModified: Date.now(),
      mode: options?.mode || INITIAL_MODE.DIR,
      children: {},
    }
    this.#markDirty(path, node.children[newDirName])
  }

  open(path: string): number {
    const node = this.#resolvePath(path)
    if (node.type !== 'file') throw new FsError('EISDIR', 'Is a directory')
    const handleId = this.#nextHandleId()
    this.#openHandlePaths.set(handleId, path)
    this.#openHandleIds.set(path, handleId)
    return handleId
  }

  readdir(path: string): string[] {
    const node = this.#resolvePath(path)
    if (node.type !== 'directory')
      throw new FsError('ENOTDIR', 'Not a directory')
    return Object.keys(node.children)
  }

  read(
    fd: number,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): number {
    const node = this.#resolvePath(this.#getPathFromFd(fd))
    if (node.type !== 'file') throw new FsError('EISDIR', 'Is a directory')
    if (position >= node.size) return 0
    const toRead = Math.min(length, node.size - position)
    const target = new Uint8Array(buffer.buffer, offset, length)
    let done = 0
    while (done < toRead) {
      const m = this.#mapPos(node, position + done)
      if (!m) break
      const seg = Math.min(toRead - done, m.contiguous)
      const n = this.#container.read(
        this.#containerOffset(m.dataBlock, m.intra),
        target.subarray(done, done + seg),
      )
      done += n
      if (n < seg) break
    }
    return done
  }

  write(
    fd: number,
    buffer: Uint8Array, // Buffer to read from
    offset: number,
    length: number,
    position: number,
  ): number {
    const path = this.#getPathFromFd(fd)
    const node = this.#resolvePath(path)
    if (node.type !== 'file') throw new FsError('EISDIR', 'Is a directory')
    this.#markDirty(path, node)
    this.#writeBytes(node, new Uint8Array(buffer, offset, length), position)
    return length
  }

  /** Core byte writer used by both `write` and `writeFile`. */
  #writeBytes(node: FileNode, src: Uint8Array, position: number): void {
    this.#ensureCapacity(node, position + src.length)
    // A gap (write past EOF) must read back as zero — zero the hole first.
    if (position > node.size) this.#zeroRange(node, node.size, position)
    let done = 0
    while (done < src.length) {
      const m = this.#mapPos(node, position + done)!
      const seg = Math.min(src.length - done, m.contiguous)
      this.#container.write(
        this.#containerOffset(m.dataBlock, m.intra),
        src.subarray(done, done + seg),
      )
      done += seg
    }
    node.size = Math.max(node.size, position + src.length)
    node.lastModified = Date.now()
  }

  writeFile(
    path: string,
    data: string | Uint8Array,
    options?: { mode?: number },
  ): void {
    const parts = this.#pathParts(path)
    const filename = parts.pop()!
    const parent = this.#resolvePath('/' + parts.join('/')) as DirectoryNode
    let node: FileNode
    if (!Object.prototype.hasOwnProperty.call(parent.children, filename)) {
      node = {
        type: 'file',
        lastModified: Date.now(),
        mode: options?.mode || INITIAL_MODE.FILE,
        size: 0,
        extents: [],
      }
      parent.children[filename] = node
    } else {
      node = parent.children[filename] as FileNode
      node.lastModified = Date.now()
    }
    this.#markDirty(path, node)
    const bytes = typeof data === 'string' ? this.#encoder.encode(data) : data
    if (bytes.length > 0) this.#writeBytes(node, bytes, 0)
  }

  truncate(path: string, len = 0): void {
    const node = this.#resolvePath(path)
    if (node.type !== 'file') throw new FsError('EISDIR', 'Is a directory')
    this.#markDirty(path, node)
    if (len === 0) {
      this.#freeExtents(node)
      return
    }
    if (len > node.size) {
      // Grow: ensure capacity, zero the new region.
      this.#ensureCapacity(node, len)
      this.#zeroRange(node, node.size, len)
      node.size = len
      return
    }
    // Shrink: free whole blocks beyond `len`; keep the block holding the boundary.
    const keepBlocks = Math.ceil(len / BLOCK_SIZE)
    let acc = 0 // blocks accumulated
    const kept: Extent[] = []
    for (const ex of node.extents) {
      if (acc >= keepBlocks) {
        this.#deferFree(ex.start, ex.count)
      } else if (acc + ex.count <= keepBlocks) {
        kept.push(ex)
        acc += ex.count
      } else {
        const keep = keepBlocks - acc
        kept.push({ start: ex.start, count: keep })
        this.#deferFree(ex.start + keep, ex.count - keep)
        acc += keep
      }
    }
    node.extents = kept
    node.size = len
  }

  rename(oldPath: string, newPath: string): void {
    const oldParts = this.#pathParts(oldPath)
    const oldName = oldParts.pop()!
    const oldParent = this.#resolvePath(
      '/' + oldParts.join('/'),
    ) as DirectoryNode
    if (!Object.prototype.hasOwnProperty.call(oldParent.children, oldName)) {
      throw new FsError('ENOENT', 'No such file or directory')
    }
    const newParts = this.#pathParts(newPath)
    const newName = newParts.pop()!
    const newParent = this.#resolvePath(
      '/' + newParts.join('/'),
    ) as DirectoryNode
    if (Object.prototype.hasOwnProperty.call(newParent.children, newName)) {
      const victim = newParent.children[newName]
      if (victim.type === 'file') this.#freeExtents(victim)
    }
    const moved = oldParent.children[oldName]
    newParent.children[newName] = moved
    delete oldParent.children[oldName]
    this.#markDeleted(oldPath)
    this.#markDirty(newPath, moved)
  }

  rmdir(path: string): void {
    const parts = this.#pathParts(path)
    const name = parts.pop()!
    const parent = this.#resolvePath('/' + parts.join('/')) as DirectoryNode
    const node = parent.children[name]
    if (!node) throw new FsError('ENOENT', 'No such file or directory')
    if (node.type !== 'directory')
      throw new FsError('ENOTDIR', 'Not a directory')
    if (Object.keys(node.children).length > 0) {
      throw new FsError('ENOTEMPTY', 'Directory not empty')
    }
    delete parent.children[name]
    this.#markDeleted(path)
  }

  unlink(path: string): void {
    const parts = this.#pathParts(path)
    const filename = parts.pop()!
    const dir = this.#resolvePath('/' + parts.join('/')) as DirectoryNode
    const node = dir.children[filename]
    if (!node) throw new FsError('ENOENT', 'No such file or directory')
    if (node.type !== 'file') throw new FsError('EISDIR', 'Is a directory')
    this.#freeExtents(node)
    delete dir.children[filename]
    if (this.#openHandleIds.has(path)) {
      this.#openHandlePaths.delete(this.#openHandleIds.get(path)!)
      this.#openHandleIds.delete(path)
    }
    this.#markDeleted(path)
  }

  utimes(path: string, _atime: number, mtime: number): void {
    const node = this.#resolvePath(path)
    node.lastModified = mtime
    this.#markDirty(path, node)
  }

  // --- internal helpers ---

  #pathParts(path: string): string[] {
    return path.split('/').filter(Boolean)
  }

  #resolvePath(path: string): Node {
    let node: Node = this.state.root
    for (const part of this.#pathParts(path)) {
      if (node.type !== 'directory')
        throw new FsError('ENOTDIR', 'Not a directory')
      if (!Object.prototype.hasOwnProperty.call(node.children, part)) {
        throw new FsError('ENOENT', 'No such file or directory')
      }
      node = node.children[part]
    }
    return node
  }

  #getPathFromFd(fd: number): string {
    const path = this.#openHandlePaths.get(fd)
    if (path === undefined) throw new FsError('EBADF', 'Bad file descriptor')
    return path
  }

  #nextHandleId(): number {
    let id = ++this.#handleIdCounter
    while (this.#openHandlePaths.has(id)) id = ++this.#handleIdCounter
    return id
  }
}
