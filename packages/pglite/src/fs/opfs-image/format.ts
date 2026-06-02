// On-disk layout for the single-file block-image container.
//
// The whole Postgres cluster lives in one OPFS file:
//
//   block 0            : superblock (layout header)
//   metaStart .. +N    : metadata slot 0 (double-buffered checkpoint)
//   metaStart+N .. +N  : metadata slot 1
//   dataStart ..       : data region — fixed-size blocks holding file extents
//
// 8 KiB blocks match the Postgres page size, so the common case (page reads /
// writes) is block-aligned and avoids read-modify-write.

export const BLOCK_SIZE = 8192
export const MAGIC = 0x50474c49 // "PGLI"
export const VERSION = 3

// Append-only journal region: between full checkpoints, each metadata change is
// written as a small WAL record here (cheap), and a full checkpoint happens only
// when this region fills. 256 blocks = 2 MiB holds thousands of records.
export const JOURNAL_START_BLOCK = 1 // right after the superblock
export const JOURNAL_BLOCKS = 256
export const JOURNAL_BYTES = JOURNAL_BLOCKS * BLOCK_SIZE

// Two double-buffered metadata slots. Each holds the serialized directory tree
// (with per-file extents); recovery picks the slot with the highest valid seq.
// 4096 blocks = 32 MiB per slot fits ~300k file/extent entries before the
// checkpoint overflows (a hard prototype limit) — far beyond a typical cluster.
// The reserved-but-unwritten region is sparse on disk, so the cost is low.
// (The real scaling fix is to store metadata in the data region so it grows
// like a normal FS; this is the cheap interim bump.)
export const META_SLOTS = 2
export const META_SLOT_BLOCKS = 4096
export const META_SLOT_BYTES = META_SLOT_BLOCKS * BLOCK_SIZE

export const META_START_BLOCK = JOURNAL_START_BLOCK + JOURNAL_BLOCKS
export const DATA_START_BLOCK = META_START_BLOCK + META_SLOTS * META_SLOT_BLOCKS

// Initial container size: header + metadata + a small data reserve.
export const INITIAL_DATA_BLOCKS = 2048 // 16 MiB of data to start
export const INITIAL_TOTAL_BLOCKS = DATA_START_BLOCK + INITIAL_DATA_BLOCKS

// Grow the container by at least this many blocks (amortise truncate cost).
export const GROW_MIN_BLOCKS = 1024 // 8 MiB

// --- Superblock (block 0) ---
export interface SuperBlock {
  magic: number
  version: number
  blockSize: number
  totalBlocks: number // current container size, in blocks (high-water mark)
  metaStartBlock: number
  metaSlotBlocks: number
  dataStartBlock: number
}

export function encodeSuperblock(sb: SuperBlock): Uint8Array {
  const buf = new Uint8Array(BLOCK_SIZE)
  const dv = new DataView(buf.buffer)
  dv.setUint32(0, sb.magic, true)
  dv.setUint32(4, sb.version, true)
  dv.setUint32(8, sb.blockSize, true)
  dv.setUint32(12, sb.totalBlocks, true)
  dv.setUint32(16, sb.metaStartBlock, true)
  dv.setUint32(20, sb.metaSlotBlocks, true)
  dv.setUint32(24, sb.dataStartBlock, true)
  return buf
}

export function decodeSuperblock(buf: Uint8Array): SuperBlock {
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength)
  return {
    magic: dv.getUint32(0, true),
    version: dv.getUint32(4, true),
    blockSize: dv.getUint32(8, true),
    totalBlocks: dv.getUint32(12, true),
    metaStartBlock: dv.getUint32(16, true),
    metaSlotBlocks: dv.getUint32(20, true),
    dataStartBlock: dv.getUint32(24, true),
  }
}

// --- Metadata slot header (16 bytes at the start of each slot) ---
export const SLOT_HEADER_BYTES = 16

export interface SlotHeader {
  seq: number // monotonically increasing; highest valid slot wins on recovery
  byteLen: number // length of the serialized payload that follows
  crc: number // crc32 of the payload
}

export function encodeSlotHeader(h: SlotHeader): Uint8Array {
  const buf = new Uint8Array(SLOT_HEADER_BYTES)
  const dv = new DataView(buf.buffer)
  dv.setFloat64(0, h.seq, true)
  dv.setUint32(8, h.byteLen, true)
  dv.setUint32(12, h.crc >>> 0, true)
  return buf
}

export function decodeSlotHeader(buf: Uint8Array): SlotHeader {
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength)
  return {
    seq: dv.getFloat64(0, true),
    byteLen: dv.getUint32(8, true),
    crc: dv.getUint32(12, true),
  }
}

// --- Journal record header (16 bytes), followed by the JSON payload ---
// `epoch` is the seq of the checkpoint this record belongs to; on recovery we
// replay records whose epoch matches the loaded checkpoint and stop at the first
// mismatch (stale data from a prior epoch) or bad crc (torn tail write).
export const REC_HEADER_BYTES = 16

export interface RecHeader {
  epoch: number
  byteLen: number
  crc: number
}

export function encodeRecHeader(h: RecHeader): Uint8Array {
  const buf = new Uint8Array(REC_HEADER_BYTES)
  const dv = new DataView(buf.buffer)
  dv.setFloat64(0, h.epoch, true)
  dv.setUint32(8, h.byteLen, true)
  dv.setUint32(12, h.crc >>> 0, true)
  return buf
}

export function decodeRecHeader(buf: Uint8Array): RecHeader {
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength)
  return {
    epoch: dv.getFloat64(0, true),
    byteLen: dv.getUint32(8, true),
    crc: dv.getUint32(12, true),
  }
}

// --- CRC32 (IEEE) ---
const CRC_TABLE = (() => {
  const t = new Uint32Array(256)
  for (let n = 0; n < 256; n++) {
    let c = n
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1
    t[n] = c >>> 0
  }
  return t
})()

export function crc32(bytes: Uint8Array): number {
  let c = 0xffffffff
  for (let i = 0; i < bytes.length; i++) {
    c = CRC_TABLE[(c ^ bytes[i]) & 0xff] ^ (c >>> 8)
  }
  return (c ^ 0xffffffff) >>> 0
}
