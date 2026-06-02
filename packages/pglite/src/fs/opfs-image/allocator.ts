// Bitmap block allocator over the container's DATA region. Indices are
// data-region-relative (data block 0 = container block `dataStartBlock`).
//
// The bitmap is held in memory and is NOT persisted directly — it is rebuilt on
// recovery by marking every block referenced by a file's extents (see
// OpfsImageFS recovery), so the only durable metadata is the directory tree.

export class BlockAllocator {
  #bits: Uint8Array // 1 bit per data block; 1 = used
  #count: number // number of data blocks tracked

  constructor(numBlocks: number) {
    this.#count = numBlocks
    this.#bits = new Uint8Array((numBlocks + 7) >> 3)
  }

  get numBlocks(): number {
    return this.#count
  }

  #get(i: number): boolean {
    return (this.#bits[i >> 3] & (1 << (i & 7))) !== 0
  }
  #set(i: number, used: boolean): void {
    const byte = i >> 3
    const mask = 1 << (i & 7)
    if (used) this.#bits[byte] |= mask
    else this.#bits[byte] &= ~mask
  }

  isFree(start: number, count: number): boolean {
    if (start < 0 || start + count > this.#count) return false
    for (let i = start; i < start + count; i++) if (this.#get(i)) return false
    return true
  }

  markUsed(start: number, count: number): void {
    for (let i = start; i < start + count; i++) this.#set(i, true)
  }

  freeRun(start: number, count: number): void {
    for (let i = start; i < start + count; i++) this.#set(i, false)
  }

  /** First-fit contiguous run of `count` free blocks. Returns start or -1. */
  allocRun(count: number): number {
    let run = 0
    let runStart = 0
    for (let i = 0; i < this.#count; i++) {
      if (this.#get(i)) {
        run = 0
        continue
      }
      if (run === 0) runStart = i
      run++
      if (run === count) {
        this.markUsed(runStart, count)
        return runStart
      }
    }
    return -1
  }

  /** Extend the bitmap to track more blocks (new blocks are free). */
  grow(newNumBlocks: number): void {
    if (newNumBlocks <= this.#count) return
    const next = new Uint8Array((newNumBlocks + 7) >> 3)
    next.set(this.#bits)
    this.#bits = next
    this.#count = newNumBlocks
  }

  usedCount(): number {
    let n = 0
    for (let i = 0; i < this.#count; i++) if (this.#get(i)) n++
    return n
  }
}
