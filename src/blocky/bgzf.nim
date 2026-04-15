## bgzf — BGZF I/O, format types, index parsing, and format sniffing.
##
## Only external dependency: libdeflate (-ldeflate), no htslib required.
## All proc signatures use explicit types per project style guide.

import std/[algorithm, atomics, os, posix, strformat, strutils]

# ---------------------------------------------------------------------------
# Verbose logging — enabled by -v flag, shared across all modules
# ---------------------------------------------------------------------------

var verbose* = false

template info*(msg: string) =
  if verbose: stderr.writeLine "info: " & msg

# ---------------------------------------------------------------------------
# Format types
# ---------------------------------------------------------------------------

type
  FileFormat* = enum
    ffVcf, ffBcf, ffText

  Compression* = enum
    compBgzf, compNone

proc `$`*(f: FileFormat): string =
  ## Human-readable format name for messages.
  case f
  of ffVcf:  "VCF"
  of ffBcf:  "BCF"
  of ffText: "text"

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

const BGZF_EOF* = [
  0x1f'u8, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
  0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
  0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00
]

const BCF_MAGIC* = [byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8]

## BGZF magic: gzip header with FEXTRA flag (1f 8b 08 04).
const BGZF_MAGIC* = [0x1f'u8, 0x8b'u8, 0x08'u8, 0x04'u8]

## Byte overhead per BGZF block: 18-byte header + 4-byte CRC32 + 4-byte ISIZE.
const BGZF_OVERHEAD* = 26
## Maximum uncompressed bytes per BGZF block.
const BGZF_MAX_BLOCK_SIZE* = 65536

# ---------------------------------------------------------------------------
# libdeflate C FFI — deflate + inflate + crc32
# ---------------------------------------------------------------------------

{.passC: "-I vendor/libdeflate-1.25".}
{.passL: "vendor/libdeflate-1.25/build/libdeflate.a".}

const LIBDEFLATE_SUCCESS = 0'i32

proc libdeflateAllocCompressor(level: cint): pointer
  {.importc: "libdeflate_alloc_compressor", header: "<libdeflate.h>".}
proc libdeflateDeflateCompress(c: pointer; inBuf: pointer; inLen: csize_t;
                               outBuf: pointer; outLen: csize_t): csize_t
  {.importc: "libdeflate_deflate_compress", header: "<libdeflate.h>".}
proc libdeflateDeflateCompressBound(c: pointer; inLen: csize_t): csize_t
  {.importc: "libdeflate_deflate_compress_bound", header: "<libdeflate.h>".}
proc libdeflateFreeCompressor(c: pointer)
  {.importc: "libdeflate_free_compressor", header: "<libdeflate.h>".}
proc libdeflateAllocDecompressor(): pointer
  {.importc: "libdeflate_alloc_decompressor", header: "<libdeflate.h>".}
proc libdeflateDeflateDecompress(d: pointer; inBuf: pointer; inLen: csize_t;
                                 outBuf: pointer; outLen: csize_t;
                                 actualOut: ptr csize_t): cint
  {.importc: "libdeflate_deflate_decompress", header: "<libdeflate.h>".}
proc libdeflateFreeDecompressor(d: pointer)
  {.importc: "libdeflate_free_decompressor", header: "<libdeflate.h>".}
proc libdeflateCrc32(crc: cuint; buf: pointer; len: csize_t): cuint
  {.importc: "libdeflate_crc32", header: "<libdeflate.h>".}

# ---------------------------------------------------------------------------
# Thread-local libdeflate codec cache
# ---------------------------------------------------------------------------
#
# libdeflate compressor/decompressor objects are expensive to allocate and
# designed to be reused.  Cache one of each per thread so the BGZF hot paths
# (decompressBgzf, compressToBgzf) never allocate a new codec per block.
# Each worker thread (threadpool or spawn) owns its own {.threadvar.} slot.

var tlDecompressor {.threadvar.}: pointer
var tlCompressor {.threadvar.}: pointer
var tlCompressorLevel {.threadvar.}: int

proc getDecompressor(): pointer =
  if tlDecompressor == nil:
    tlDecompressor = libdeflateAllocDecompressor()
    if tlDecompressor == nil:
      quit("libdeflate_alloc_decompressor returned nil", 1)
  tlDecompressor

proc getCompressor(level: int): pointer =
  if tlCompressor == nil or tlCompressorLevel != level:
    if tlCompressor != nil:
      libdeflateFreeCompressor(tlCompressor)
    tlCompressor = libdeflateAllocCompressor(level.cint)
    if tlCompressor == nil:
      quit("libdeflate_alloc_compressor returned nil", 1)
    tlCompressorLevel = level
  tlCompressor

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

proc leU16(buf: openArray[byte]; pos: int): uint16 {.inline.} =
  ## Read a little-endian uint16 from buf at pos.
  buf[pos].uint16 or (buf[pos + 1].uint16 shl 8)

proc leU32*(buf: openArray[byte]; pos: int): uint32 {.inline.} =
  ## Read a little-endian uint32 from buf at pos.
  buf[pos].uint32 or (buf[pos+1].uint32 shl 8) or
  (buf[pos+2].uint32 shl 16) or (buf[pos+3].uint32 shl 24)

proc readLeU32(data: openArray[byte]; pos: var int): uint32 =
  ## Read a little-endian uint32 from data at pos, then advance pos by 4.
  result = data[pos].uint32 or (data[pos+1].uint32 shl 8) or
           (data[pos+2].uint32 shl 16) or (data[pos+3].uint32 shl 24)
  pos += 4

proc readLeI32(data: openArray[byte]; pos: var int): int32 =
  ## Read a little-endian int32 from data at pos, then advance pos by 4.
  cast[int32](readLeU32(data, pos))

proc readLeU64(data: openArray[byte]; pos: var int): uint64 =
  ## Read a little-endian uint64 from data at pos, then advance pos by 8.
  let lo = readLeU32(data, pos).uint64   # advances pos by 4
  let hi = readLeU32(data, pos).uint64   # advances pos by 4 again
  result = lo or (hi shl 32)

proc putLeU16(buf: var seq[byte]; pos: int; v: uint16) {.inline.} =
  ## Write a little-endian uint16 into buf at pos.
  buf[pos]   = byte(v and 0xff)
  buf[pos+1] = byte(v shr 8)

proc putLeU32(buf: var seq[byte]; pos: int; v: uint32) {.inline.} =
  ## Write a little-endian uint32 into buf at pos.
  buf[pos]   = byte(v and 0xff)
  buf[pos+1] = byte((v shr 8) and 0xff)
  buf[pos+2] = byte((v shr 16) and 0xff)
  buf[pos+3] = byte((v shr 24) and 0xff)

proc bgzfBlockSize*(buf: openArray[byte]): int =
  ## Parse a BGZF block header at the start of buf; return total block size.
  ## Returns -1 if not a valid BGZF block.
  if buf.len < 18:
    return -1
  # buf.len >= 18 proven above; single push/pop to eliminate bounds checks
  # on the remaining accesses (all within [0..17] for standard BGZF xlen=6).
  {.push boundChecks: off.}
  result = -1
  if buf[0] == 0x1f and buf[1] == 0x8b and buf[2] == 0x08 and buf[3] == 0x04:
    let xlen = leU16(buf, 10).int
    var p = 12
    while p + 4 <= 12 + xlen:
      let slen = leU16(buf, p + 2).int
      if buf[p] == 0x42 and buf[p + 1] == 0x43:  # 'B','C' subfield
        result = leU16(buf, p + 4).int + 1        # BSIZE - 1 + 1
        break
      p += 4 + slen
  {.pop.}

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

proc scanBgzfBlockStarts*(path: string; startAt: int64 = 0;
                           endAt: int64 = -1): seq[int64] =
  ## Scan BGZF blocks in path beginning at startAt.
  ## Returns the file offset of each valid block start.
  ## Stops at endAt (exclusive), end of file, or an invalid block header.
  result = @[]
  let f = open(path, fmRead)
  defer: f.close()
  var buf = newSeqUninit[byte](18)
  var cur = startAt
  while true:
    if endAt >= 0 and cur >= endAt:
      break
    f.setFilePos(cur)
    if readBytes(f, buf, 0, 18) < 18:
      break
    let blkSize = bgzfBlockSize(buf)
    if blkSize < 0:
      break
    result.add(cur)
    cur += blkSize.int64

# ---------------------------------------------------------------------------
# sendfile(2) — zero-copy fd-to-fd transfer
# ---------------------------------------------------------------------------

when defined(linux):
  proc c_copy_file_range(fd_in: cint; off_in: ptr Off;
                          fd_out: cint; off_out: ptr Off;
                          len: csize_t; flags: cuint): int
    {.importc: "copy_file_range", header: "<unistd.h>".}
  proc c_sendfile(outFd, inFd: cint; offset: ptr Off; count: csize_t): int
    {.importc: "sendfile", header: "<sys/sendfile.h>".}

proc isPermanentUnsupport(e: cint): bool {.inline.} =
  ## True for errno values that indicate the syscall is permanently
  ## unsupported on this filesystem/kernel — safe to cache and skip.
  e == ENOSYS or e == EXDEV or e == EOPNOTSUPP

var gTierCopyFileRangeFailed* {.global.}: Atomic[bool]
var gTierSendfileFailed* {.global.}: Atomic[bool]
var tlCopyBuf {.threadvar.}: seq[byte]

proc copyRange*(outFd, inFd: cint; count: Off; startOffset: Off = 0) =
  ## Copy count bytes from inFd at startOffset to outFd.
  ## Tries copy_file_range → sendfile → pread/pwrite (Linux).
  ## Each tier caches permanent failures (ENOSYS/EXDEV/EOPNOTSUPP)
  ## globally so all threads skip it.  Retries on EINTR.
  ## outFd must be positioned by the caller.  copy_file_range uses and
  ## advances the fd position (off_out = nil).  sendfile writes at the
  ## current fd position.  pread/pwrite tracks write offset explicitly.
  var curInOff: Off = startOffset
  var copied: Off = 0
  when defined(linux):
    # Tier 1: copy_file_range (kernel-space, CoW, widest FS support).
    if not gTierCopyFileRangeFailed.load(moRelaxed):
      while copied < count:
        let remaining = count - copied
        # off_out = nil: use and advance outFd's current file position.
        let sent = c_copy_file_range(inFd, addr curInOff, outFd, nil,
                                      csize_t(remaining), 0)
        if sent > 0:
          copied += sent.Off
          continue
        if sent < 0 and errno == EINTR: continue
        if isPermanentUnsupport(errno):
          gTierCopyFileRangeFailed.store(true, moRelease)
          info("copy_file_range unsupported, trying sendfile")
        break
      if copied >= count: return

    # Tier 2: sendfile (zero-copy, offset param controls inFd position).
    if not gTierSendfileFailed.load(moRelaxed):
      while copied < count:
        let remaining = count - copied
        let sent = c_sendfile(outFd, inFd, addr curInOff, csize_t(remaining))
        if sent > 0:
          copied += sent.Off
          continue
        if sent < 0 and errno == EINTR: continue
        if isPermanentUnsupport(errno):
          gTierSendfileFailed.store(true, moRelease)
          info("sendfile unsupported, falling back to pread/pwrite")
        break
      if copied >= count: return

  # Tier 3: pread/pwrite fallback (always works, no lseek on inFd).
  # Get current outFd position for pwrite (pwrite doesn't advance fd pos).
  var curOutOff: Off = posix.lseek(outFd, 0, SEEK_CUR)
  const BufSize = 262144  # 256 KiB
  if tlCopyBuf.len < BufSize: tlCopyBuf = newSeqUninit[byte](BufSize)
  while copied < count:
    let toRead = min(count - copied, BufSize.Off)
    let n = posix.pread(inFd, addr tlCopyBuf[0], toRead.int, curInOff)
    if n < 0:
      if errno == EINTR: continue
      break
    if n == 0: break
    var written: int = 0
    while written < n:
      let w = posix.pwrite(outFd, addr tlCopyBuf[written], n - written,
                            curOutOff + written.Off)
      if w < 0:
        if errno == EINTR: continue
        break
      if w == 0: break
      written += w
    curInOff += n.Off
    curOutOff += n.Off
    copied += n.Off

proc copyRangeFromFile*(srcPath: string; dstFd: cint; start: int64; length: int64) =
  ## Copy length bytes from srcPath[start..] to dstFd.
  ## Tries copy_file_range → sendfile → pread/pwrite.
  if length <= 0: return
  let srcFd = posix.open(srcPath.cstring, O_RDONLY)
  if srcFd < 0:
    quit("copyRangeFromFile: could not open " & srcPath, 1)
  defer: discard posix.close(srcFd)
  copyRange(dstFd, srcFd, length.Off, start.Off)

proc decompressBgzf*(data: openArray[byte]): seq[byte] =
  ## Decompress the first BGZF block in data; return the uncompressed bytes.
  ## Calls quit(1) on malformed input.  Reuses a thread-local decompressor.
  let blkSize = bgzfBlockSize(data)
  if blkSize < 0:
    quit("decompressBgzf: not a valid BGZF block header", 1)
  let isize = leU32(data, blkSize - 4).int
  if isize == 0:
    return @[]
  result = newSeqUninit[byte](isize)
  let dcmp = getDecompressor()
  let compLen = blkSize - BGZF_OVERHEAD
  let ret = libdeflateDeflateDecompress(
    dcmp,
    unsafeAddr data[18], compLen.csize_t,
    addr result[0],      isize.csize_t,
    nil)
  if ret != LIBDEFLATE_SUCCESS:
    quit(&"decompressBgzf: deflate_decompress returned {ret}", 1)

proc decompressBgzfInto*(data: openArray[byte]; buf: var seq[byte]) =
  ## Decompress the first BGZF block into buf, resizing as needed.
  ## Reuses buf's allocation across calls — zero alloc in steady state.
  ## Use for hot paths that decompress, write, and discard (no accumulation).
  let blkSize = bgzfBlockSize(data)
  if blkSize < 0:
    quit("decompressBgzfInto: not a valid BGZF block header", 1)
  let isize = leU32(data, blkSize - 4).int
  if isize == 0:
    buf.setLen(0)
    return
  if buf.len < isize:
    buf = newSeqUninit[byte](isize)
  let dcmp = getDecompressor()
  let compLen = blkSize - BGZF_OVERHEAD
  let ret = libdeflateDeflateDecompress(
    dcmp,
    unsafeAddr data[18], compLen.csize_t,
    addr buf[0],          isize.csize_t,
    nil)
  if ret != LIBDEFLATE_SUCCESS:
    quit(&"decompressBgzfInto: deflate_decompress returned {ret}", 1)
  buf.setLen(isize)

# ---------------------------------------------------------------------------
# Thread-local BGZF compression buffer — shared by all compress functions.
# Fixed size covers the worst-case BGZF block (65536 input + deflate overhead).
# BGZF header template is written once on first allocation and never touched again.
# ---------------------------------------------------------------------------

const TL_COMPRESS_BUF_SIZE = 66560  # 65 KiB — fits any BGZF block
var tlCompressBuf {.threadvar.}: seq[byte]
var tlCompressBufReady {.threadvar.}: bool

proc ensureCompressBuf() {.inline.} =
  if not tlCompressBufReady:
    tlCompressBuf = newSeqUninit[byte](TL_COMPRESS_BUF_SIZE)
    tlCompressBuf[0] = 0x1f; tlCompressBuf[1] = 0x8b
    tlCompressBuf[2] = 0x08; tlCompressBuf[3] = 0x04
    tlCompressBuf[4] = 0; tlCompressBuf[5] = 0
    tlCompressBuf[6] = 0; tlCompressBuf[7] = 0
    tlCompressBuf[8] = 0; tlCompressBuf[9] = 0xff
    putLeU16(tlCompressBuf, 10, 6'u16)
    tlCompressBuf[12] = 0x42; tlCompressBuf[13] = 0x43
    putLeU16(tlCompressBuf, 14, 2'u16)
    tlCompressBufReady = true

proc compressBlockInto(cmp: pointer; data: pointer; dataLen: int): int {.inline.} =
  ## Compress one chunk into tlCompressBuf (header already set).
  ## Returns the total BGZF block size.
  let cdataLen = libdeflateDeflateCompress(
    cmp, data, dataLen.csize_t,
    addr tlCompressBuf[18], (TL_COMPRESS_BUF_SIZE - BGZF_OVERHEAD).csize_t).int
  if cdataLen == 0:
    quit("compressBlockInto: deflate_compress failed", 1)
  let totalSize = BGZF_OVERHEAD + cdataLen
  putLeU16(tlCompressBuf, 16, uint16(totalSize - 1))   # BSIZE - 1
  let crc = libdeflateCrc32(0'u32, data, dataLen.csize_t)
  putLeU32(tlCompressBuf, 18 + cdataLen, crc.uint32)   # CRC32
  putLeU32(tlCompressBuf, 18 + cdataLen + 4, dataLen.uint32)  # ISIZE
  return totalSize

proc compressToBgzf*(data: openArray[byte]; level: int = 6): seq[byte] =
  ## Compress data into a single BGZF block.  Returns a new seq.
  ## data.len must be <= BGZF_MAX_BLOCK_SIZE (65536).
  if data.len > BGZF_MAX_BLOCK_SIZE:
    quit(&"compressToBgzf: input too large ({data.len} > {BGZF_MAX_BLOCK_SIZE})", 1)
  ensureCompressBuf()
  let cmp = getCompressor(level)
  let inPtr = if data.len > 0: unsafeAddr data[0] else: nil
  let totalSize = compressBlockInto(cmp, inPtr, data.len)
  result = tlCompressBuf[0 ..< totalSize]

proc compressToBgzfMulti*(data: openArray[byte]; level: int = 6): seq[byte] =
  ## Compress data into one or more BGZF blocks, splitting every 65536 bytes.
  ## Returns the concatenated BGZF blocks as a seq[byte].
  result = @[]
  if data.len == 0:
    result.add(compressToBgzf(data, level))
    return
  var pos = 0
  while pos < data.len:
    let chunkEnd = min(pos + BGZF_MAX_BLOCK_SIZE, data.len)
    result.add(compressToBgzf(data[pos ..< chunkEnd], level))
    pos = chunkEnd

proc compressToBgzfMulti*(outFile: File; data: openArray[byte]; level: int = 6) =
  ## Compress data into BGZF blocks and write directly to outFile.
  ## Zero per-call allocation — compresses into thread-local buffer and writes.
  if data.len == 0:
    let z = compressToBgzf(data, level)
    discard outFile.writeBytes(z, 0, z.len)
    return
  ensureCompressBuf()
  let cmp = getCompressor(level)
  var pos = 0
  while pos < data.len:
    let chunkEnd = min(pos + BGZF_MAX_BLOCK_SIZE, data.len)
    let chunkLen = chunkEnd - pos
    let totalSize = compressBlockInto(cmp, unsafeAddr data[pos], chunkLen)
    discard outFile.writeBytes(tlCompressBuf, 0, totalSize)
    pos = chunkEnd

proc readBgzfBlockSize*(f: File; offset: int64): int64 =
  ## Read the 18-byte header of the BGZF block at offset and return its
  ## total size.  Calls quit(1) on short read or invalid header.  Reuses the
  ## caller's open File handle.
  var hdr {.noinit.}: array[18, byte]
  f.setFilePos(offset)
  if readBytes(f, hdr, 0, 18) < 18:
    quit(&"readBgzfBlockSize: short read at offset {offset}", 1)
  let sz = bgzfBlockSize(hdr).int64
  if sz <= 0:
    quit(&"readBgzfBlockSize: invalid BGZF block at offset {offset}", 1)
  return sz

proc splitBgzfBlockBothSides*(f: File; offset: int64; uOff: int):
    (seq[byte], seq[byte], int64) =
  ## Decompress the BGZF block at offset once and recompress both halves:
  ## head = data[0 ..< uOff], tail = data[uOff ..< len].  Returns
  ## (headBgzf, tailBgzf, blkSize).  Shared helper for `computeShards`
  ## boundary precomputation — the caller supplies the open File so a single
  ## handle is reused across all boundary splits.
  ## head is empty when uOff == 0; tail is empty when uOff >= data.len.
  let blkSize = readBgzfBlockSize(f, offset)
  var blk = newSeqUninit[byte](blkSize)
  f.setFilePos(offset)
  discard readBytes(f, blk, 0, blkSize.int)
  let data = decompressBgzf(blk)
  let split = min(uOff, data.len)
  let head = if split == 0: @[] else: compressToBgzfMulti(data[0 ..< split])
  let tail = if split >= data.len: @[] else: compressToBgzfMulti(data[split ..< data.len])
  result = (head, tail, blkSize)

proc decompressBgzfBytes*(data: openArray[byte]): seq[byte] =
  ## Decompress a sequence of concatenated BGZF blocks; return uncompressed bytes.
  ## Stops at the first invalid or incomplete block.
  result = @[]
  var pos = 0
  while pos + 18 <= data.len:
    let blkSize = bgzfBlockSize(data.toOpenArray(pos, data.high))
    if blkSize <= 0 or pos + blkSize > data.len: break
    result.add(decompressBgzf(data.toOpenArray(pos, pos + blkSize - 1)))
    pos += blkSize

proc decompressBgzfFile*(path: string): seq[byte] =
  ## Decompress an entire BGZF file into a single contiguous byte sequence.
  ## Reads the whole file once and walks concatenated BGZF blocks in memory.
  let raw = readFile(path)
  result = decompressBgzfBytes(raw.toOpenArrayByte(0, raw.len - 1))

proc decompressCopyBytes*(srcPath: string; dst: File; start: int64; length: int64) =
  ## Read BGZF blocks from [start, start+length) in srcPath, decompress each,
  ## and write the raw uncompressed bytes to dst.
  ## Uses reusable buffers to avoid per-block allocation.
  let src = open(srcPath, fmRead)
  defer: src.close()
  src.setFilePos(start)
  var cur = start
  let endAt = start + length
  var decompBuf: seq[byte]
  # Reusable block buffer — grows to max block size, never shrinks.
  var blk = newSeqUninit[byte](BGZF_MAX_BLOCK_SIZE + BGZF_OVERHEAD)
  while cur + 18 <= endAt:
    # Read header into blk, then read rest of block — single seek per call.
    if readBytes(src, blk, 0, 18) < 18: break
    let blkSize = bgzfBlockSize(blk)
    if blkSize <= 0 or cur + blkSize.int64 > endAt: break
    if blkSize > blk.len: blk.setLen(blkSize)
    # Read remaining bytes after the 18-byte header already in blk.
    if blkSize > 18:
      if readBytes(src, blk, 18, blkSize - 18) < blkSize - 18: break
    decompressBgzfInto(blk.toOpenArray(0, blkSize - 1), decompBuf)
    if decompBuf.len > 0:
      discard dst.writeBytes(decompBuf, 0, decompBuf.len)
    cur += blkSize.int64

# ---------------------------------------------------------------------------
# Streaming BGZF compress / decompress (File → File)
# ---------------------------------------------------------------------------

proc bgzfCompressStream*(inFile, outFile: File; level: int = 6) =
  ## Read raw bytes from inFile, compress into BGZF blocks, write to outFile.
  ## Appends a BGZF EOF block at the end.  Uses thread-local codec cache.
  var buf = newSeqUninit[byte](BGZF_MAX_BLOCK_SIZE)
  while true:
    let n = readBytes(inFile, buf, 0, BGZF_MAX_BLOCK_SIZE)
    if n == 0: break
    compressToBgzfMulti(outFile, buf.toOpenArray(0, n - 1), level)
  discard outFile.writeBytes(BGZF_EOF, 0, BGZF_EOF.len)

proc bgzfDecompressStream*(inFile, outFile: File) =
  ## Read BGZF blocks from inFile, decompress each, write raw bytes to outFile.
  ## Skips EOF blocks (ISIZE=0).  Uses thread-local decompressor and reusable
  ## block buffer (grows once, never shrinks).
  var blk = newSeqUninit[byte](BGZF_MAX_BLOCK_SIZE + BGZF_OVERHEAD)
  var decompBuf: seq[byte]
  while true:
    let hdrRead = readBytes(inFile, blk, 0, 18)
    if hdrRead < 18: break
    let blkSize = bgzfBlockSize(blk)
    if blkSize <= 0: break
    if blkSize > blk.len: blk.setLen(blkSize)
    if blkSize > 18:
      let rest = readBytes(inFile, blk, 18, blkSize - 18)
      if rest < blkSize - 18: break
    # Skip EOF blocks (ISIZE = 0 in last 4 bytes).
    let isize = leU32(blk, blkSize - 4)
    if isize == 0: continue
    decompressBgzfInto(blk.toOpenArray(0, blkSize - 1), decompBuf)
    if decompBuf.len > 0:
      discard outFile.writeBytes(decompBuf, 0, decompBuf.len)

# ---------------------------------------------------------------------------
# Index parsing — TBI / CSI virtual offsets
# ---------------------------------------------------------------------------

proc parseTbiVirtualOffsets*(tbiPath: string): seq[(int64, int)] =
  ## Parse a .tbi index and return sorted unique virtual offsets as
  ## (block_file_offset, within_block_offset) pairs for all chunk starts.
  let raw = decompressBgzfFile(tbiPath)
  if raw.len < 8 or raw[0] != byte('T') or raw[1] != byte('B') or
     raw[2] != byte('I') or raw[3] != 0x01:
    quit(&"parseTbiVirtualOffsets: not a valid TBI index: {tbiPath}", 1)
  var pos = 4
  let nRef = readLeI32(raw, pos).int
  pos += 24                            # skip 6 int32s
  let lNm = readLeI32(raw, pos).int
  pos += lNm                          # skip sequence-name block
  var offsets: seq[(int64, int)]
  for _ in 0 ..< nRef:
    let nBin = readLeU32(raw, pos).int
    for _ in 0 ..< nBin:
      pos += 4  # skip bin_id
      let nChunk = readLeU32(raw, pos).int
      for _ in 0 ..< nChunk:
        let beg    = readLeU64(raw, pos)
        let endOff = readLeU64(raw, pos)
        if endOff > beg:
          offsets.add(((beg shr 16).int64, (beg and 0xFFFF).int))
    let nIntv = readLeU32(raw, pos).int
    pos += 8 * nIntv  # skip linear index intervals
  offsets.sort(proc(a, b: (int64, int)): int =
    if a[0] != b[0]: cmp(a[0], b[0]) else: cmp(a[1], b[1]))
  var deduped: seq[(int64, int)]
  for i, v in offsets:
    if i == 0 or v != offsets[i - 1]:
      deduped.add(v)
  result = deduped

proc parseCsiVirtualOffsets*(csiPath: string): seq[(int64, int)] =
  ## Parse a .csi index and return sorted unique virtual offsets as
  ## (block_file_offset, within_block_offset) pairs for all chunk starts.
  let raw = decompressBgzfFile(csiPath)
  if raw.len < 8 or raw[0] != byte('C') or raw[1] != byte('S') or
     raw[2] != byte('I') or raw[3] != 0x01:
    quit(&"parseCsiVirtualOffsets: not a valid CSI index: {csiPath}", 1)
  var pos = 4
  pos += 8               # skip min_shift (int32) + depth (int32)
  let lAux = readLeI32(raw, pos).int
  pos += lAux
  let nRef = readLeI32(raw, pos).int
  var offsets: seq[(int64, int)]
  for _ in 0 ..< nRef:
    let nBin = readLeI32(raw, pos).int
    for _ in 0 ..< nBin:
      pos += 4   # skip bin (uint32)
      pos += 8   # skip loffset (uint64)
      let nChunk = readLeI32(raw, pos).int
      for _ in 0 ..< nChunk:
        let beg    = readLeU64(raw, pos)
        let endOff = readLeU64(raw, pos)
        if endOff > beg:
          offsets.add(((beg shr 16).int64, (beg and 0xFFFF).int))
  offsets.sort(proc(a, b: (int64, int)): int =
    if a[0] != b[0]: cmp(a[0], b[0]) else: cmp(a[1], b[1]))
  var deduped: seq[(int64, int)]
  for i, v in offsets:
    if i == 0 or v != offsets[i - 1]:
      deduped.add(v)
  result = deduped

proc parseGziBlockStarts*(gziPath: string): seq[int64] =
  ## Parse a .gzi index and return sorted BGZF block start offsets.
  ## GZI format: uint64 count, then count pairs of (compressed_off, uncompressed_off)
  ## as uint64 LE.  Only compressed offsets are used — GZI is a scan shortcut,
  ## not a virtual-offset index.
  let data = cast[seq[byte]](readFile(gziPath))
  if data.len < 8:
    quit(&"parseGziBlockStarts: file too small: {gziPath}", 1)
  var pos = 0
  let count = readLeU64(data, pos).int
  if data.len < 8 + count * 16:
    quit(&"parseGziBlockStarts: truncated .gzi file: {gziPath} (expected {8 + count * 16} bytes, got {data.len})", 1)
  result = newSeqOfCap[int64](count + 1)
  result.add(0'i64)  # first block at offset 0 is implicit in GZI format
  for _ in 0 ..< count:
    let compOff = readLeU64(data, pos).int64
    discard readLeU64(data, pos)  # skip cumulative uncompressed offset
    result.add(compOff)
  result.sort()

proc readIndexVirtualOffsets*(vcfPath: string): seq[(int64, int)] =
  ## Detect a .csi or .tbi index and return sorted unique virtual offsets as
  ## (block_file_offset, within_block_offset) pairs. Returns @[] if no index.
  let csi = vcfPath & ".csi"
  let tbi = vcfPath & ".tbi"
  if fileExists(csi):
    result = parseCsiVirtualOffsets(csi)
  elif fileExists(tbi):
    result = parseTbiVirtualOffsets(tbi)
  else:
    result = @[]

# ---------------------------------------------------------------------------
# Header extraction — BCF and VCF
# ---------------------------------------------------------------------------

proc extractBcfHeaderAndFirstOffset*(path: string): (seq[byte], int64, int) =
  ## Walk BGZF blocks from the start of path to extract the BCF header and
  ## locate the first data record in one pass.  Returns (uncompressed header
  ## bytes, first-record block file offset, first-record uncompressed byte
  ## offset within that block).  Verifies the BCF magic and calls quit(1) on
  ## any format error.  No full-file scan — only reads enough blocks to cover
  ## the header plus the first record byte.
  let f = open(path, fmRead)
  defer: f.close()
  var accum: seq[byte]
  var lText = -1'i64
  var headerSize = -1'i64   # 5 + 4 + l_text
  var off = 0'i64
  var cumUncomp = 0'i64
  var hdr = newSeqUninit[byte](18)
  while true:
    f.setFilePos(off)
    if readBytes(f, hdr, 0, 18) < 18: break
    let blkSize = bgzfBlockSize(hdr)
    if blkSize <= 0: break
    var blk = newSeqUninit[byte](blkSize)
    f.setFilePos(off)
    discard readBytes(f, blk, 0, blkSize)
    let content = decompressBgzf(blk)
    let blockLen = content.len.int64
    accum.add(content)
    if lText < 0 and accum.len >= 9:
      for i in 0 ..< 5:
        if accum[i] != BCF_MAGIC[i]:
          quit(&"extractBcfHeaderAndFirstOffset: {path}: invalid BCF magic", 1)
      var p = 5
      lText = readLeU32(accum, p).int64
      headerSize = 5'i64 + 4'i64 + lText
    if headerSize >= 0 and cumUncomp + blockLen > headerSize:
      # First record byte lives in this block.
      let firstUOff = (headerSize - cumUncomp).int
      return (accum[0 ..< headerSize.int], off, firstUOff)
    cumUncomp += blockLen
    off += blkSize.int64
  if headerSize < 0:
    quit(&"extractBcfHeaderAndFirstOffset: {path}: file too short to read BCF header", 1)
  quit(&"extractBcfHeaderAndFirstOffset: {path}: first record not found in file", 1)

proc blockHasData(content: openArray[byte]; prevEndedWithNewline: bool): bool =
  ## Return true if content contains at least one complete line not starting
  ## with '#'.  prevEndedWithNewline must be true if the previous BGZF block
  ## ended with '\n' (i.e. the first byte of content is the start of a new
  ## line); if false, the first partial line is a continuation from the
  ## previous block and is skipped.
  var lineStart = 0
  # Skip the partial first line when we are mid-line from the previous block.
  if not prevEndedWithNewline:
    var found = false
    for i in 0 ..< content.len:
      if content[i] == byte('\n'):
        lineStart = i + 1
        found = true
        break
    if not found:
      return false   # entire block is a line continuation — no complete lines
  # Check each complete line (terminated by '\n').
  for i in lineStart ..< content.len:
    if content[i] == byte('\n'):
      if i > lineStart and content[lineStart] != byte('#'):
        return true
      lineStart = i + 1
  # Do NOT check the partial last line: it may be the start of a header line
  # whose '#' character lives in the next block.
  return false

proc getHeaderAndFirstBlock*(vcfPath: string): (seq[byte], int64) =
  ## Scan BGZF blocks to collect all VCF header lines ('#' lines) and locate
  ## the first block containing data.  No htslib dependency — reads raw bytes.
  ## Returns (header recompressed as BGZF, first-data-block file offset).
  ## Handles long header lines spanning BGZF block boundaries correctly.
  let f = open(vcfPath, fmRead)
  defer: f.close()
  var hdrBuf = newSeqUninit[byte](18)
  if readBytes(f, hdrBuf, 0, 18) < 18 or bgzfBlockSize(hdrBuf) < 0:
    stderr.writeLine &"error: no BGZF blocks found in {vcfPath}"
    quit(1)
  var headerBytes: seq[byte]
  var pos = 0'i64
  var prevEndedWithNewline = true   # start of file is always a line boundary
  while true:
    f.setFilePos(pos)
    if readBytes(f, hdrBuf, 0, 18) < 18: break
    let blkSize = bgzfBlockSize(hdrBuf)
    if blkSize <= 0: break
    var blk = newSeqUninit[byte](blkSize)
    f.setFilePos(pos)
    discard readBytes(f, blk, 0, blkSize)
    let content = decompressBgzf(blk)
    if content.len > 0:
      if blockHasData(content, prevEndedWithNewline):
        # First data block — extract any leading '#' lines to complete the header.
        # If the previous block ended mid-line, the continuation bytes here are
        # part of a '#' line already started in headerBytes; include them up to '\n'.
        var i = 0
        if not prevEndedWithNewline:
          while i < content.len:
            headerBytes.add(content[i])
            if content[i] == byte('\n'):
              i += 1
              break
            i += 1
        var lineStart = i
        while i < content.len:
          if content[i] == byte('\n'):
            if i > lineStart and content[lineStart] == byte('#'):
              for j in lineStart .. i: headerBytes.add(content[j])
            lineStart = i + 1
          i += 1
        let compressedHeader = compressToBgzfMulti(headerBytes)
        return (compressedHeader, pos)
      else:
        # Pure header block — collect all decompressed bytes.
        headerBytes.add(content)
        prevEndedWithNewline = content[^1] == byte('\n')
    pos += blkSize.int64
  # Edge case: file has no data records (header only).
  let compressedHeader = compressToBgzfMulti(headerBytes)
  result = (compressedHeader, pos)

# ---------------------------------------------------------------------------
# Format sniffing
# ---------------------------------------------------------------------------

proc isBgzfStream*(firstBytes: openArray[byte]): bool =
  ## Return true if firstBytes begins with a BGZF block header (magic 1f 8b 08 04).
  firstBytes.len >= BGZF_MAGIC.len and
  firstBytes[0] == BGZF_MAGIC[0] and firstBytes[1] == BGZF_MAGIC[1] and
  firstBytes[2] == BGZF_MAGIC[2] and firstBytes[3] == BGZF_MAGIC[3]

proc sniffFormat*(firstBytes: openArray[byte]): FileFormat =
  ## Detect format from uncompressed first bytes of a stream.
  ## BCF\x02\x02 → ffBcf; ##fileformat → ffVcf; anything else → ffText.
  if firstBytes.len >= BCF_MAGIC.len and
     firstBytes[0] == BCF_MAGIC[0] and firstBytes[1] == BCF_MAGIC[1] and
     firstBytes[2] == BCF_MAGIC[2] and firstBytes[3] == BCF_MAGIC[3] and
     firstBytes[4] == BCF_MAGIC[4]:
    return ffBcf
  const vcfMagic = "##fileformat"
  if firstBytes.len >= vcfMagic.len:
    var match = true
    for i in 0 ..< vcfMagic.len:
      if firstBytes[i] != byte(vcfMagic[i]):
        match = false
        break
    if match:
      return ffVcf
  result = ffText

proc sniffStreamFormat*(rawHead: openArray[byte]): (FileFormat, bool) =
  ## Detect format and stream compression from the first bytes of a pipeline stdout.
  ## rawHead must contain at least the first complete BGZF block if the stream is BGZF.
  ## Returns (format, isBgzf).
  if isBgzfStream(rawHead):
    let decompressed = decompressBgzf(rawHead)
    result = (sniffFormat(decompressed), true)
  else:
    result = (sniffFormat(rawHead), false)

proc sniffFileFormat*(path: string): (FileFormat, bool) =
  ## Detect format and compression of a file on disk by reading its first bytes.
  ## Returns (ffVcf|ffBcf, isCompressed). Exits 1 on I/O error.
  var f: File
  if not open(f, path, fmRead):
    stderr.writeLine "error: cannot open file: " & path
    quit(1)
  var head: array[65536, byte]
  let nRead = f.readBytes(head, 0, head.len)
  f.close()
  if nRead == 0:
    stderr.writeLine "error: file is empty: " & path
    quit(1)
  result = sniffStreamFormat(head[0 ..< nRead])

proc inferInputFormat*(path: string): FileFormat =
  ## Infer the format of a scatter/run input file.
  ## Fast path: recognised extensions (.bcf, .vcf.gz, .vcf.bgz, .vcf).
  ## Fallback: sniff the file's magic bytes. Exits 1 on I/O error.
  if path.endsWith(".bcf"):
    return ffBcf
  if path.endsWith(".vcf.gz") or path.endsWith(".vcf.bgz") or
     path.endsWith(".vcf"):
    return ffVcf
  let (fmt, _) = sniffFileFormat(path)
  return fmt
