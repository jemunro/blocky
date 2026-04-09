## vcf_utils — BGZF I/O, VCF/BCF format types, and format sniffing.
##
## Only external dependency: libdeflate (-ldeflate), no htslib required.
## All proc signatures use explicit types per project style guide.

import std/[strformat]

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
# Internal helpers
# ---------------------------------------------------------------------------

proc leU16(buf: openArray[byte]; pos: int): uint16 {.inline.} =
  ## Read a little-endian uint16 from buf at pos.
  buf[pos].uint16 or (buf[pos + 1].uint16 shl 8)

proc leU32*(buf: openArray[byte]; pos: int): uint32 {.inline.} =
  ## Read a little-endian uint32 from buf at pos.
  buf[pos].uint32 or (buf[pos+1].uint32 shl 8) or
  (buf[pos+2].uint32 shl 16) or (buf[pos+3].uint32 shl 24)

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
  var buf = newSeq[byte](18)
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

proc rawCopyBytes*(srcPath: string; dst: File; start: int64; length: int64) =
  ## Copy length bytes from srcPath starting at start into the open file dst.
  ## Uses 4 MiB read chunks for I/O efficiency.
  let src = open(srcPath, fmRead)
  defer: src.close()
  src.setFilePos(start)
  const ChunkSize = 4 * 1024 * 1024
  var buf = newSeq[byte](ChunkSize)
  var remaining = length
  while remaining > 0:
    let toRead = min(remaining, ChunkSize.int64).int
    let nRead = readBytes(src, buf, 0, toRead)
    if nRead == 0: break
    discard dst.writeBytes(buf, 0, nRead)
    remaining -= nRead.int64

proc decompressBgzf*(data: openArray[byte]): seq[byte] =
  ## Decompress the first BGZF block in data; return the uncompressed bytes.
  ## Calls quit(1) on malformed input.
  let blkSize = bgzfBlockSize(data)
  if blkSize < 0:
    quit("decompressBgzf: not a valid BGZF block header", 1)
  let isize = leU32(data, blkSize - 4).int
  if isize == 0:
    return @[]
  result = newSeq[byte](isize)
  let dcmp = libdeflateAllocDecompressor()
  if dcmp == nil:
    quit("decompressBgzf: alloc_decompressor returned nil", 1)
  let compLen = blkSize - BGZF_OVERHEAD
  let ret = libdeflateDeflateDecompress(
    dcmp,
    unsafeAddr data[18], compLen.csize_t,
    addr result[0],      isize.csize_t,
    nil)
  libdeflateFreeDecompressor(dcmp)
  if ret != LIBDEFLATE_SUCCESS:
    quit(&"decompressBgzf: deflate_decompress returned {ret}", 1)

proc decompressBgzfFile*(path: string): seq[byte] =
  ## Decompress an entire BGZF file into a single contiguous byte sequence.
  ## Iterates all blocks in order; EOF blocks (ISIZE=0) contribute nothing.
  result = @[]
  let starts = scanBgzfBlockStarts(path)
  let f = open(path, fmRead)
  defer: f.close()
  var buf = newSeq[byte](18)
  for off in starts:
    f.setFilePos(off)
    discard readBytes(f, buf, 0, 18)
    let blkSize = bgzfBlockSize(buf)
    if blkSize <= 0: break
    var blk = newSeq[byte](blkSize)
    f.setFilePos(off)
    discard readBytes(f, blk, 0, blkSize)
    result.add(decompressBgzf(blk))

proc compressToBgzf*(data: openArray[byte]; level: int = 6): seq[byte] =
  ## Compress data into a single valid BGZF block using raw deflate.
  ## Builds the BGZF header (BC extra field, CRC32, ISIZE) manually.
  ## data.len must be <= BGZF_MAX_BLOCK_SIZE (65536).
  if data.len > BGZF_MAX_BLOCK_SIZE:
    quit(&"compressToBgzf: input too large ({data.len} > {BGZF_MAX_BLOCK_SIZE})", 1)
  let cmp = libdeflateAllocCompressor(level.cint)
  if cmp == nil:
    quit("compressToBgzf: alloc_compressor returned nil", 1)
  let bound = libdeflateDeflateCompressBound(cmp, data.len.csize_t).int
  var cdata = newSeq[byte](bound)
  let inPtr = if data.len > 0: unsafeAddr data[0] else: nil
  let cdataLen = libdeflateDeflateCompress(
    cmp, inPtr, data.len.csize_t, addr cdata[0], bound.csize_t).int
  libdeflateFreeCompressor(cmp)
  if cdataLen == 0:
    quit("compressToBgzf: deflate_compress failed", 1)
  # Compute CRC32 of the original uncompressed data.
  let crc = libdeflateCrc32(0'u32, inPtr, data.len.csize_t)
  # Build the BGZF block: 18-byte header + cdata + CRC32 + ISIZE.
  let totalSize = BGZF_OVERHEAD + cdataLen
  result = newSeq[byte](totalSize)
  result[0] = 0x1f; result[1] = 0x8b; result[2] = 0x08; result[3] = 0x04
  # MTIME=0, XFL=0, OS=0xff
  result[8] = 0x00; result[9] = 0xff
  putLeU16(result, 10, 6'u16)                        # XLEN = 6
  result[12] = 0x42; result[13] = 0x43               # SI1='B', SI2='C'
  putLeU16(result, 14, 2'u16)                        # SLEN = 2
  putLeU16(result, 16, uint16(totalSize - 1))        # BSIZE - 1
  copyMem(addr result[18], addr cdata[0], cdataLen)
  putLeU32(result, 18 + cdataLen,     crc.uint32)    # CRC32
  putLeU32(result, 18 + cdataLen + 4, data.len.uint32)  # ISIZE

proc compressToBgzfMulti*(data: openArray[byte]; level: int = 6): seq[byte] =
  ## Compress data into one or more BGZF blocks, splitting every 65536 bytes.
  ## Use this instead of compressToBgzf when the input may exceed 65536 bytes
  ## (e.g. a large VCF header).
  result = @[]
  if data.len == 0:
    result.add(compressToBgzf(data, level))
    return
  var pos = 0
  while pos < data.len:
    let chunkEnd = min(pos + BGZF_MAX_BLOCK_SIZE, data.len)
    result.add(compressToBgzf(data[pos ..< chunkEnd], level))
    pos = chunkEnd

proc splitBgzfBlockAtUOffset*(path: string; offset: int64; uOff: int): (seq[byte], seq[byte]) =
  ## Decompress the BGZF block at file offset and split the uncompressed data at
  ## byte position uOff.  Returns (head, tail) where head = data[0 ..< uOff] and
  ## tail = data[uOff ..< len], each recompressed as BGZF.
  ## head is empty when uOff == 0; tail is empty when uOff >= data.len.
  let f = open(path, fmRead)
  defer: f.close()
  var hdr = newSeq[byte](18)
  f.setFilePos(offset)
  if readBytes(f, hdr, 0, 18) < 18:
    quit(&"splitBgzfBlockAtUOffset: {path}: short read at offset {offset}", 1)
  let blkSize = bgzfBlockSize(hdr)
  if blkSize <= 0:
    quit(&"splitBgzfBlockAtUOffset: {path}: invalid BGZF block at offset {offset}", 1)
  var blk = newSeq[byte](blkSize)
  f.setFilePos(offset)
  discard readBytes(f, blk, 0, blkSize)
  let data = decompressBgzf(blk)
  let split = min(uOff, data.len)
  let head = if split == 0: @[] else: compressToBgzfMulti(data[0 ..< split])
  let tail = if split >= data.len: @[] else: compressToBgzfMulti(data[split ..< data.len])
  result = (head, tail)

proc bcfFirstDataVirtualOffset*(path: string): (int64, int) =
  ## Return the virtual offset (file_offset, u_off) of the first BCF record.
  ## file_offset is the BGZF block file offset; u_off is the uncompressed byte
  ## offset within that block where the first record starts.
  let starts = scanBgzfBlockStarts(path)
  let f = open(path, fmRead)
  defer: f.close()
  var lText = -1'i64
  var firstRecordUncompOff = -1'i64
  var cumUncomp = 0'i64
  var headerBuf: seq[byte]
  for off in starts:
    var hdr = newSeq[byte](18)
    f.setFilePos(off)
    if readBytes(f, hdr, 0, 18) < 18: break
    let blkSize = bgzfBlockSize(hdr)
    if blkSize <= 0: break
    var blk = newSeq[byte](blkSize)
    f.setFilePos(off)
    discard readBytes(f, blk, 0, blkSize)
    let decompressed = decompressBgzf(blk)
    let blockLen = decompressed.len.int64
    if lText < 0:
      headerBuf.add(decompressed)
      if headerBuf.len >= 9:
        if headerBuf[0] != byte('B') or headerBuf[1] != byte('C') or
           headerBuf[2] != byte('F') or headerBuf[3] != 0x02'u8 or
           headerBuf[4] != 0x02'u8:
          quit(&"bcfFirstDataVirtualOffset: {path}: not a BCF file (bad magic)", 1)
        lText = leU32(headerBuf, 5).int64
        firstRecordUncompOff = 5'i64 + 4'i64 + lText
    if firstRecordUncompOff >= 0 and cumUncomp + blockLen > firstRecordUncompOff:
      let uOff = (firstRecordUncompOff - cumUncomp).int
      return (off, uOff)
    cumUncomp += blockLen
  if firstRecordUncompOff < 0:
    quit(&"bcfFirstDataVirtualOffset: {path}: file too short to read BCF header", 1)
  quit(&"bcfFirstDataVirtualOffset: {path}: first record not found in file", 1)

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

proc decompressCopyBytes*(srcPath: string; dst: File; start: int64; length: int64) =
  ## Read BGZF blocks from [start, start+length) in srcPath, decompress each,
  ## and write the raw uncompressed bytes to dst.
  let src = open(srcPath, fmRead)
  defer: src.close()
  var cur = start
  let endAt = start + length
  var hdrBuf = newSeq[byte](18)
  while cur + 18 <= endAt:
    src.setFilePos(cur)
    if readBytes(src, hdrBuf, 0, 18) < 18: break
    let blkSize = bgzfBlockSize(hdrBuf)
    if blkSize <= 0 or cur + blkSize.int64 > endAt: break
    var blk = newSeq[byte](blkSize)
    src.setFilePos(cur)
    discard readBytes(src, blk, 0, blkSize)
    let decompressed = decompressBgzf(blk)
    if decompressed.len > 0:
      discard dst.writeBytes(decompressed, 0, decompressed.len)
    cur += blkSize.int64

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
