## gather — format inference, interceptor, and concatenation for --gather.
##
## Implements the gather pipeline:
##   inferFileFormat → GatherConfig → runInterceptor (per shard) → concatenateShards

import std/[heapqueue, options, os, strformat, strutils, tables]
import std/posix
{.warning[Deprecated]: off.}
import std/threadpool
{.warning[Deprecated]: on.}
import vcf_utils

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

type
  GatherConfig* = object
    format*:        FileFormat
    compression*:   Compression
    outputPath*:    string
    tmpDir*:        string
    headerPattern*: Option[string]   ## --header-pattern; None = not set
    headerN*:       Option[int]      ## --header-n; None = not set
    shardCount*:    int
    toStdout*:      bool             ## write output to stdout instead of outputPath

# ---------------------------------------------------------------------------
# Format inference
# ---------------------------------------------------------------------------

proc inferFileFormat*(path: string; fmtOverride: string): (FileFormat, Compression) =
  ## Infer format and compression from the gather output path extension.
  ## fmtOverride ("vcf", "bcf", "txt") overrides format when non-empty; exits 1 if invalid.
  ## Compression: .gz / .bgz / .bcf → compBgzf; anything else → compNone.
  ## Format (when no override):
  ##   .vcf.gz / .vcf.bgz → VCF+BGZF
  ##   .vcf               → VCF+Uncompressed
  ##   .bcf               → BCF+BGZF
  ##   .gz / .bgz         → Text+BGZF
  ##   anything else      → Text+Uncompressed (no error)

  # Compression is solely determined by the output path extension.
  let compression =
    if path.endsWith(".gz") or path.endsWith(".bgz") or path.endsWith(".bcf"): compBgzf
    else: compNone

  # Format is from override if supplied; otherwise inferred from extension.
  var fmt: FileFormat
  if fmtOverride != "":
    case fmtOverride
    of "vcf": fmt = ffVcf
    of "bcf": fmt = ffBcf
    of "txt": fmt = ffText
    else:
      stderr.writeLine &"error: --gather-fmt: invalid value '{fmtOverride}'" &
        " (expected vcf, bcf, or txt)"
      quit(1)
  else:
    if path.endsWith(".vcf.gz") or path.endsWith(".vcf.bgz"):
      fmt = ffVcf
    elif path.endsWith(".vcf"):
      fmt = ffVcf
    elif path.endsWith(".bcf"):
      fmt = ffBcf
    elif path.endsWith(".gz") or path.endsWith(".bgz"):
      fmt = ffText
    else:
      fmt = ffText  # unknown extension → text, no error

  result = (fmt, compression)

# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

proc validateGatherConfig*(cfg: GatherConfig) =
  ## Exits 1 if both headerPattern and headerN are set (mutually exclusive flags).
  ## Exits 1 if headerPattern or headerN is used with VCF or BCF format (text-only flags).
  if cfg.headerPattern.isSome and cfg.headerN.isSome:
    stderr.writeLine "error: --header-pattern and --header-n are mutually exclusive"
    quit(1)
  if cfg.format in {ffVcf, ffBcf} and (cfg.headerPattern.isSome or cfg.headerN.isSome):
    stderr.writeLine "error: --header-pattern and --header-n are only valid for text " &
      "format; VCF and BCF headers are stripped automatically"
    quit(1)

# ---------------------------------------------------------------------------
# G2 — GC-safe cross-thread shared buffer type
# ---------------------------------------------------------------------------

type SharedBuf* = object
  ## GC-safe cross-thread buffer: fixed byte array + populated length + ready flag.
  ## Writers set .buf and .len first, then .ready = true as the release signal.
  ## Readers spin on .ready before accessing .buf or .len.
  buf*:   array[4 * 1024 * 1024, byte]
  len*:   int32
  ready*: bool

# ---------------------------------------------------------------------------
# G2 — Global detected format (set by shard 0 interceptor, read by shards 1..N)
# ---------------------------------------------------------------------------

var gDetectedFormat*: FileFormat   ## Format detected from shard 0 stream.
var gStreamIsBgzf*:   bool = false   ## Whether shard 0 stream was BGZF-compressed.
## gChromLine — #CHROM line captured by shard 0. gChromLine.ready also serves
## as the format-detection release flag for shards 1..N: they spin on it before
## reading gDetectedFormat or gStreamIsBgzf.
var gChromLine*: SharedBuf

## M5 merge globals — set by shard 0 feeder, read by main thread.
var gMergeFormat*:     FileFormat = ffVcf
var gMergeHeader*:     SharedBuf
var gMergeBgzfWarned*: bool = false

# ---------------------------------------------------------------------------
# G3 — Header stripping helpers
# ---------------------------------------------------------------------------

proc decompressAllBgzfBlocks*(data: openArray[byte]): seq[byte] =
  ## Decompress every BGZF block in data into a single contiguous byte sequence.
  ## EOF blocks (ISIZE = 0) contribute nothing to the result.
  result = @[]
  var pos = 0
  while pos + 18 <= data.len:
    let blkSize = bgzfBlockSize(data.toOpenArray(pos, data.high))
    if blkSize <= 0 or pos + blkSize > data.len: break
    result.add(decompressBgzf(data.toOpenArray(pos, pos + blkSize - 1)))
    pos += blkSize

proc flushBgzfAccum*(rawAccum: var seq[byte]; bgzfPos: var int;
                     pending: var seq[byte]) =
  ## Decompress all complete BGZF blocks from rawAccum[bgzfPos..] into pending.
  while bgzfPos + 18 <= rawAccum.len:
    let blkSize = bgzfBlockSize(rawAccum.toOpenArray(bgzfPos, rawAccum.high))
    if blkSize <= 0 or bgzfPos + blkSize > rawAccum.len: break
    pending.add(decompressBgzf(rawAccum.toOpenArray(bgzfPos, bgzfPos + blkSize - 1)))
    bgzfPos += blkSize

proc appendReadToAccum*(data: openArray[byte]; n: int; isBgzf: bool;
                        rawAccum: var seq[byte]; bgzfPos: var int;
                        pending: var seq[byte]) =
  ## Append n bytes from data[], routing through BGZF decompression if isBgzf.
  if isBgzf:
    rawAccum.add(data[0 ..< n])
    flushBgzfAccum(rawAccum, bgzfPos, pending)
  else:
    let base = pending.len
    pending.setLen(base + n)
    copyMem(addr pending[base], unsafeAddr data[0], n)

proc stripBcfHeader*(data: seq[byte]): seq[byte] =
  ## Strip BCF header (magic + l_text + header text) from uncompressed BCF bytes.
  ## Returns only the binary record bytes that follow the header.
  ## Returns @[] if data is too short to contain the full header or has no records.
  if data.len < 9: return @[]
  let lText = leU32(data, 5).int
  let headerSize = 5 + 4 + lText
  if headerSize >= data.len: return @[]
  result = data[headerSize ..< data.len]

proc stripLinesByPattern*(data: seq[byte]; pattern: string): seq[byte] =
  ## Return data with every line whose first bytes match pattern removed.
  ## Lines shorter than pattern are kept. An empty pattern strips nothing.
  result = @[]
  if pattern.len == 0:
    result = data
    return
  var lineStart = 0
  for i in 0 ..< data.len:
    if data[i] == byte('\n'):
      let lineEnd = i + 1
      var match = (lineEnd - lineStart) >= pattern.len
      if match:
        for j in 0 ..< pattern.len:
          if data[lineStart + j] != byte(pattern[j]):
            match = false
            break
      if not match:
        result.add(data[lineStart ..< lineEnd])
      lineStart = lineEnd
  # Handle a trailing partial line (no final newline).
  if lineStart < data.len:
    let partial = data[lineStart ..< data.len]
    var match = partial.len >= pattern.len
    if match:
      for j in 0 ..< pattern.len:
        if partial[j] != byte(pattern[j]):
          match = false
          break
    if not match:
      result.add(partial)

proc stripFirstNLines*(data: seq[byte]; n: int): seq[byte] =
  ## Return data with the first n lines (including their newline) removed.
  var linesSkipped = 0
  var i = 0
  while i < data.len and linesSkipped < n:
    if data[i] == byte('\n'):
      linesSkipped += 1
    i += 1
  if i < data.len:
    result = data[i ..< data.len]
  else:
    result = @[]

# ---------------------------------------------------------------------------
# G3 — Header-end finders (used by the optimised BGZF shard path)
# ---------------------------------------------------------------------------

proc findVcfHeaderEnd*(data: seq[byte]): int =
  ## Return the byte index of the first byte of the first non-'#' line in data.
  ## Returns -1 if every byte seen so far belongs to '#' lines (header not yet complete).
  var i = 0
  while i < data.len:
    if data[i] != byte('#'):
      return i
    while i < data.len and data[i] != byte('\n'):
      i += 1
    i += 1  # skip '\n'
  return -1

proc findBcfHeaderEnd*(data: seq[byte]): int =
  ## Return 5 + 4 + l_text if data contains the full BCF header blob, else -1.
  if data.len < 9: return -1
  let headerEnd = 5 + 4 + leU32(data, 5).int
  if data.len >= headerEnd: return headerEnd
  return -1

# ---------------------------------------------------------------------------
# M1 — Contig table extraction
# ---------------------------------------------------------------------------

proc extractContigTable*(headerBytes: seq[byte]): seq[string] =
  ## Return ordered contig names from a VCF or BCF header byte sequence.
  ## headerBytes may be BGZF-compressed (auto-detected) or raw uncompressed.
  ## For VCF: extracts ID values from ##contig=<ID=...> lines.
  ## For BCF: reads the embedded text header blob and applies the same parsing.
  result = @[]

  # Decompress if BGZF.
  var text: seq[byte]
  if isBgzfStream(headerBytes):
    text = decompressAllBgzfBlocks(headerBytes)
  else:
    text = headerBytes

  # For BCF: skip the binary preamble (5 magic + 4 l_text) to reach the text blob.
  if text.len >= 9 and
     text[0] == byte('B') and text[1] == byte('C') and text[2] == byte('F') and
     text[3] == 0x02'u8 and text[4] == 0x02'u8:
    let lText = leU32(text, 5).int
    let endPos = min(9 + lText, text.len)
    text = text[9 ..< endPos]

  # Parse ##contig=<ID=...> lines.
  const prefix = "##contig=<ID="
  var i = 0
  while i < text.len:
    var j = i
    while j < text.len and text[j] != byte('\n'): j += 1
    let lineLen = j - i
    if lineLen >= prefix.len:
      var match = true
      for k in 0 ..< prefix.len:
        if text[i + k] != byte(prefix[k]):
          match = false; break
      if match:
        var idStart = i + prefix.len
        var idEnd = idStart
        while idEnd < j and text[idEnd] != byte(',') and text[idEnd] != byte('>'):
          idEnd += 1
        var name = newString(idEnd - idStart)
        for k in 0 ..< (idEnd - idStart): name[k] = char(text[idStart + k])
        result.add(name)
    i = j + 1

# ---------------------------------------------------------------------------
# M2 — Buffered fd reader and single-record readers
# ---------------------------------------------------------------------------

const FdBufSize = 65536

type BufferedFdReader* = object
  ## Buffered reader wrapping a POSIX file descriptor.  Reads FdBufSize bytes
  ## at a time to amortise syscall overhead.
  fd*:   cint
  buf:   array[FdBufSize, byte]
  pos:   int     ## next unread byte in buf
  len:   int     ## number of valid bytes in buf
  eof:   bool

proc initBufferedFdReader*(fd: cint): BufferedFdReader =
  BufferedFdReader(fd: fd, pos: 0, len: 0, eof: false)

proc fill(r: var BufferedFdReader) =
  ## Refill the internal buffer from the fd.
  if r.eof: return
  let n = posix.read(r.fd, addr r.buf[0], FdBufSize)
  if n <= 0:
    r.eof = true
    r.len = 0
  else:
    r.len = n
  r.pos = 0

proc readExact(r: var BufferedFdReader; dst: var openArray[byte]; count: int): bool =
  ## Read exactly `count` bytes into dst. Returns false on EOF before count.
  var written = 0
  while written < count:
    if r.pos >= r.len:
      r.fill()
      if r.eof: return false
    let avail = min(r.len - r.pos, count - written)
    copyMem(addr dst[written], addr r.buf[r.pos], avail)
    r.pos += avail
    written += avail
  return true

proc readNextVcfRecord*(r: var BufferedFdReader): seq[byte] =
  ## Read one VCF record (\n-terminated line) from the buffered reader.
  result = @[]
  while true:
    if r.pos >= r.len:
      r.fill()
      if r.eof: return
    # Scan for \n in the buffered data.
    let start = r.pos
    var i = start
    while i < r.len:
      if r.buf[i] == byte('\n'):
        # Found end of record. Copy start..i (inclusive) to result.
        let chunk = i - start + 1
        let oldLen = result.len
        result.setLen(oldLen + chunk)
        copyMem(addr result[oldLen], addr r.buf[start], chunk)
        r.pos = i + 1
        return
      inc i
    # No \n found — copy entire remaining buffer and refill.
    let chunk = r.len - start
    let oldLen = result.len
    result.setLen(oldLen + chunk)
    copyMem(addr result[oldLen], addr r.buf[start], chunk)
    r.pos = r.len

proc readNextBcfRecord*(r: var BufferedFdReader): seq[byte] =
  ## Read one BCF binary record from the buffered reader.
  var hdr: array[8, byte]
  if not r.readExact(hdr, 8): return @[]
  let lShared = (hdr[0].uint32 or (hdr[1].uint32 shl 8) or
                 (hdr[2].uint32 shl 16) or (hdr[3].uint32 shl 24)).int
  let lIndiv  = (hdr[4].uint32 or (hdr[5].uint32 shl 8) or
                 (hdr[6].uint32 shl 16) or (hdr[7].uint32 shl 24)).int
  let payloadLen = lShared + lIndiv
  result = newSeq[byte](8 + payloadLen)
  copyMem(addr result[0], addr hdr[0], 8)
  # Read payload directly into result[8..].
  var written = 0
  while written < payloadLen:
    if r.pos >= r.len:
      r.fill()
      if r.eof: return @[]
    let avail = min(r.len - r.pos, payloadLen - written)
    copyMem(addr result[8 + written], addr r.buf[r.pos], avail)
    r.pos += avail
    written += avail

# ---------------------------------------------------------------------------
# Buffered fd writer — batches small writes into 64KB syscalls
# ---------------------------------------------------------------------------

type BufferedFdWriter* = object
  fd:   cint
  buf:  array[FdBufSize, byte]
  pos:  int

proc initBufferedFdWriter*(fd: cint): BufferedFdWriter =
  BufferedFdWriter(fd: fd, pos: 0)

proc flush*(w: var BufferedFdWriter) =
  var written = 0
  while written < w.pos:
    let n = posix.write(w.fd, addr w.buf[written], w.pos - written)
    if n <= 0: break
    written += n
  w.pos = 0

proc write*(w: var BufferedFdWriter; data: openArray[byte]) =
  var srcPos = 0
  while srcPos < data.len:
    let space = FdBufSize - w.pos
    let chunk = min(space, data.len - srcPos)
    copyMem(addr w.buf[w.pos], unsafeAddr data[srcPos], chunk)
    w.pos += chunk
    srcPos += chunk
    if w.pos >= FdBufSize:
      w.flush()

# Unbuffered single-record readers for backward compatibility with tests.
# These read one record without buffering, so they work when called repeatedly
# on the same fd without losing data between calls.
proc readNextVcfRecord*(fd: cint): seq[byte] =
  result = @[]
  var b: byte
  while true:
    let n = posix.read(fd, addr b, 1)
    if n <= 0: return
    result.add(b)
    if b == byte('\n'): return

proc readNextBcfRecord*(fd: cint): seq[byte] =
  var hdr: array[8, byte]
  var got = 0
  while got < 8:
    let n = posix.read(fd, addr hdr[got], 8 - got)
    if n <= 0: return @[]
    got += n
  let lShared = (hdr[0].uint32 or (hdr[1].uint32 shl 8) or
                 (hdr[2].uint32 shl 16) or (hdr[3].uint32 shl 24)).int
  let lIndiv  = (hdr[4].uint32 or (hdr[5].uint32 shl 8) or
                 (hdr[6].uint32 shl 16) or (hdr[7].uint32 shl 24)).int
  let total = lShared + lIndiv
  result = newSeq[byte](8 + total)
  for i in 0 ..< 8: result[i] = hdr[i]
  var pos = 8
  while pos < result.len:
    let n = posix.read(fd, addr result[pos], result.len - pos)
    if n <= 0: return @[]
    pos += n

# ---------------------------------------------------------------------------
# M3 — Sort key extraction from a single record
# ---------------------------------------------------------------------------

proc buildContigMap*(contigTable: seq[string]): Table[string, int] =
  ## Build a hash map from contig name → rank for O(1) lookup.
  result = initTable[string, int]()
  for i, name in contigTable:
    result[name] = i

proc extractSortKey*(record: seq[byte]; fmt: FileFormat;
                     contigMap: Table[string, int];
                     chromBuf: var string): (int, int32) =
  ## Return (contig_rank, pos) for a single uncompressed record.
  ## chromBuf is a reusable scratch string to avoid per-record allocation.
  if record.len == 0: return (high(int), 0'i32)
  if fmt == ffBcf:
    if record.len < 16: return (high(int), 0'i32)
    let chromId = int32(record[8].uint32 or (record[9].uint32 shl 8) or
                        (record[10].uint32 shl 16) or (record[11].uint32 shl 24))
    let pos     = int32(record[12].uint32 or (record[13].uint32 shl 8) or
                        (record[14].uint32 shl 16) or (record[15].uint32 shl 24))
    let rank = if chromId >= 0 and chromId < contigMap.len: chromId.int else: high(int)
    result = (rank, pos)
  else:
    var tab0 = -1
    var tab1 = -1
    for i in 0 ..< record.len:
      if record[i] == byte('\t'):
        if tab0 < 0: tab0 = i
        elif tab1 < 0: tab1 = i; break
    if tab0 < 0 or tab1 < 0: return (high(int), 0'i32)
    # Reuse chromBuf to avoid per-record heap allocation.
    chromBuf.setLen(tab0)
    for i in 0 ..< tab0: chromBuf[i] = char(record[i])
    let rank = contigMap.getOrDefault(chromBuf, high(int))
    var pos: int32 = 0
    for i in (tab0 + 1) ..< tab1:
      let d = record[i].int - '0'.int
      if d < 0 or d > 9: return (high(int), 0'i32)
      pos = pos * 10 + d.int32
    pos -= 1
    result = (rank, pos)

proc extractSortKey*(record: seq[byte]; fmt: FileFormat;
                     contigMap: Table[string, int]): (int, int32) =
  ## Convenience overload without scratch buffer.
  var buf = ""
  extractSortKey(record, fmt, contigMap, buf)


# Backward-compatible overload for tests using seq[string] contigTable.
proc extractSortKey*(record: seq[byte]; fmt: FileFormat;
                     contigTable: seq[string]): (int, int32) =
  extractSortKey(record, fmt, buildContigMap(contigTable))

# ---------------------------------------------------------------------------
# M4 — k-way merge from sorted fd streams
# ---------------------------------------------------------------------------

type
  MergeEntry = object
    rank:  int
    pos:   int32
    fdIdx: int
    rec:   seq[byte]

proc `<`(a, b: MergeEntry): bool {.inline.} =
  if a.rank != b.rank: return a.rank < b.rank
  a.pos < b.pos

proc kWayMerge*(fds: seq[cint]; outFd: cint; fmt: FileFormat;
                contigTable: seq[string]) =
  ## k-way priority-queue merge of N sorted, uncompressed, header-stripped record
  ## streams. Each fd must produce VCF lines (\n-terminated) or raw BCF binary
  ## records in sorted (contig_rank, pos) order.
  ## Records are emitted to outFd in merged genomic order.
  ## fds may be any POSIX file descriptors (pipes, files, etc.).
  var heap = initHeapQueue[MergeEntry]()
  let contigMap = buildContigMap(contigTable)
  var writer = initBufferedFdWriter(outFd)
  var chromBuf = ""

  # Create one buffered reader per input fd.
  var readers = newSeq[BufferedFdReader](fds.len)
  for i in 0 ..< fds.len:
    readers[i] = initBufferedFdReader(fds[i])

  template nextRec(idx: int): seq[byte] =
    if fmt == ffBcf: readers[idx].readNextBcfRecord()
    else:            readers[idx].readNextVcfRecord()

  # Seed the heap with the first record from each stream.
  for i in 0 ..< fds.len:
    var rec = nextRec(i)
    if rec.len > 0:
      let (rank, pos) = extractSortKey(rec, fmt, contigMap, chromBuf)
      heap.push(MergeEntry(rank: rank, pos: pos, fdIdx: i, rec: move rec))

  # Merge loop: always emit the minimum-key record.
  while heap.len > 0:
    let entry = heap.pop()
    writer.write(entry.rec)
    # Refill from the same stream.
    var rec = nextRec(entry.fdIdx)
    if rec.len > 0:
      let (rank, pos) = extractSortKey(rec, fmt, contigMap, chromBuf)
      heap.push(MergeEntry(rank: rank, pos: pos, fdIdx: entry.fdIdx, rec: move rec))
  writer.flush()

# ---------------------------------------------------------------------------
# S2 — #CHROM line extraction helpers
# ---------------------------------------------------------------------------

proc extractChromLine*(data: seq[byte]): string =
  ## Return the first line starting with "#CHROM" from uncompressed bytes.
  ## Returns "" if not found.
  const chromPrefix = "#CHROM"
  var i = 0
  while i < data.len:
    var j = i
    while j < data.len and data[j] != byte('\n'): j += 1
    let lineLen = j - i
    if lineLen >= chromPrefix.len:
      var match = true
      for k in 0 ..< chromPrefix.len:
        if data[i + k] != byte(chromPrefix[k]):
          match = false; break
      if match:
        var s = newString(lineLen)
        for k in 0 ..< lineLen: s[k] = char(data[i + k])
        return s
    i = j + 1
  return ""

proc chromLineFromBytes*(rawBytes: seq[byte]; fmt: FileFormat; isBgzf: bool): string =
  ## Extract the #CHROM line from raw (possibly BGZF-compressed) shard bytes.
  ## Decompresses only enough BGZF blocks to find the header boundary.
  ## Returns "" for text format or if not found.
  if fmt == ffText: return ""
  if isBgzf:
    var decompBuf: seq[byte]
    var pos = 0
    while pos < rawBytes.len:
      let blkSize = bgzfBlockSize(rawBytes.toOpenArray(pos, rawBytes.high))
      if blkSize <= 0: break
      decompBuf.add(decompressBgzf(rawBytes.toOpenArray(pos, pos + blkSize - 1)))
      pos += blkSize
      let hEnd =
        if fmt == ffBcf: findBcfHeaderEnd(decompBuf)
        else:            findVcfHeaderEnd(decompBuf)
      if hEnd >= 0:
        return extractChromLine(decompBuf[0 ..< hEnd])
    return extractChromLine(decompBuf)
  else:
    let hEnd =
      if fmt == ffBcf: findBcfHeaderEnd(rawBytes)
      else:            findVcfHeaderEnd(rawBytes)
    let limit = if hEnd >= 0: hEnd else: rawBytes.len
    return extractChromLine(rawBytes[0 ..< limit])

proc chromLineFromFile*(path: string; fmt: FileFormat; isBgzf: bool): string =
  ## Read just enough bytes from path to extract the #CHROM line.
  ## Returns "" for text format or if not found within the header.
  if fmt == ffText: return ""
  let f = open(path, fmRead)
  defer: f.close()
  const BufSize = 65536
  var buf = newSeq[byte](BufSize)
  var rawBuf: seq[byte]
  var decompBuf: seq[byte]
  var blockPos = 0
  while decompBuf.len < 10 * 1024 * 1024:  # 10 MB safety limit
    let got = readBytes(f, buf, 0, BufSize)
    if got <= 0: break
    rawBuf.add(buf[0 ..< got])
    while blockPos + 18 <= rawBuf.len:
      let blkSize = bgzfBlockSize(rawBuf.toOpenArray(blockPos, rawBuf.high))
      if blkSize <= 0 or blockPos + blkSize > rawBuf.len: break
      decompBuf.add(decompressBgzf(rawBuf.toOpenArray(blockPos, blockPos + blkSize - 1)))
      blockPos += blkSize
    let hEnd =
      if fmt == ffBcf: findBcfHeaderEnd(decompBuf)
      else:            findVcfHeaderEnd(decompBuf)
    if hEnd >= 0:
      return extractChromLine(decompBuf[0 ..< hEnd])
  return extractChromLine(decompBuf)

proc extractInputHeaderBytes*(path: string): seq[byte] =
  ## Read and return the decompressed header bytes (up to first data record)
  ## from a VCF.gz or BCF input file.  BGZF blocks are decompressed in chunks.
  let fmt = if path.endsWith(".bcf"): ffBcf else: ffVcf
  let f = open(path, fmRead)
  defer: f.close()
  const BufSize = 65536
  var buf      = newSeq[byte](BufSize)
  var rawBuf:    seq[byte]
  var decompBuf: seq[byte]
  var blockPos = 0
  while decompBuf.len < 10 * 1024 * 1024:
    let got = readBytes(f, buf, 0, BufSize)
    if got <= 0: break
    rawBuf.add(buf[0 ..< got])
    while blockPos + 18 <= rawBuf.len:
      let blkSize = bgzfBlockSize(rawBuf.toOpenArray(blockPos, rawBuf.high))
      if blkSize <= 0 or blockPos + blkSize > rawBuf.len: break
      decompBuf.add(decompressBgzf(rawBuf.toOpenArray(blockPos, blockPos + blkSize - 1)))
      blockPos += blkSize
    let hEnd =
      if fmt == ffBcf: findBcfHeaderEnd(decompBuf)
      else:            findVcfHeaderEnd(decompBuf)
    if hEnd >= 0: return decompBuf[0 ..< hEnd]
  result = decompBuf

# ---------------------------------------------------------------------------
# Shared shard-writing helpers (used by both runInterceptor and gatherFiles)
# ---------------------------------------------------------------------------

proc stripTrailingEof*(bytes: seq[byte]): seq[byte] =
  ## Return bytes with the 28-byte BGZF EOF block stripped from the end, if present.
  if bytes.len >= BGZF_EOF.len and
     bytes[bytes.len - BGZF_EOF.len ..< bytes.len] == @BGZF_EOF:
    bytes[0 ..< bytes.len - BGZF_EOF.len]
  else:
    bytes

proc writeShardZero*(outFile: File; bytes: seq[byte]; isBgzf: bool;
                     compression: Compression) =
  ## Write shard 0 to outFile: no header stripping, recompress as needed.
  ## bytes must already have the trailing BGZF EOF block removed.
  let toWrite: seq[byte] =
    if isBgzf and compression == compNone:
      decompressAllBgzfBlocks(bytes)
    elif not isBgzf and compression == compBgzf:
      compressToBgzfMulti(bytes)
    else:
      bytes
  discard outFile.writeBytes(toWrite, 0, toWrite.len)

proc writeShardData*(outFile: File; bytes: seq[byte]; fmt: FileFormat;
                     isBgzf: bool; cfg: GatherConfig) =
  ## Write one shard 1..N to outFile: strip headers and recompress as needed.
  ## bytes must already have the trailing BGZF EOF block removed.
  if isBgzf and fmt in {ffVcf, ffBcf}:
    # Optimised path: decompress only the header-containing blocks, then
    # raw-copy (or decompress-and-write) the remaining data blocks.
    var blockPos = 0
    # Pre-allocate for header decompression. Large-sample VCF headers (e.g.
    # 2504 samples) decompress to several MB; 2 MB covers most cases and
    # avoids repeated doubling reallocs while scanning for the header end.
    var decompAccum = newSeqOfCap[byte](2 * 1024 * 1024)
    var headerEnd = -1
    while blockPos < bytes.len and headerEnd < 0:
      let blkSize = bgzfBlockSize(bytes.toOpenArray(blockPos, bytes.high))
      if blkSize <= 0: break
      decompAccum.add(
        decompressBgzf(bytes.toOpenArray(blockPos, blockPos + blkSize - 1)))
      blockPos += blkSize
      headerEnd =
        if fmt == ffBcf: findBcfHeaderEnd(decompAccum)
        else:             findVcfHeaderEnd(decompAccum)
    if headerEnd < 0: headerEnd = decompAccum.len  # edge: all-header shard
    let tail = decompAccum[headerEnd ..< decompAccum.len]
    if tail.len > 0:
      let chunk =
        if cfg.compression == compBgzf: compressToBgzfMulti(tail)
        else: tail
      discard outFile.writeBytes(chunk, 0, chunk.len)
    while blockPos < bytes.len:
      let blkSize = bgzfBlockSize(bytes.toOpenArray(blockPos, bytes.high))
      if blkSize <= 0: break
      if cfg.compression == compBgzf:
        discard outFile.writeBytes(bytes, blockPos, blkSize)
      else:
        let d = decompressBgzf(bytes.toOpenArray(blockPos, blockPos + blkSize - 1))
        discard outFile.writeBytes(d, 0, d.len)
      blockPos += blkSize
  else:
    # General path: decompress all, strip headers, recompress.
    let data: seq[byte] =
      if isBgzf: decompressAllBgzfBlocks(bytes)
      else: bytes
    let stripped: seq[byte] =
      case fmt
      of ffBcf: stripBcfHeader(data)
      of ffVcf: stripLinesByPattern(data, "#")
      of ffText:
        if cfg.headerPattern.isSome:
          stripLinesByPattern(data, cfg.headerPattern.get)
        elif cfg.headerN.isSome:
          stripFirstNLines(data, cfg.headerN.get)
        else:
          data
    let toWrite: seq[byte] =
      if cfg.compression == compBgzf: compressToBgzfMulti(stripped)
      else: stripped
    discard outFile.writeBytes(toWrite, 0, toWrite.len)

# ---------------------------------------------------------------------------
# Interceptor thread proc
# ---------------------------------------------------------------------------

## Maximum size `pending` reaches inside `runInterceptor`, recorded per-thread
## for the streaming-property regression test (G3.6).  Production overhead is
## a single int compare per flush; the value is read after the interceptor
## returns by tests that drive `runInterceptor` directly.
var gMaxPendingBytes* {.threadvar.}: int

## Total bytes re-encoded by `runInterceptor` via `compressToBgzfMulti` on the
## BGZF→BGZF raw-forwarding fast path.  Tests assert this is 0 for shard 0 and
## ≤ one BGZF block (65536 bytes) for shards 1..N — proof that the fast path
## is forwarding raw blocks rather than re-encoding everything.
var gReencodedBytes* {.threadvar.}: int

proc isBgzfEofBlock(buf: openArray[byte]; pos, blkSize: int): bool {.inline.} =
  ## Return true if the BGZF block at buf[pos ..< pos+blkSize] is the
  ## canonical 28-byte BGZF EOF marker.
  if blkSize != BGZF_EOF.len: return false
  for i in 0 ..< BGZF_EOF.len:
    if buf[pos + i] != BGZF_EOF[i]: return false
  return true

proc runInterceptor*(cfg: GatherConfig; shardIdx: int; inputFd: cint; tmpPath: string): int =
  ## Per-shard interceptor thread proc.  Streams subprocess stdout to a temp
  ## file (or stdout for shard 0 when `cfg.toStdout`) without ever holding the
  ## entire shard in memory.
  ##
  ## Phase A — read first chunk; sniff format on shard 0; shards 1..N spinwait
  ##           on `gChromLine.ready` then read the format from globals.
  ## Phase B — accumulate uncompressed `pending` (via `appendReadToAccum`) until
  ##           `findVcfHeaderEnd` / `findBcfHeaderEnd` succeeds.  Shard 0 sets
  ##           `gChromLine`/`gChromLine.ready`; shards 1..N validate `#CHROM`
  ##           and bail out cleanly on mismatch (no record bytes written).
  ## Phase C — flush `pending` (recompressed if `cfg.compression == compBgzf`,
  ##           otherwise raw).  Continue reading: each iteration accumulates
  ##           into `pending`, flushes when it crosses `FlushThresh`, and trims
  ##           the consumed prefix of `rawAccum` so memory stays bounded.
  ##
  ## The trailing BGZF EOF block from the input naturally vanishes because
  ## `decompressBgzf(EOF_block)` returns 0 bytes.  `concatenateShards` writes
  ## exactly one EOF at the very end of the merged output (see G5 contract).
  ##
  ## Text format takes a buffered fallback path: text inputs in practice are
  ## tiny, the streaming win is on BGZF VCF/BCF, and reusing
  ## `writeShardZero`/`writeShardData` keeps `--header-n` / `--header-pattern`
  ## semantics byte-identical to `gatherFiles`.
  let isStdout = (shardIdx == 0 and cfg.toStdout)
  let outFile: File = if isStdout: stdout else: open(tmpPath, fmWrite)
  gMaxPendingBytes = 0
  gReencodedBytes  = 0
  try:
    const ChunkSize = 65536
    const FlushThresh = 1 * 1024 * 1024
    var buf = newSeq[byte](ChunkSize)
    var fmt: FileFormat
    var isBgzf: bool
    var rawAccum: seq[byte]
    var bgzfPos  = 0
    var pending:  seq[byte]

    # ── Phase A: first read + format detection ───────────────────────
    let initRead = posix.read(inputFd, cast[pointer](addr buf[0]), ChunkSize)
    if initRead <= 0:
      # Empty shard.  Shard 0 must still release shards 1..N.
      if shardIdx == 0:
        gDetectedFormat = ffText
        gStreamIsBgzf   = false
        gChromLine.len   = 0
        gChromLine.ready = true
      return 0
    if shardIdx == 0:
      let head = buf[0 ..< initRead]
      let (detFmt, detBgzf) = sniffStreamFormat(head)
      fmt = detFmt
      isBgzf = detBgzf
      if fmt != cfg.format and not cfg.toStdout:
        stderr.writeLine &"warning: stream format detected as {fmt} " &
          &"but --gather expects {cfg.format}; proceeding"
    else:
      # Wait until shard 0 has detected the format.
      while not gChromLine.ready:
        sleep(1)
      fmt = gDetectedFormat
      isBgzf = gStreamIsBgzf

    # ── Text fallback: buffer the whole shard, reuse the existing helpers.
    # Text inputs are tiny in practice; --header-n / --header-pattern semantics
    # match `gatherFiles` exactly when we delegate to writeShardZero/Data.
    if fmt == ffText:
      var allBytes = newSeqOfCap[byte](4 * 1024 * 1024)
      allBytes.add(buf[0 ..< initRead])
      while true:
        let got = posix.read(inputFd, cast[pointer](addr buf[0]), ChunkSize)
        if got <= 0: break
        let base = allBytes.len
        allBytes.setLen(base + got)
        copyMem(addr allBytes[base], addr buf[0], got)
      if shardIdx == 0:
        gDetectedFormat = fmt
        gStreamIsBgzf   = isBgzf
        gChromLine.len   = 0
        gChromLine.ready = true
        let cleaned = if isBgzf: stripTrailingEof(allBytes) else: allBytes
        writeShardZero(outFile, cleaned, isBgzf, cfg.compression)
      else:
        let cleaned = if isBgzf: stripTrailingEof(allBytes) else: allBytes
        writeShardData(outFile, cleaned, fmt, isBgzf, cfg)
      return 0

    # ── Phase B: streaming VCF/BCF — accumulate header ──────────────
    appendReadToAccum(buf, initRead.int, isBgzf, rawAccum, bgzfPos, pending)
    if pending.len > gMaxPendingBytes: gMaxPendingBytes = pending.len

    var hEnd = -1
    while true:
      hEnd =
        if fmt == ffBcf: findBcfHeaderEnd(pending)
        else:            findVcfHeaderEnd(pending)
      if hEnd >= 0: break
      let n = posix.read(inputFd, cast[pointer](addr buf[0]), ChunkSize)
      if n <= 0: break
      appendReadToAccum(buf, n.int, isBgzf, rawAccum, bgzfPos, pending)
      if pending.len > gMaxPendingBytes: gMaxPendingBytes = pending.len
    if hEnd < 0: hEnd = pending.len  # short stream — header runs to EOF

    # Set or validate #CHROM before any record byte is written.
    if shardIdx == 0:
      let chromStr = extractChromLine(pending[0 ..< hEnd])
      gDetectedFormat = fmt
      gStreamIsBgzf   = isBgzf
      gChromLine.len = min(chromStr.len, gChromLine.buf.len).int32
      for k in 0 ..< gChromLine.len.int:
        gChromLine.buf[k] = byte(chromStr[k])
      gChromLine.ready = true
    else:
      let myChromLine = extractChromLine(pending[0 ..< hEnd])
      let glen = gChromLine.len.int
      var match = (myChromLine.len == glen)
      if match:
        for k in 0 ..< glen:
          if byte(myChromLine[k]) != gChromLine.buf[k]:
            match = false; break
      if not match:
        var shard0Line = newString(glen)
        for k in 0 ..< glen: shard0Line[k] = char(gChromLine.buf[k])
        stderr.writeLine &"error: gather: #CHROM line mismatch at shard {shardIdx + 1}:"
        stderr.writeLine &"  shard 1: {shard0Line}"
        stderr.writeLine &"  shard {shardIdx + 1}: {myChromLine}"
        return 1

    # ── Phase C: stream the rest of the shard to outFile ────────────
    if isBgzf and cfg.compression == compBgzf:
      # ── BGZF → BGZF fast path: forward raw BGZF blocks ─────────────
      # Identify the split block (the BGZF block whose decompressed contents
      # straddle the header boundary) by walking rawAccum and summing each
      # block's ISIZE field (last 4 bytes).
      var splitBlockRawStart    = -1
      var splitBlockRawEnd      = -1
      var splitBlockUncompStart = 0
      block findSplit:
        var p = 0
        while p < bgzfPos:
          let blkSize  = bgzfBlockSize(rawAccum.toOpenArray(p, bgzfPos - 1))
          if blkSize <= 0 or p + blkSize > bgzfPos: break
          let blkIsize = leU32(rawAccum.toOpenArray(p, p + blkSize - 1),
                               blkSize - 4).int
          if splitBlockUncompStart + blkIsize > hEnd:
            splitBlockRawStart = p
            splitBlockRawEnd   = p + blkSize
            break findSplit
          splitBlockUncompStart += blkIsize
          p += blkSize

      # Compute the initial bytes to forward.
      var rawWriteStart: int
      var splitTail: seq[byte]
      if shardIdx == 0:
        # Shard 0 keeps the header — forward every raw block from the start.
        rawWriteStart = 0
      elif splitBlockRawStart < 0:
        # Header consumed every block phase B saw — nothing to write yet.
        rawWriteStart = bgzfPos
      elif hEnd == splitBlockUncompStart:
        # Header ends exactly at a BGZF block boundary — no re-encode needed.
        rawWriteStart = splitBlockRawStart
      else:
        # Header ends mid-block: re-encode the post-header tail of the split
        # block as one fresh BGZF block, then forward subsequent blocks raw.
        let splitBlkSize  = splitBlockRawEnd - splitBlockRawStart
        let splitBlkIsize = leU32(
          rawAccum.toOpenArray(splitBlockRawStart, splitBlockRawEnd - 1),
          splitBlkSize - 4).int
        let splitBlockUncompEnd = splitBlockUncompStart + splitBlkIsize
        splitTail = pending[hEnd ..< splitBlockUncompEnd]
        rawWriteStart = splitBlockRawEnd

      # Write re-encoded split tail (if any) + raw-forward remaining phase-B
      # blocks, walking block-by-block to skip any embedded BGZF EOF block.
      if splitTail.len > 0:
        let z = compressToBgzfMulti(splitTail)
        discard outFile.writeBytes(z, 0, z.len)
        gReencodedBytes += splitTail.len
      var fp = rawWriteStart
      while fp + 18 <= bgzfPos:
        let blkSize = bgzfBlockSize(rawAccum.toOpenArray(fp, bgzfPos - 1))
        if blkSize <= 0 or fp + blkSize > bgzfPos: break
        if not isBgzfEofBlock(rawAccum, fp, blkSize):
          discard outFile.writeBytes(rawAccum, fp, blkSize)
        fp += blkSize

      # Free phase-B buffers; switch to a small carry buffer for streaming.
      # Phase B's last appendReadToAccum may have left a partial BGZF block
      # in rawAccum[bgzfPos..^1] (bytes that haven't yet formed a complete
      # block); preserve them as the initial carry — dropping them would
      # lose records that fall just past the header boundary.
      var carry: seq[byte] =
        if bgzfPos < rawAccum.len: rawAccum[bgzfPos ..< rawAccum.len]
        else: @[]
      rawAccum.setLen(0); pending.setLen(0); bgzfPos = 0

      while true:
        let n = posix.read(inputFd, cast[pointer](addr buf[0]), ChunkSize)
        if n <= 0: break
        let base = carry.len
        carry.setLen(base + n)
        copyMem(addr carry[base], addr buf[0], n)
        var p = 0
        while p + 18 <= carry.len:
          let blkSize = bgzfBlockSize(carry.toOpenArray(p, carry.high))
          if blkSize <= 0: break               # corrupt frame — bail
          if p + blkSize > carry.len: break    # incomplete — wait for next read
          if not isBgzfEofBlock(carry, p, blkSize):
            discard outFile.writeBytes(carry, p, blkSize)
          p += blkSize
        if p > 0:
          carry = if p < carry.len: carry[p ..< carry.len] else: @[]
      # At EOF a non-empty carry indicates a truncated subprocess output —
      # silently drop it (the previous decompress path would also have lost
      # the trailing partial block).

      result = 0
      return

    # ── Phase C (other modes): decompress / recompress flush loop ───
    # Shards 1..N drop the header bytes; shard 0 keeps them (header + records).
    if shardIdx > 0:
      pending = if hEnd < pending.len: pending[hEnd ..< pending.len] else: @[]

    template flushPending() =
      if pending.len > 0:
        if cfg.compression == compBgzf:
          let z = compressToBgzfMulti(pending)
          discard outFile.writeBytes(z, 0, z.len)
        else:
          discard outFile.writeBytes(pending, 0, pending.len)
        pending.setLen(0)

    template trimRawAccum() =
      if isBgzf and bgzfPos > 0:
        # Drop the already-decompressed prefix of rawAccum so it stays bounded.
        rawAccum = if bgzfPos < rawAccum.len: rawAccum[bgzfPos ..< rawAccum.len]
                   else: @[]
        bgzfPos = 0

    flushPending()
    trimRawAccum()

    while true:
      let n = posix.read(inputFd, cast[pointer](addr buf[0]), ChunkSize)
      if n <= 0: break
      appendReadToAccum(buf, n.int, isBgzf, rawAccum, bgzfPos, pending)
      if pending.len > gMaxPendingBytes: gMaxPendingBytes = pending.len
      if pending.len >= FlushThresh:
        flushPending()
        trimRawAccum()

    flushPending()

    result = 0
  finally:
    if not isStdout: outFile.close()
    discard posix.close(inputFd)

# ---------------------------------------------------------------------------
# G5 — cleanup and concatenation
# ---------------------------------------------------------------------------

proc cleanupTempDir*(tmpDir: string; tmpPaths: seq[string]; success: bool) =
  ## On success: delete every temp shard file then remove the temp dir.
  ## On failure: print the path of each temp file to stderr and leave them on disk.
  if success:
    for p in tmpPaths:
      try: removeFile(p) except OSError: discard
    try: removeDir(tmpDir) except OSError: discard
  else:
    stderr.writeLine "gather: temp files left on disk for debugging:"
    for p in tmpPaths:
      stderr.writeLine "  " & p

proc concatenateShards*(cfg: GatherConfig; tmpPaths: seq[string]) =
  ## Raw-copy each temp shard file (in order) into cfg.outputPath (or stdout).
  ## Appends a single BGZF EOF block at the end when cfg.compression == compBgzf.
  ## Calls cleanupTempDir on success.
  let outFile: File = if cfg.toStdout: stdout else: open(cfg.outputPath, fmAppend)
  for p in tmpPaths:
    rawCopyBytes(p, outFile, 0, getFileSize(p))
  if cfg.compression == compBgzf:
    discard outFile.writeBytes(BGZF_EOF, 0, BGZF_EOF.len)
  if not cfg.toStdout: outFile.close()
  cleanupTempDir(cfg.tmpDir, tmpPaths, true)

# ---------------------------------------------------------------------------
# C3 — Direct-file gather (no temp dir)
# ---------------------------------------------------------------------------

proc gatherFiles*(cfg: GatherConfig; inputPaths: seq[string]) =
  ## Concatenate pre-existing shard files into cfg.outputPath (or stdout).
  ## Shard 0 is written with its header intact; shards 1..N have headers stripped.
  ## For VCF/BCF: validates that #CHROM lines match before writing anything.
  ## No temp files are created.  Resets the global format-detection state.
  if inputPaths.len == 0:
    stderr.writeLine "error: gather: no input files provided"
    quit(1)
  gChromLine.ready = false
  gDetectedFormat = ffText
  gStreamIsBgzf   = false
  gChromLine.len   = 0

  # ── Phase 1: read shard 0, detect format, validate #CHROM ──────────────────
  let s0Size = getFileSize(inputPaths[0]).int
  var s0Bytes = newSeq[byte](s0Size)
  block:
    let fs0 = open(inputPaths[0], fmRead)
    discard readBytes(fs0, s0Bytes, 0, s0Size)
    fs0.close()
  let (fmt, isBgzf) = sniffStreamFormat(s0Bytes)
  gDetectedFormat = fmt
  gStreamIsBgzf   = isBgzf
  gChromLine.ready = true
  if fmt != cfg.format and not cfg.toStdout:
    stderr.writeLine &"warning: shard 0 format detected as {fmt} " &
      &"but output expects {cfg.format}; proceeding"

  if fmt in {ffVcf, ffBcf}:
    let chrom0 = chromLineFromBytes(s0Bytes, fmt, isBgzf)
    for j in 1 ..< inputPaths.len:
      let chromJ = chromLineFromFile(inputPaths[j], fmt, isBgzf)
      if chromJ != chrom0:
        stderr.writeLine &"error: gather: #CHROM line mismatch between shard 1 and shard {j+1} ({inputPaths[j]}):"
        stderr.writeLine &"  shard 1: {chrom0}"
        stderr.writeLine &"  shard {j+1}: {chromJ}"
        quit(1)

  # ── Phase 2: open output, write shard 0 then shards 1..N ───────────────────
  let outFile: File = if cfg.toStdout: stdout else: open(cfg.outputPath, fmWrite)

  # Write shard 0 (bytes already buffered in s0Bytes).
  let bytes0 = if isBgzf: stripTrailingEof(s0Bytes) else: s0Bytes
  writeShardZero(outFile, bytes0, isBgzf, cfg.compression)

  # Write shards 1..N: read fresh from disk, strip headers.
  for j in 1 ..< inputPaths.len:
    let jSize = getFileSize(inputPaths[j]).int
    var allBytes = newSeq[byte](jSize)
    block:
      let fj = open(inputPaths[j], fmRead)
      discard readBytes(fj, allBytes, 0, jSize)
      fj.close()
    let bytes = if isBgzf: stripTrailingEof(allBytes) else: allBytes
    writeShardData(outFile, bytes, fmt, isBgzf, cfg)

  if cfg.compression == compBgzf:
    discard outFile.writeBytes(BGZF_EOF, 0, BGZF_EOF.len)
  if not cfg.toStdout: outFile.close()

# ---------------------------------------------------------------------------
# M6 — gather --merge: k-way merge of existing shard files
# ---------------------------------------------------------------------------

proc doFileFeeder(shardIdx: int; path: string; relayWriteFd: cint): int {.gcsafe.} =
  ## Open path, strip VCF/BCF header, decompress if BGZF, relay raw record
  ## bytes to relayWriteFd. Shard 0 also captures the header into
  ## gMergeHeader.buf and signals gMergeHeader.ready. Closes relayWriteFd.
  const ReadSize = 65536
  var raw     = newSeq[byte](ReadSize)
  var pending: seq[byte]
  var isBgzf  = false
  var fmt     = ffVcf
  var rawAccum: seq[byte]
  var bgzfPos = 0

  let fileFd = posix.open(path.cstring, O_RDONLY)
  if fileFd < 0:
    discard posix.close(relayWriteFd)
    if shardIdx == 0:
      gMergeFormat      = ffVcf
      gMergeHeader.ready = true
    return 1

  let n0 = posix.read(fileFd, cast[pointer](addr raw[0]), ReadSize)
  if n0 <= 0:
    discard posix.close(fileFd)
    discard posix.close(relayWriteFd)
    if shardIdx == 0:
      gMergeFormat      = ffVcf
      gMergeHeader.ready = true
    return 0

  let (detFmt, detBgzf) = sniffStreamFormat(raw[0 ..< n0])
  fmt    = detFmt
  isBgzf = detBgzf
  appendReadToAccum(raw, n0.int, isBgzf, rawAccum, bgzfPos, pending)

  var hEnd = -1
  while hEnd < 0:
    hEnd =
      case fmt
      of ffBcf:  findBcfHeaderEnd(pending)
      of ffVcf:  findVcfHeaderEnd(pending)
      of ffText: 0
    if hEnd >= 0: break
    let n = posix.read(fileFd, cast[pointer](addr raw[0]), ReadSize)
    if n <= 0: break
    appendReadToAccum(raw, n.int, isBgzf, rawAccum, bgzfPos, pending)
  if hEnd < 0: hEnd = pending.len

  if shardIdx == 0:
    let sz = min(hEnd, gMergeHeader.buf.len)
    if sz > 0:
      copyMem(addr gMergeHeader.buf[0], unsafeAddr pending[0], sz)
    gMergeHeader.len   = sz.int32
    gMergeFormat      = fmt
    gMergeHeader.ready = true

  template relayBytes(data: openArray[byte]) =
    var w = 0
    while w < data.len:
      let nw = posix.write(relayWriteFd, cast[pointer](unsafeAddr data[w]),
                           data.len - w)
      if nw <= 0:
        discard posix.close(relayWriteFd)
        discard posix.close(fileFd)
        return 0
      w += nw

  if hEnd < pending.len:
    relayBytes(pending.toOpenArray(hEnd, pending.high))
  pending = @[]

  while true:
    let n = posix.read(fileFd, cast[pointer](addr raw[0]), ReadSize)
    if n <= 0: break
    if isBgzf:
      rawAccum.add(raw[0 ..< n])
      flushBgzfAccum(rawAccum, bgzfPos, pending)
      if pending.len > 0:
        relayBytes(pending.toOpenArray(0, pending.high))
        pending = @[]
    else:
      relayBytes(raw.toOpenArray(0, n - 1))

  discard posix.close(relayWriteFd)
  discard posix.close(fileFd)
  result = 0

proc gatherFilesMerge*(cfg: GatherConfig; inputPaths: seq[string]) =
  ## k-way merge of sorted shard files into cfg.outputPath (or stdout).
  ## Each shard file is opened, decompressed if BGZF, header stripped,
  ## and records merged in genomic order via kWayMerge. No temp files.
  if inputPaths.len == 0:
    stderr.writeLine "error: gather: no input files provided"
    quit(1)

  let nShards = inputPaths.len
  setMaxPoolSize(max(4, nShards))

  gMergeHeader.ready = false
  gMergeHeader.len   = 0
  gMergeFormat      = ffVcf

  var relayReadFds: seq[cint]
  var feederFvs:   seq[FlowVar[int]]

  for i in 0 ..< nShards:
    var relayPipe: array[2, cint]
    if posix.pipe(relayPipe) != 0:
      stderr.writeLine &"error: pipe() failed for shard {i + 1}"
      quit(1)
    relayReadFds.add(relayPipe[0])
    feederFvs.add(spawn doFileFeeder(i, inputPaths[i], relayPipe[1]))

  while not gMergeHeader.ready: sleep(1)

  let hdrSlice    = @(gMergeHeader.buf[0 ..< gMergeHeader.len])
  let contigTable = extractContigTable(hdrSlice)

  let outFd: cint =
    if cfg.toStdout: STDOUT_FILENO
    else:
      let fd = posix.open(cfg.outputPath.cstring,
                          O_WRONLY or O_CREAT or O_TRUNC, 0o666.Mode)
      if fd < 0:
        stderr.writeLine "error: could not create output file: " & cfg.outputPath
        quit(1)
      fd

  var hw = 0
  while hw < gMergeHeader.len.int:
    let n = posix.write(outFd, cast[pointer](addr gMergeHeader.buf[hw]),
                        gMergeHeader.len.int - hw)
    if n <= 0: break
    hw += n

  kWayMerge(relayReadFds, outFd, gMergeFormat, contigTable)

  for fd in relayReadFds: discard posix.close(fd)
  if not cfg.toStdout: discard posix.close(outFd)

  var anyFailed = false
  for fv in feederFvs:
    if (^fv) != 0: anyFailed = true
  if anyFailed: quit(1)
