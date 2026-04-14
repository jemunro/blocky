## gather — format inference and concatenation for gather subcommand.

import std/[os, strformat, strutils]
import std/posix
import bgzf

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

type
  GatherConfig* = object
    format*:        FileFormat
    compression*:   Compression
    outputPath*:    string
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
# Header stripping helpers
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

proc stripBcfHeader*(data: seq[byte]): seq[byte] =
  ## Strip BCF header (magic + l_text + header text) from uncompressed BCF bytes.
  ## Returns only the binary record bytes that follow the header.
  ## Returns @[] if data is too short to contain the full header or has no records.
  if data.len < 9: return @[]
  let lText = leU32(data, 5).int
  let headerSize = 5 + 4 + lText
  if headerSize >= data.len: return @[]
  result = data[headerSize ..< data.len]

# ---------------------------------------------------------------------------
# Header-end finders
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
# #CHROM line extraction helpers
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
  var buf = newSeqUninit[byte](BufSize)
  var rawBuf: seq[byte]
  var decompBuf: seq[byte]
  var blockPos = 0
  while decompBuf.len < 10 * 1024 * 1024:  # 10 MB safety limit
    let got = readBytes(f, buf, 0, BufSize)
    if got <= 0: break
    rawBuf.add(buf.toOpenArray(0, got.int - 1))
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
  var buf      = newSeqUninit[byte](BufSize)
  var rawBuf:    seq[byte]
  var decompBuf: seq[byte]
  var blockPos = 0
  while decompBuf.len < 10 * 1024 * 1024:
    let got = readBytes(f, buf, 0, BufSize)
    if got <= 0: break
    rawBuf.add(buf.toOpenArray(0, got.int - 1))
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
# Shared shard-writing helpers
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
                     isBgzf: bool; compression: Compression) =
  ## Write one shard 1..N to outFile: strip headers and recompress as needed.
  ## bytes must already have the trailing BGZF EOF block removed.
  ## Headers: BCF binary header stripped; VCF/text #-prefixed lines stripped.
  if isBgzf:
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
        else:            findVcfHeaderEnd(decompAccum)
    if headerEnd < 0: headerEnd = decompAccum.len  # edge: all-header shard
    let tail = decompAccum[headerEnd ..< decompAccum.len]
    if tail.len > 0:
      if compression == compBgzf:
        compressToBgzfMulti(outFile, tail)
      else:
        discard outFile.writeBytes(tail, 0, tail.len)
    var decompBuf: seq[byte]
    while blockPos < bytes.len:
      let blkSize = bgzfBlockSize(bytes.toOpenArray(blockPos, bytes.high))
      if blkSize <= 0: break
      if compression == compBgzf:
        discard outFile.writeBytes(bytes, blockPos, blkSize)
      else:
        decompressBgzfInto(bytes.toOpenArray(blockPos, blockPos + blkSize - 1), decompBuf)
        discard outFile.writeBytes(decompBuf, 0, decompBuf.len)
      blockPos += blkSize
  else:
    # Uncompressed path: strip headers, recompress if needed.
    let data = bytes
    let headerEnd =
      case fmt
      of ffBcf:         findBcfHeaderEnd(data)
      of ffVcf, ffText: findVcfHeaderEnd(data)
    let stripped =
      if headerEnd > 0 and headerEnd < data.len: data[headerEnd ..< data.len]
      elif headerEnd == 0: data
      else: @[]
    if compression == compBgzf:
      compressToBgzfMulti(outFile, stripped)
    else:
      discard outFile.writeBytes(stripped, 0, stripped.len)

# ---------------------------------------------------------------------------
# SharedBuf — GC-safe cross-thread buffer for #CHROM validation
# ---------------------------------------------------------------------------

type SharedBuf* = object
  ## Fixed-size byte buffer for cross-thread handoff. Writer sets buf+len
  ## then sets ready=true. Reader spin-waits on ready before reading.
  buf*:   array[4 * 1024 * 1024, byte]
  len*:   int32
  ready*: bool

# ---------------------------------------------------------------------------
# Interceptor — streaming header strip + BGZF recompression for tmp files
# ---------------------------------------------------------------------------

proc interceptShard*(shardIdx: int; inputFd: cint; tmpPath: string;
                     chromBufPtr: ptr SharedBuf): int {.gcsafe.} =
  ## Read subprocess stdout from inputFd (pipe), strip headers for shards 1..N,
  ## BGZF-compress, and write to tmpPath. Shard 0 writes everything and sets
  ## chromBufPtr with the #CHROM line. Shards 1..N validate #CHROM against it.
  ## Returns 0 on success, 1 on #CHROM mismatch.
  const ChunkSize = 65536
  const FlushThresh = 1 * 1024 * 1024
  var readBuf {.threadvar.}: seq[byte]
  if readBuf.len < ChunkSize: readBuf = newSeqUninit[byte](ChunkSize)

  # Phase A: first read + format detection.
  let initRead = posix.read(inputFd, cast[pointer](addr readBuf[0]), ChunkSize)
  if initRead <= 0:
    # Empty shard. Shard 0 must still release shards 1..N.
    if shardIdx == 0:
      chromBufPtr[].len = 0
      chromBufPtr[].ready = true
    discard posix.close(inputFd)
    # Write empty BGZF file (just EOF block).
    let f = open(tmpPath, fmWrite)
    discard f.writeBytes(BGZF_EOF, 0, BGZF_EOF.len)
    f.close()
    return 0

  let (fmt, isBgzf) = sniffStreamFormat(readBuf.toOpenArray(0, initRead.int - 1))
  info(&"intercept shard {shardIdx}: {fmt}, bgzf={isBgzf}")

  # Phase B: accumulate header.
  var rawAccum: seq[byte]
  var bgzfPos = 0
  var pending: seq[byte]  # decompressed bytes accumulated so far

  # Append initial read.
  if isBgzf:
    rawAccum.add(readBuf.toOpenArray(0, initRead.int - 1))
    while bgzfPos + 18 <= rawAccum.len:
      let blkSize = bgzfBlockSize(rawAccum.toOpenArray(bgzfPos, rawAccum.high))
      if blkSize <= 0 or bgzfPos + blkSize > rawAccum.len: break
      pending.add(decompressBgzf(rawAccum.toOpenArray(bgzfPos, bgzfPos + blkSize - 1)))
      bgzfPos += blkSize
  else:
    pending.add(readBuf.toOpenArray(0, initRead.int - 1))

  # Read until header end found.
  var hEnd = -1
  while hEnd < 0:
    hEnd =
      case fmt
      of ffBcf:  findBcfHeaderEnd(pending)
      of ffVcf:  findVcfHeaderEnd(pending)
      of ffText: findVcfHeaderEnd(pending)  # # prefix for all non-BCF
    if hEnd >= 0: break
    let n = posix.read(inputFd, cast[pointer](addr readBuf[0]), ChunkSize)
    if n <= 0: break
    if isBgzf:
      rawAccum.add(readBuf.toOpenArray(0, n.int - 1))
      while bgzfPos + 18 <= rawAccum.len:
        let blkSize = bgzfBlockSize(rawAccum.toOpenArray(bgzfPos, rawAccum.high))
        if blkSize <= 0 or bgzfPos + blkSize > rawAccum.len: break
        pending.add(decompressBgzf(rawAccum.toOpenArray(bgzfPos, bgzfPos + blkSize - 1)))
        bgzfPos += blkSize
    else:
      pending.add(readBuf.toOpenArray(0, n.int - 1))
  if hEnd < 0: hEnd = pending.len  # all header, no data

  # Phase C: #CHROM validation.
  let chromLine = extractChromLine(pending[0 ..< hEnd])
  if shardIdx == 0:
    let sz = min(chromLine.len, chromBufPtr[].buf.len)
    if sz > 0:
      copyMem(addr chromBufPtr[].buf[0], unsafeAddr chromLine[0], sz)
    chromBufPtr[].len = sz.int32
    chromBufPtr[].ready = true
    info(&"shard 0: #CHROM stored ({sz} bytes)")
  else:
    while not chromBufPtr[].ready: sleep(1)
    let refLen = chromBufPtr[].len.int
    var mismatch = (chromLine.len != refLen)
    if not mismatch:
      for k in 0 ..< refLen:
        if byte(chromLine[k]) != chromBufPtr[].buf[k]:
          mismatch = true; break
    if mismatch:
      var refLine = newString(refLen)
      for k in 0 ..< refLen: refLine[k] = char(chromBufPtr[].buf[k])
      stderr.writeLine &"error: #CHROM line mismatch at shard {shardIdx + 1}:"
      stderr.writeLine &"  shard 1: {refLine}"
      stderr.writeLine &"  shard {shardIdx + 1}: {chromLine}"
      discard posix.close(inputFd)
      return 1

  # Phase D: write tmp file.
  let outFile = open(tmpPath, fmWrite)

  if isBgzf:
    # ── BGZF optimised path ──────────────────────────────────────────────
    if shardIdx == 0:
      # Shard 0: raw-forward all BGZF blocks accumulated so far, then stream rest.
      var p = 0
      while p + 18 <= bgzfPos:
        let blkSize = bgzfBlockSize(rawAccum.toOpenArray(p, bgzfPos - 1))
        if blkSize <= 0 or p + blkSize > bgzfPos: break
        discard outFile.writeBytes(rawAccum, p, blkSize)
        p += blkSize
    else:
      # Shards 1..N: find which raw BGZF block contains hEnd, skip header blocks.
      # Walk rawAccum's BGZF blocks, tracking cumulative decompressed size.
      var cumDecomp = 0
      var splitBlockRawStart = -1
      var splitBlockRawEnd = -1
      var splitBlockDecompStart = 0
      block findSplit:
        var p = 0
        while p + 18 <= bgzfPos:
          let blkSize = bgzfBlockSize(rawAccum.toOpenArray(p, bgzfPos - 1))
          if blkSize <= 0 or p + blkSize > bgzfPos: break
          let blkIsize = leU32(rawAccum.toOpenArray(p, p + blkSize - 1),
                               blkSize - 4).int
          if cumDecomp + blkIsize > hEnd:
            splitBlockRawStart = p
            splitBlockRawEnd = p + blkSize
            splitBlockDecompStart = cumDecomp
            break findSplit
          cumDecomp += blkIsize
          p += blkSize

      var rawWriteStart: int
      if splitBlockRawStart < 0:
        rawWriteStart = bgzfPos  # header consumed all phase-B blocks
      elif hEnd == splitBlockDecompStart:
        rawWriteStart = splitBlockRawStart  # header ends at block boundary
      else:
        # Header ends mid-block: re-encode the post-header tail.
        let splitBlkSize = splitBlockRawEnd - splitBlockRawStart
        let splitBlkIsize = leU32(
          rawAccum.toOpenArray(splitBlockRawStart, splitBlockRawEnd - 1),
          splitBlkSize - 4).int
        let splitBlockDecompEnd = splitBlockDecompStart + splitBlkIsize
        let tail = pending[hEnd ..< splitBlockDecompEnd]
        if tail.len > 0:
          compressToBgzfMulti(outFile, tail)
        rawWriteStart = splitBlockRawEnd

      # Raw-forward remaining phase-B blocks (skip BGZF EOF blocks).
      var fp = rawWriteStart
      while fp + 18 <= bgzfPos:
        let blkSize = bgzfBlockSize(rawAccum.toOpenArray(fp, bgzfPos - 1))
        if blkSize <= 0 or fp + blkSize > bgzfPos: break
        if blkSize != BGZF_EOF.len or
           rawAccum[fp ..< fp + blkSize] != @BGZF_EOF:
          discard outFile.writeBytes(rawAccum, fp, blkSize)
        fp += blkSize

    # Stream remaining pipe data: raw-forward complete BGZF blocks.
    var carry: seq[byte] =
      if bgzfPos < rawAccum.len: rawAccum[bgzfPos ..< rawAccum.len]
      else: @[]
    rawAccum.setLen(0); pending.setLen(0)
    while true:
      let n = posix.read(inputFd, cast[pointer](addr readBuf[0]), ChunkSize)
      if n <= 0: break
      carry.add(readBuf.toOpenArray(0, n.int - 1))
      var p = 0
      while p + 18 <= carry.len:
        let blkSize = bgzfBlockSize(carry.toOpenArray(p, carry.high))
        if blkSize <= 0: break
        if p + blkSize > carry.len: break
        if blkSize != BGZF_EOF.len or carry[p ..< p + blkSize] != @BGZF_EOF:
          discard outFile.writeBytes(carry, p, blkSize)
        p += blkSize
      if p > 0:
        carry = if p < carry.len: carry[p ..< carry.len] else: @[]

  else:
    # ── Uncompressed path ────────────────────────────────────────────────
    # BGZF-compress and write.
    if shardIdx == 0:
      # Write header + post-header data.
      if pending.len > 0:
        compressToBgzfMulti(outFile, pending)
    else:
      # Skip header, write post-header data.
      if hEnd < pending.len:
        compressToBgzfMulti(outFile, pending[hEnd ..< pending.len])
    pending.setLen(0)

    # Stream remaining pipe data: compress in chunks.
    var accum: seq[byte]
    while true:
      let n = posix.read(inputFd, cast[pointer](addr readBuf[0]), ChunkSize)
      if n <= 0: break
      accum.add(readBuf.toOpenArray(0, n.int - 1))
      if accum.len >= FlushThresh:
        compressToBgzfMulti(outFile, accum)
        accum.setLen(0)
    if accum.len > 0:
      compressToBgzfMulti(outFile, accum)

  # Write BGZF EOF block.
  discard outFile.writeBytes(BGZF_EOF, 0, BGZF_EOF.len)
  outFile.close()
  discard posix.close(inputFd)
  result = 0

# ---------------------------------------------------------------------------
# Direct-file gather
# ---------------------------------------------------------------------------

proc gatherShardSendfile(outFile: File; shardPath: string;
                         fmt: FileFormat) =
  ## Write one BGZF shard 1..N to outFile with header stripping, using sendfile
  ## for the post-header data blocks.  Only the header-containing blocks are
  ## read into memory; the bulk of the data is copied in kernel space.
  let fileSize = getFileSize(shardPath)
  let dataEnd = fileSize - 28  # skip trailing BGZF EOF
  let f = open(shardPath, fmRead)
  defer: f.close()
  # Scan header blocks: decompress until we find where the header ends.
  var blockPos: int64 = 0
  var decompAccum = newSeqOfCap[byte](2 * 1024 * 1024)
  var headerEnd = -1
  var hdrBuf = newSeqUninit[byte](BGZF_MAX_BLOCK_SIZE + BGZF_OVERHEAD)
  while blockPos < dataEnd and headerEnd < 0:
    f.setFilePos(blockPos)
    if readBytes(f, hdrBuf, 0, 18) < 18: break
    let blkSize = bgzfBlockSize(hdrBuf)
    if blkSize <= 0: break
    if blkSize > hdrBuf.len: hdrBuf.setLen(blkSize)
    f.setFilePos(blockPos)
    if readBytes(f, hdrBuf, 0, blkSize) < blkSize: break
    decompAccum.add(decompressBgzf(hdrBuf.toOpenArray(0, blkSize - 1)))
    blockPos += blkSize.int64
    headerEnd =
      if fmt == ffBcf: findBcfHeaderEnd(decompAccum)
      else:            findVcfHeaderEnd(decompAccum)
  if headerEnd < 0: headerEnd = decompAccum.len
  # Write recompressed post-header tail of the boundary block.
  let tail = decompAccum[headerEnd ..< decompAccum.len]
  if tail.len > 0:
    compressToBgzfMulti(outFile, tail)
  # Sendfile remaining data blocks directly from file.
  if blockPos < dataEnd:
    outFile.flushFile()
    rawCopyBytesFd(shardPath, getFileHandle(outFile).cint, blockPos, dataEnd - blockPos)

proc gatherFiles*(cfg: GatherConfig; inputPaths: seq[string]) =
  ## Concatenate pre-existing shard files into cfg.outputPath (or stdout).
  ## Shard 0 is written with its header intact; shards 1..N have headers stripped.
  ## For VCF/BCF: validates that #CHROM lines match before writing anything.
  ## No temp files are created.
  if inputPaths.len == 0:
    stderr.writeLine "error: gather: no input files provided"
    quit(1)

  # ── Phase 1: read shard 0, detect format, validate #CHROM ──────────────────
  info(&"gather: {inputPaths.len} shards")
  let s0Size = getFileSize(inputPaths[0]).int
  var s0Bytes = newSeqUninit[byte](s0Size)
  block:
    let fs0 = open(inputPaths[0], fmRead)
    discard readBytes(fs0, s0Bytes, 0, s0Size)
    fs0.close()
  let (fmt, isBgzf) = sniffStreamFormat(s0Bytes)
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

  # Write shards 1..N: strip headers, copy data blocks.
  for j in 1 ..< inputPaths.len:
    if isBgzf and cfg.compression == compBgzf:
      # Optimised: read only header blocks, sendfile the rest.
      gatherShardSendfile(outFile, inputPaths[j], fmt)
    else:
      # Fallback: in-memory path for cross-format or decompression cases.
      let jSize = getFileSize(inputPaths[j]).int
      var allBytes = newSeqUninit[byte](jSize)
      block:
        let fj = open(inputPaths[j], fmRead)
        discard readBytes(fj, allBytes, 0, jSize)
        fj.close()
      let bytes = if isBgzf: stripTrailingEof(allBytes) else: allBytes
      writeShardData(outFile, bytes, fmt, isBgzf, cfg.compression)

  if cfg.compression == compBgzf:
    discard outFile.writeBytes(BGZF_EOF, 0, BGZF_EOF.len)
  if not cfg.toStdout: outFile.close()
  info(&"gather: complete, {inputPaths.len} shards concatenated")
