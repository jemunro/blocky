## gather — format inference and concatenation for gather subcommand.

import std/[atomics, os, strformat, strutils]
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
      decompBuf.addMem(decompressBgzf(rawBytes.toOpenArray(pos, pos + blkSize - 1)))
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
    rawBuf.addMem(buf.toOpenArray(0, got.int - 1))
    while blockPos + 18 <= rawBuf.len:
      let blkSize = bgzfBlockSize(rawBuf.toOpenArray(blockPos, rawBuf.high))
      if blkSize <= 0 or blockPos + blkSize > rawBuf.len: break
      decompBuf.addMem(decompressBgzf(rawBuf.toOpenArray(blockPos, blockPos + blkSize - 1)))
      blockPos += blkSize
    let hEnd =
      if fmt == ffBcf: findBcfHeaderEnd(decompBuf)
      else:            findVcfHeaderEnd(decompBuf)
    if hEnd >= 0:
      return extractChromLine(decompBuf[0 ..< hEnd])
  return extractChromLine(decompBuf)

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
                     chromBufPtr: ptr SharedBuf;
                     directFd: cint = -1;
                     directUncompress: bool = false;
                     compressLevel: int = 6;
                     outputCounter: ptr Atomic[int] = nil): int {.gcsafe.} =
  ## Read subprocess stdout from inputFd (pipe), strip headers for shards 1..N,
  ## BGZF-compress, and write to tmpPath. Shard 0 writes everything and sets
  ## chromBufPtr with the #CHROM line. Shards 1..N validate #CHROM against it.
  ## Returns 0 on success, 1 on #CHROM mismatch.
  ##
  ## directFd >= 0: write directly to this fd instead of tmpPath (shard 0 only).
  ##   No BGZF EOF is written; the concat thread appends it at the end.
  ## directUncompress: when directFd >= 0, write uncompressed data (-u flag).
  ## compressLevel: BGZF compression level for tmp files (1 when -u is set).
  ## outputCounter: if non-nil, incremented when subprocess produces output.
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
    if directFd < 0:
      # Write empty BGZF file (just EOF block) to tmp.
      let f = open(tmpPath, fmWrite)
      discard f.writeBytes(BGZF_EOF, 0, BGZF_EOF.len)
      f.close()
    # directFd: nothing to write (concat thread writes final EOF).
    return 0

  let (fmt, isBgzf) = sniffStreamFormat(readBuf.toOpenArray(0, initRead.int - 1))
  if outputCounter != nil:
    discard outputCounter[].fetchAdd(1, moRelaxed)
  info(&"intercept shard {shardIdx}: {fmt}, bgzf={isBgzf}")

  # Phase B: accumulate header.
  var rawAccum = newSeqOfCap[byte](ChunkSize * 2)
  var bgzfPos = 0
  var pending = newSeqOfCap[byte](ChunkSize * 4)  # decompressed bytes accumulated so far

  # Append initial read.
  if isBgzf:
    rawAccum.addMem(readBuf.toOpenArray(0, initRead.int - 1))
    while bgzfPos + 18 <= rawAccum.len:
      let blkSize = bgzfBlockSize(rawAccum.toOpenArray(bgzfPos, rawAccum.high))
      if blkSize <= 0 or bgzfPos + blkSize > rawAccum.len: break
      pending.addMem(decompressBgzf(rawAccum.toOpenArray(bgzfPos, bgzfPos + blkSize - 1)))
      bgzfPos += blkSize
  else:
    pending.addMem(readBuf.toOpenArray(0, initRead.int - 1))

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
      rawAccum.addMem(readBuf.toOpenArray(0, n.int - 1))
      while bgzfPos + 18 <= rawAccum.len:
        let blkSize = bgzfBlockSize(rawAccum.toOpenArray(bgzfPos, rawAccum.high))
        if blkSize <= 0 or bgzfPos + blkSize > rawAccum.len: break
        pending.addMem(decompressBgzf(rawAccum.toOpenArray(bgzfPos, bgzfPos + blkSize - 1)))
        bgzfPos += blkSize
    else:
      pending.addMem(readBuf.toOpenArray(0, n.int - 1))
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

  # Phase D: write output (direct to fd for shard 0, or tmp file).
  let outFile =
    if directFd >= 0:
      var f: File
      discard open(f, FileHandle(directFd), fmWrite)
      f
    else:
      open(tmpPath, fmWrite)
  let direct = directFd >= 0

  if isBgzf:
    # ── BGZF optimised path ──────────────────────────────────────────────
    if shardIdx == 0:
      # Shard 0: forward all BGZF blocks accumulated so far, then stream rest.
      if direct and directUncompress:
        # -u: write decompressed bytes directly.
        if pending.len > 0:
          discard outFile.writeBytes(pending, 0, pending.len)
      else:
        # Raw-forward BGZF blocks.
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
          compressToBgzfMulti(outFile, tail, compressLevel)
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

    # Stream remaining pipe data: forward complete BGZF blocks.
    var carry = newSeqOfCap[byte](262144)
    if bgzfPos < rawAccum.len:
      carry.addMem(rawAccum.toOpenArray(bgzfPos, rawAccum.high))
    rawAccum.setLen(0); pending.setLen(0)
    var decompBuf: seq[byte]
    while true:
      let n = posix.read(inputFd, cast[pointer](addr readBuf[0]), ChunkSize)
      if n <= 0: break
      let oldLen = carry.len
      carry.setLenUninit(oldLen + n.int)
      copyMem(addr carry[oldLen], addr readBuf[0], n.int)
      var p = 0
      while p + 18 <= carry.len:
        let blkSize = bgzfBlockSize(carry.toOpenArray(p, carry.high))
        if blkSize <= 0: break
        if p + blkSize > carry.len: break
        if not (blkSize == BGZF_EOF.len and
                carry.toOpenArray(p, p + blkSize - 1) == BGZF_EOF.toOpenArray(0, BGZF_EOF.len - 1)):
          if direct and directUncompress:
            decompressBgzfInto(carry.toOpenArray(p, p + blkSize - 1), decompBuf)
            if decompBuf.len > 0:
              discard outFile.writeBytes(decompBuf, 0, decompBuf.len)
          else:
            discard outFile.writeBytes(carry, p, blkSize)
        p += blkSize
      if p > 0:
        if p < carry.len:
          moveMem(addr carry[0], addr carry[p], carry.len - p)
          carry.setLen(carry.len - p)
        else:
          carry.setLen(0)

  else:
    # ── Uncompressed path ────────────────────────────────────────────────
    if shardIdx == 0:
      if direct and directUncompress:
        # -u direct: write raw bytes.
        if pending.len > 0:
          discard outFile.writeBytes(pending, 0, pending.len)
      else:
        # BGZF-compress header + post-header data.
        if pending.len > 0:
          compressToBgzfMulti(outFile, pending)
    else:
      # Skip header, BGZF-compress post-header data.
      if hEnd < pending.len:
        compressToBgzfMulti(outFile, pending[hEnd ..< pending.len], compressLevel)
    pending.setLen(0)

    # Stream remaining pipe data.
    var accum = newSeqOfCap[byte](FlushThresh + ChunkSize)
    while true:
      let n = posix.read(inputFd, cast[pointer](addr readBuf[0]), ChunkSize)
      if n <= 0: break
      accum.addMem(readBuf.toOpenArray(0, n.int - 1))
      if accum.len >= FlushThresh:
        if direct and directUncompress:
          discard outFile.writeBytes(accum, 0, accum.len)
        else:
          compressToBgzfMulti(outFile, accum, compressLevel)
        accum.setLen(0)
    if accum.len > 0:
      if direct and directUncompress:
        discard outFile.writeBytes(accum, 0, accum.len)
      else:
        compressToBgzfMulti(outFile, accum, compressLevel)

  if direct:
    flushFile(outFile)
    # Do NOT write EOF or close — concat thread owns the fd and writes final EOF.
  else:
    discard outFile.writeBytes(BGZF_EOF, 0, BGZF_EOF.len)
    outFile.close()
  discard posix.close(inputFd)
  result = 0

# ---------------------------------------------------------------------------
# Direct-file gather
# ---------------------------------------------------------------------------

proc gatherShard(outFile: File; shardPath: string;
                 fmt: FileFormat; isBgzf: bool;
                 compression: Compression; chromRef: string): int =
  ## Validate #CHROM, strip header, and write data blocks — all in one pass.
  ## Returns 0 on success, 1 on #CHROM mismatch (nothing written on mismatch).
  ## For BGZF input: bulk-reads header region, sendfiles remaining blocks.
  ## For uncompressed input: streams header skip + data write.
  let fileSize = getFileSize(shardPath)
  let f = open(shardPath, fmRead)
  defer: f.close()

  if isBgzf:
    let dataEnd = fileSize - 28  # skip trailing BGZF EOF
    # Bulk-read header region (1 MB covers any realistic header).
    const HdrReadSize = 1024 * 1024
    let readSize = min(dataEnd, HdrReadSize.int64).int
    var hdrRegion = newSeqUninit[byte](readSize)
    let nRead = readBytes(f, hdrRegion, 0, readSize)
    if nRead < readSize: hdrRegion.setLen(nRead)
    # Scan BGZF blocks to find header end.
    var blockPos = 0
    var decompAccum = newSeqOfCap[byte](2 * 1024 * 1024)
    var headerEnd = -1
    while blockPos < hdrRegion.len and headerEnd < 0:
      let blkSize = bgzfBlockSize(hdrRegion.toOpenArray(blockPos, hdrRegion.high))
      if blkSize <= 0 or blockPos + blkSize > hdrRegion.len: break
      decompAccum.addMem(decompressBgzf(hdrRegion.toOpenArray(blockPos, blockPos + blkSize - 1)))
      blockPos += blkSize
      headerEnd =
        if fmt == ffBcf: findBcfHeaderEnd(decompAccum)
        else:            findVcfHeaderEnd(decompAccum)
    if headerEnd < 0: headerEnd = decompAccum.len
    # Validate #CHROM before writing anything.
    if chromRef.len > 0:
      let chromLine = extractChromLine(decompAccum[0 ..< headerEnd])
      if chromLine != chromRef: return 1
    # Write post-header tail of boundary block.
    let tail = decompAccum[headerEnd ..< decompAccum.len]
    if tail.len > 0:
      if compression == compBgzf:
        compressToBgzfMulti(outFile, tail)
      else:
        discard outFile.writeBytes(tail, 0, tail.len)
    # Write remaining data blocks.
    let blockPosI64 = blockPos.int64
    if blockPosI64 < dataEnd:
      if compression == compBgzf:
        # BGZF→BGZF: sendfile raw blocks.
        outFile.flushFile()
        copyRange(getFileHandle(outFile).cint, getFileHandle(f).cint,
                    (dataEnd - blockPosI64).Off, blockPosI64.Off)
      else:
        # BGZF→uncompressed: stream-decompress remaining blocks.
        f.setFilePos(blockPosI64)
        decompressCopyBytes(shardPath, outFile, blockPosI64, dataEnd - blockPosI64)
  else:
    # Uncompressed input: read header region, find header end, stream rest.
    const HdrReadSize = 1024 * 1024
    let readSize = min(fileSize, HdrReadSize.int64).int
    var hdrRegion = newSeqUninit[byte](readSize)
    let nRead = readBytes(f, hdrRegion, 0, readSize)
    if nRead < readSize: hdrRegion.setLen(nRead)
    let headerEnd =
      case fmt
      of ffBcf:         findBcfHeaderEnd(hdrRegion)
      of ffVcf, ffText: findVcfHeaderEnd(hdrRegion)
    let dataStart = if headerEnd > 0: headerEnd else: 0
    # Validate #CHROM before writing.
    if chromRef.len > 0 and headerEnd > 0:
      let chromLine = extractChromLine(hdrRegion[0 ..< headerEnd])
      if chromLine != chromRef: return 1
    # Write post-header data.
    let remaining = fileSize - dataStart.int64
    if remaining > 0:
      if compression == compBgzf:
        # uncompressed→BGZF: write header-region tail + stream-compress rest.
        let tailLen = min(nRead - dataStart, readSize - dataStart)
        if tailLen > 0:
          compressToBgzfMulti(outFile, hdrRegion.toOpenArray(dataStart, dataStart + tailLen - 1))
        if dataStart.int64 + tailLen.int64 < fileSize:
          f.setFilePos(dataStart.int64 + tailLen.int64)
          bgzfCompressStream(f, outFile)
      else:
        # uncompressed→uncompressed: sendfile from dataStart.
        outFile.flushFile()
        copyRangeFromFile(shardPath, getFileHandle(outFile).cint, dataStart.int64, remaining)
  return 0

proc gatherFiles*(cfg: GatherConfig; inputPaths: seq[string]) =
  ## Concatenate pre-existing shard files into cfg.outputPath (or stdout).
  ## Shard 0 is written with its header intact; shards 1..N have headers stripped.
  ## For VCF/BCF: validates that #CHROM lines match before writing anything.
  ## No temp files are created.
  if inputPaths.len == 0:
    stderr.writeLine "error: gather: no input files provided"
    quit(1)

  # ── Phase 1: detect format and extract #CHROM from shard 0 header ──────────
  info(&"gather: {inputPaths.len} shards")
  # Read only the header region of shard 0 (not the entire file).
  let s0Size = getFileSize(inputPaths[0])
  let (fmt, isBgzf) = block:
    let f0 = open(inputPaths[0], fmRead)
    var peek = newSeqUninit[byte](min(s0Size, 65536).int)
    let n = readBytes(f0, peek, 0, peek.len)
    f0.close()
    if n < peek.len: peek.setLen(n)
    sniffStreamFormat(peek)
  if fmt != cfg.format and not cfg.toStdout:
    stderr.writeLine &"warning: shard 0 format detected as {fmt} " &
      &"but output expects {cfg.format}; proceeding"

  let chrom0 = if fmt in {ffVcf, ffBcf}: chromLineFromFile(inputPaths[0], fmt, isBgzf)
               else: ""

  # ── Phase 2: open output, write shard 0 then shards 1..N ───────────────────
  let outFile: File = if cfg.toStdout: stdout else: open(cfg.outputPath, fmWrite)

  # Write shard 0: no header stripping needed — stream directly.
  block:
    let fs0 = open(inputPaths[0], fmRead)
    defer: fs0.close()
    if isBgzf and cfg.compression == compBgzf:
      # BGZF→BGZF: sendfile (skip trailing 28-byte EOF).
      let copySize = s0Size - 28
      if copySize > 0:
        outFile.flushFile()
        copyRangeFromFile(inputPaths[0], getFileHandle(outFile).cint, 0, copySize)
    elif isBgzf and cfg.compression == compNone:
      # BGZF→uncompressed: stream-decompress.
      bgzfDecompressStream(fs0, outFile)
    elif not isBgzf and cfg.compression == compBgzf:
      # uncompressed→BGZF: stream-compress.
      bgzfCompressStream(fs0, outFile)
    else:
      # uncompressed→uncompressed: sendfile raw.
      outFile.flushFile()
      copyRangeFromFile(inputPaths[0], getFileHandle(outFile).cint, 0, s0Size)

  # Write shards 1..N: validate #CHROM, strip headers, stream data.
  for j in 1 ..< inputPaths.len:
    let rc = gatherShard(outFile, inputPaths[j], fmt, isBgzf,
                          cfg.compression, chrom0)
    if rc != 0:
      stderr.writeLine &"error: gather: #CHROM line mismatch in shard {j+1} ({inputPaths[j]})"
      quit(1)

  if cfg.compression == compBgzf:
    discard outFile.writeBytes(BGZF_EOF, 0, BGZF_EOF.len)
  if not cfg.toStdout: outFile.close()
  info(&"gather: complete, {inputPaths.len} shards concatenated")
