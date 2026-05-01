## scatter — split a bgzipped file into N shards.
##
## This module is focused on the sharding algorithm itself:
##   - sequential scatter (`computeShards`, `doWriteShard`, `scatter`)
##
## Format knowledge — BGZF I/O, TBI/CSI index parsing, header extraction
## (`extractBcfHeaderAndFirstOffset`, `getHeaderAndFirstBlock`), and
## block splitting — lives in `bgzf`.

import std/[algorithm, atomics, cpuinfo, os, posix, sequtils, strformat, strutils]
import bgzf

# ---------------------------------------------------------------------------
# Output path helpers
# ---------------------------------------------------------------------------

proc shardOutputPath*(tmpl: string; shardIdx: int; nShards: int): string =
  ## Resolve the output path for shard shardIdx (0-based).
  ## If tmpl contains "{}": replace with zero-padded 1-based shard number.
  ## Otherwise: prepend "shard_NN." to the basename of tmpl, keep directory.
  let padded = align($(shardIdx + 1), len($nShards), '0')
  if "{}" in tmpl:
    return tmpl.replace("{}", padded)
  let dir  = tmpl.parentDir
  let base = tmpl.lastPathPart
  let prefixed = "shard_" & padded & "." & base
  result = if dir.len == 0: prefixed else: dir / prefixed

# FileFormat and Compression are imported from bgzf.

# ---------------------------------------------------------------------------
# Shard writing helpers
# ---------------------------------------------------------------------------

type ShardTask* = object
  ## All data needed to write one output shard.  Passed by value to worker
  ## threads so each thread owns its own copy of the small header/boundary
  ## buffers; the bulk of the data is read directly from vcfPath.
  vcfPath:      string
  outFd*:       cint        ## open writable file descriptor (file or pipe write-end)
  headerBgzf:   seq[byte]   ## BGZF-compressed header bytes
  boundaryTail: seq[byte]   ## raw boundary tail bytes at shard start; empty if shard starts on block boundary
  rawStart:     int64       ## start of middle raw-copy region
  rawEnd:       int64       ## end of middle raw-copy region (exclusive)
  boundaryHead: seq[byte]   ## raw boundary head bytes at shard end; empty for last shard
  eofSeq:       seq[byte]   ## BGZF EOF block (same 28 bytes for every shard)
  logLine:      string      ## pre-formatted info line; printed when writing begins
  decompress*:  bool        ## if true, decompress BGZF blocks before writing

proc writeAllFd(fd: cint; data: openArray[byte]) {.gcsafe.} =
  ## Write all bytes to fd via posix.write.  Used instead of File.writeBytes
  ## in the non-decompress path to avoid buffer conflicts with sendfile.
  var written = 0
  while written < data.len:
    let n = posix.write(fd, unsafeAddr data[written], data.len - written)
    if n <= 0: break
    written += n

proc doWriteShard*(task: ShardTask): int {.gcsafe.} =
  ## Write one shard to its output fd.  Returns 0.  Designed for use with
  ## spawn so that multiple shards can be written in parallel.
  ## IOError (broken pipe) is caught silently: when writing to a pipe whose
  ## read-end has been closed by an early-exiting child, the failure is
  ## detected via waitpid's exit code rather than via a write error.
  ## When task.decompress is true, BGZF blocks are decompressed before writing
  ## so the receiver gets a raw uncompressed byte stream.
  if task.decompress:
    var f: File
    discard open(f, FileHandle(task.outFd), fmWrite)
    try:
      let headerRaw = decompressBgzfBytes(task.headerBgzf)
      discard f.writeBytes(headerRaw, 0, headerRaw.len)
      if task.boundaryTail.len > 0:
        discard f.writeBytes(task.boundaryTail, 0, task.boundaryTail.len)
      if task.rawEnd > task.rawStart:
        decompressCopyBytes(task.vcfPath, f, task.rawStart,
                            task.rawEnd - task.rawStart)
      if task.boundaryHead.len > 0:
        discard f.writeBytes(task.boundaryHead, 0, task.boundaryHead.len)
    except IOError:
      discard
    try: f.close() except IOError: discard
  else:
    # fd-level writes + sendfile — no File wrapper to avoid buffer conflicts.
    # Boundary halves are raw bytes; compress on-the-fly before writing.
    writeAllFd(task.outFd, task.headerBgzf)
    if task.boundaryTail.len > 0:
      writeAllFd(task.outFd, compressToBgzfMulti(task.boundaryTail))
    if task.rawEnd > task.rawStart:
      copyRangeFromFile(task.vcfPath, task.outFd, task.rawStart,
                      task.rawEnd - task.rawStart)
    if task.boundaryHead.len > 0:
      writeAllFd(task.outFd, compressToBgzfMulti(task.boundaryHead))
    writeAllFd(task.outFd, task.eofSeq)
    discard posix.close(task.outFd)
  if task.logLine.len > 0:
    stderr.writeLine "info: " & task.logLine
  return 0

# ---------------------------------------------------------------------------
# Bounded workers — atomic pull from shared task/boundary lists
# ---------------------------------------------------------------------------

var gScatterNext {.global.}: Atomic[int]

type ScatterWorkerArgs = object
  tasksPtr: ptr seq[ShardTask]
  nTotal: int

proc scatterWorkerThread(args: ScatterWorkerArgs) {.thread.} =
  ## Pull shard indices atomically and write each shard.  Exactly nWorkers
  ## of these run concurrently, bounding I/O concurrency on the input file.
  while true:
    let idx = gScatterNext.fetchAdd(1, moRelaxed)
    if idx >= args.nTotal: break
    discard doWriteShard(args.tasksPtr[][idx])

type BoundarySplit = object
  head:    seq[byte]   # raw bytes [0 ..< uOff]
  tail:    seq[byte]   # raw bytes [uOff ..< len]
  blkSize: int64       # total BGZF block size at this offset

var gBoundaryNext {.global.}: Atomic[int]

type BoundaryWorkerArgs = object
  pathPtr: ptr string
  voffsPtr: ptr seq[(int64, int)]
  splitsPtr: ptr seq[BoundarySplit]
  nTotal: int

proc boundaryWorkerThread(args: BoundaryWorkerArgs) {.thread.} =
  ## Pull boundary indices atomically and split each boundary block.
  ## Each worker opens its own fd to avoid seek contention.
  let f = open(args.pathPtr[], fmRead)
  defer: f.close()
  while true:
    let bi = gBoundaryNext.fetchAdd(1, moRelaxed)
    if bi >= args.nTotal: break
    let (off, uOff) = args.voffsPtr[][bi]
    if uOff == 0:
      args.splitsPtr[][bi].blkSize = readBgzfBlockSize(f, off)
    else:
      let (h, t, sz) = splitBgzfBlockBothSides(f, off, uOff)
      args.splitsPtr[][bi] = BoundarySplit(head: h, tail: t, blkSize: sz)

# ---------------------------------------------------------------------------
# Scan-mode boundary resolution — find record boundaries by decompression
# ---------------------------------------------------------------------------

proc c_memchr(s: pointer; c: cint; n: csize_t): pointer
  {.importc: "memchr", header: "<string.h>".}

proc readDecompressBlock(f: File; off: int64): (seq[byte], int64) {.inline.} =
  ## Read and decompress the BGZF block at file offset off.
  let blkSize = readBgzfBlockSize(f, off)
  var blk = newSeqUninit[byte](blkSize)
  f.setFilePos(off)
  discard readBytes(f, blk, 0, blkSize.int)
  (decompressBgzf(blk), blkSize)

proc makeSplit(data: seq[byte]; uOff: int; blkSize: int64): BoundarySplit =
  ## Split decompressed block data at uOff; store raw byte slices.
  let head = if uOff == 0: @[] else: data[0 ..< uOff]
  let tail = if uOff >= data.len: @[] else: data[uOff ..< data.len]
  BoundarySplit(head: head, tail: tail, blkSize: blkSize)

proc resolveScanBoundaries(vcfPath: string; voffs: seq[(int64, int)];
                           candidateIdxs: seq[int]):
    (seq[(int64, int)], seq[BoundarySplit]) =
  ## Resolve scan-mode boundary candidates to actual record boundaries.
  ##
  ## Iterative: pass 1 tries the candidate block; subsequent passes expand
  ## bidirectionally (backward first).  Boundaries that collide with
  ## neighbours are dropped (reducing shard count) with a warning.
  let nB = candidateIdxs.len
  var resolved   = newSeq[bool](nB)
  var resolvedAt = newSeq[int](nB)  # index into voffs of resolved block
  var bvoffs     = newSeq[(int64, int)](nB)
  var splits     = newSeq[BoundarySplit](nB)
  let f = open(vcfPath, fmRead)
  defer: f.close()

  template tryBlock(bi, vi: int): bool =
    ## Decompress voffs[vi], search for '\n'. On success, store the split
    ## and mark boundary bi as resolved.  Returns true on success.
    let (off, _) = voffs[vi]
    let (data, blkSize) = readDecompressBlock(f, off)
    var found = false
    if data.len > 0:
      let p = c_memchr(unsafeAddr data[0], cint('\n'), data.len.csize_t)
      if p != nil:
        let uOff = cast[int](p) - cast[int](unsafeAddr data[0]) + 1
        resolved[bi] = true
        resolvedAt[bi] = vi
        bvoffs[bi] = (off, uOff)
        splits[bi] = makeSplit(data, uOff, blkSize)
        found = true
    found

  # Pass 1: try each candidate block.
  for bi in 0 ..< nB:
    discard tryBlock(bi, candidateIdxs[bi])

  # Pass 2+: expand unresolved boundaries bidirectionally.
  var distance = 1
  while true:
    var progress = false
    for bi in 0 ..< nB:
      if resolved[bi]: continue
      let vi = candidateIdxs[bi]
      let lo = if bi > 0 and resolved[bi - 1]: resolvedAt[bi - 1] + 1 else: 0
      let hi = if bi < nB - 1 and resolved[bi + 1]: resolvedAt[bi + 1] - 1 else: voffs.len - 1
      # Try backward first (keeps boundary closer to candidate).
      let bk = vi - distance
      if bk >= lo and tryBlock(bi, bk):
        progress = true; continue
      let fw = vi + distance
      if fw <= hi and tryBlock(bi, fw):
        progress = true; continue
    # Check if any unresolved boundaries can still expand.
    distance += 1
    var canExpand = false
    for bi in 0 ..< nB:
      if resolved[bi]: continue
      let vi = candidateIdxs[bi]
      let lo = if bi > 0 and resolved[bi - 1]: resolvedAt[bi - 1] + 1 else: 0
      let hi = if bi < nB - 1 and resolved[bi + 1]: resolvedAt[bi + 1] - 1 else: voffs.len - 1
      if vi - distance >= lo or vi + distance <= hi:
        canExpand = true; break
    if not canExpand: break

  # Collect resolved boundaries; warn and drop unresolved ones.
  var finalVoffs: seq[(int64, int)]
  var finalSplits: seq[BoundarySplit]
  for bi in 0 ..< nB:
    if resolved[bi]:
      finalVoffs.add(bvoffs[bi])
      finalSplits.add(splits[bi])
    else:
      stderr.writeLine &"warning: could not find record boundary near block {candidateIdxs[bi]} — dropping shard boundary"
  (finalVoffs, finalSplits)

# ---------------------------------------------------------------------------
# Shard task construction — shared by indexed and scanned paths
# ---------------------------------------------------------------------------

proc buildShardTasks(vcfPath: string; headerBytes: seq[byte];
                     firstBlockOff: int64; firstUOff: int;
                     boundaryVoffs: seq[(int64, int)];
                     splits: seq[BoundarySplit];
                     eofStart: int64): seq[ShardTask] =
  ## Build ShardTask objects from precomputed boundary splits.
  ## Shared by both indexed and scanned paths.
  let effN = boundaryVoffs.len + 1
  let eofSeq: seq[byte] = @BGZF_EOF

  # First-shard tail split (used only when firstUOff > 0 — typical for BCF).
  var firstShardTail: seq[byte]
  var firstBlkSize: int64
  if firstUOff > 0:
    let f = open(vcfPath, fmRead)
    let (_, t, sz) = splitBgzfBlockBothSides(f, firstBlockOff, firstUOff)
    f.close()
    firstShardTail = t
    firstBlkSize = sz

  result = newSeq[ShardTask](effN)
  for i in 0 ..< effN:
    let (startBlockOff, startUOff) =
      if i == 0: (firstBlockOff, firstUOff) else: boundaryVoffs[i - 1]

    let bTail: seq[byte] =
      if startUOff > 0:
        if i == 0: firstShardTail else: splits[i - 1].tail
      else:
        @[]

    let rawStart: int64 =
      if startUOff == 0:
        startBlockOff
      elif i == 0:
        startBlockOff + firstBlkSize
      else:
        startBlockOff + splits[i - 1].blkSize

    var rawEnd: int64
    var boundaryHead: seq[byte]
    if i < effN - 1:
      let (endBlockOff, _) = boundaryVoffs[i]
      rawEnd = endBlockOff
      boundaryHead = splits[i].head
    else:
      rawEnd = eofStart

    if rawEnd < rawStart: rawEnd = rawStart
    result[i] = ShardTask(vcfPath: vcfPath, outFd: -1,
                          headerBgzf: headerBytes, boundaryTail: bTail,
                          rawStart: rawStart, rawEnd: rawEnd,
                          boundaryHead: boundaryHead, eofSeq: eofSeq,
                          logLine: "")

# ---------------------------------------------------------------------------
# computeShardsIndexed — TBI/CSI path with known virtual offsets
# ---------------------------------------------------------------------------

proc computeShardsIndexed(vcfPath: string; headerBytes: seq[byte];
                          firstBlockOff: int64; firstUOff: int;
                          voffs: seq[(int64, int)]; effN: int;
                          nThreads: int; fileSize: int64): seq[ShardTask] =
  ## Index-mode: voffs have correct uOff values from TBI/CSI.
  ## Split boundary blocks in parallel using the existing boundaryWorker.
  let dataStart = firstBlockOff        # first byte of data (after header)
  let dataEnd   = fileSize - 28        # BGZF EOF block start
  let dataRange = dataEnd - dataStart

  var boundaryVoffs: seq[(int64, int)]
  var lastIdx = -1
  for i in 1 ..< effN:
    let targetOff = dataStart + (i.int64 * dataRange) div effN.int64
    # Binary search: first voff whose block_off >= targetOff.
    let idx = lowerBound(voffs, (targetOff, 0),
                         proc(a, b: (int64, int)): int = cmp(a[0], b[0]))
    # Valid range for this boundary: must not reuse an earlier voff, and must
    # leave one slot per remaining boundary (so the last i can always use
    # voffs[voffs.len-1]).
    let lo = lastIdx + 1
    let hi = voffs.len - (effN - i)   # leaves (effN-1-i) slots after this one
    var best = clamp(if idx < voffs.len: idx else: hi, lo, hi)
    # Snap backward if closer to target and still within [lo, hi].
    if best > lo and
       abs(voffs[best - 1][0] - targetOff) <= abs(voffs[best][0] - targetOff):
      best -= 1
    lastIdx = best
    boundaryVoffs.add(voffs[best])

  var splits = newSeq[BoundarySplit](boundaryVoffs.len)
  if boundaryVoffs.len > 0:
    gBoundaryNext.store(0, moRelaxed)
    let nWorkers = min(nThreads, boundaryVoffs.len)
    let args = BoundaryWorkerArgs(pathPtr: unsafeAddr vcfPath,
                                  voffsPtr: addr boundaryVoffs,
                                  splitsPtr: addr splits,
                                  nTotal: boundaryVoffs.len)
    if nWorkers <= 1:
      boundaryWorkerThread(args)
    else:
      var threads = newSeq[Thread[BoundaryWorkerArgs]](nWorkers)
      for i in 0 ..< nWorkers:
        createThread(threads[i], boundaryWorkerThread, args)
      for i in 0 ..< nWorkers:
        joinThread(threads[i])

  info(&"computeShards: {effN} shards, file size {fileSize} bytes, EOF at {fileSize - 28}")
  buildShardTasks(vcfPath, headerBytes, firstBlockOff, firstUOff,
                  boundaryVoffs, splits, fileSize - 28)

# ---------------------------------------------------------------------------
# computeShardsScanned — scan/GZI path, resolves record boundaries
# ---------------------------------------------------------------------------

proc computeShardsScanned(vcfPath: string; headerBytes: seq[byte];
                          firstBlockOff: int64; firstUOff: int;
                          voffs: seq[(int64, int)]; effN: int;
                          fileSize: int64): seq[ShardTask] =
  ## Scan-mode: voffs are block-level only (uOff=0).  Resolve each boundary
  ## to a record boundary by decompression, then build shard tasks.
  var candidateIdxs: seq[int]
  for i in 1 ..< effN:
    candidateIdxs.add((i * voffs.len) div effN)

  var (boundaryVoffs, splits) = resolveScanBoundaries(vcfPath, voffs, candidateIdxs)
  let actualN = boundaryVoffs.len + 1
  if actualN < effN:
    info(&"scan boundary resolution: reduced to {actualN} shards")

  info(&"computeShards: {actualN} shards, file size {fileSize} bytes, EOF at {fileSize - 28}")
  buildShardTasks(vcfPath, headerBytes, firstBlockOff, firstUOff,
                  boundaryVoffs, splits, fileSize - 28)

# ---------------------------------------------------------------------------
# computeShards — dispatcher
# ---------------------------------------------------------------------------

proc scanBlockOffsets(vcfPath: string; firstBlockOff: int64;
                      fileSize: int64): seq[(int64, int)] =
  ## Scan all BGZF blocks in [firstBlockOff, fileSize-28) and return as
  ## (block_off, 0) virtual offsets.  Used by --scan and no-index fallback.
  let starts = scanBgzfBlockStarts(vcfPath, startAt = firstBlockOff,
                                   endAt = fileSize - 28)
  info(&"scan: found {starts.len} data blocks")
  result = newSeqOfCap[(int64, int)](starts.len)
  for off in starts: result.add((off, 0))

proc collectVoffs(vcfPath: string; firstBlockOff: int64; fileSize: int64;
                  forceScan: bool; format: FileFormat):
    (seq[(int64, int)], bool) =
  ## Collect virtual offsets for boundary selection.  Returns (voffs, scanMode).
  ## Priority: --scan > CSI/TBI index > GZI > full scan.
  if forceScan and format != ffBcf:
    info("--scan: ignoring index, scanning all BGZF blocks")
    return (scanBlockOffsets(vcfPath, firstBlockOff, fileSize), true)

  var voffs = readIndexVirtualOffsets(vcfPath)
  voffs.keepItIf(it[0] >= firstBlockOff)
  if voffs.len > 0:
    let indexType = if fileExists(vcfPath & ".csi"): "CSI"
                    elif fileExists(vcfPath & ".tbi"): "TBI" else: "index"
    info(&"index: {indexType} with {voffs.len} offsets for {vcfPath}")
    return (voffs, false)

  let gziPath = vcfPath & ".gzi"
  if fileExists(gziPath):
    info(&"using GZI index: {gziPath}")
    let starts = parseGziBlockStarts(gziPath)
    info(&"GZI: {starts.len} block offsets")
    var gziVoffs: seq[(int64, int)]
    for off in starts:
      if off >= firstBlockOff: gziVoffs.add((off, 0))
    return (gziVoffs, true)

  stderr.writeLine &"warning: no index found for {vcfPath} — scanning BGZF blocks directly"
  stderr.writeLine "  (create an index with 'tabix -p vcf' for faster operation)"
  (scanBlockOffsets(vcfPath, firstBlockOff, fileSize), true)

proc computeShards*(vcfPath: string; nShards: int; nThreads: int = 1;
                    forceScan: bool = false;
                    format: FileFormat = ffVcf;
                    clampShards: bool = false): seq[ShardTask] =
  ## Compute per-shard byte ranges and prepend buffers for vcfPath.
  ## Returns ShardTask objects with outFd = -1 and logLine = "".
  ## Caller must set task.outFd before calling doWriteShard.
  let fileSize = getFileSize(vcfPath)

  # Extract header and first data offset.
  var headerBytes: seq[byte]
  var firstBlockOff: int64
  var firstUOff: int
  if format == ffBcf:
    let csi = vcfPath & ".csi"
    if not fileExists(csi):
      stderr.writeLine &"error: BCF input requires a CSI index: {csi} not found"
      stderr.writeLine &"  (create one with 'bcftools index {vcfPath}')"
      quit(1)
    let (hdr, fdbo, uOff) = extractBcfHeaderAndFirstOffset(vcfPath)
    headerBytes = compressToBgzfMulti(hdr)
    firstBlockOff = fdbo
    firstUOff = uOff
  else:
    let (hb, fb, uOff) = getHeaderAndFirstBlock(vcfPath)
    headerBytes = hb
    firstBlockOff = fb
    firstUOff = uOff

  # Collect virtual offsets: index > GZI > scan.
  var (voffs, scanMode) = collectVoffs(vcfPath, firstBlockOff, fileSize,
                                       forceScan, format)

  # Sort, dedupe, ensure first data offset present.
  let firstVO = (firstBlockOff, firstUOff)
  if firstVO notin voffs: voffs.add(firstVO)
  voffs.sort(proc(a, b: (int64, int)): int =
    if a[0] != b[0]: cmp(a[0], b[0]) else: cmp(a[1], b[1]))
  let preDedup = voffs.len
  var deduped: seq[(int64, int)]
  var groupStart = 0
  for i in 0 ..< voffs.len:
    if i == voffs.len - 1 or voffs[i + 1][0] != voffs[i][0]:
      deduped.add(voffs[(groupStart + i) div 2])
      groupStart = i + 1
  voffs = deduped
  if preDedup != voffs.len:
    info(&"dedup: {preDedup} -> {voffs.len} unique block offsets")
  info(&"computeShards: {voffs.len} virtual offsets, firstData=({firstBlockOff},{firstUOff})")

  # Validate shard count against available offsets.
  var effN = nShards
  if nShards > voffs.len:
    if clampShards:
      stderr.writeLine &"info: --clamp: reducing -n from {nShards} to {voffs.len} ({voffs.len} index entries available in {vcfPath})"
      effN = voffs.len
    else:
      stderr.writeLine &"error: requested {nShards} shards but only {voffs.len} index entries available in {vcfPath}"
      if format == ffVcf and not forceScan:
        stderr.writeLine &"  reduce -n to at most {voffs.len}, use --scan to scan all BGZF blocks, or pass --clamp to reduce -n automatically"
      else:
        stderr.writeLine &"  reduce -n to at most {voffs.len} or pass --clamp to reduce -n automatically"
      quit(1)

  # Dispatch to indexed or scanned path.
  if scanMode:
    computeShardsScanned(vcfPath, headerBytes, firstBlockOff, firstUOff, voffs, effN, fileSize)
  else:
    computeShardsIndexed(vcfPath, headerBytes, firstBlockOff, firstUOff,
                         voffs, effN, nThreads, fileSize)

proc scatter*(vcfPath: string; nShards: int; outputTemplate: string;
              nThreads: int = 1; forceScan: bool = false;
              format: FileFormat = ffVcf; clampShards: bool = false) =
  ## Split vcfPath into nShards bgzipped files.
  ## outputTemplate may contain {} (replaced with zero-padded shard number)
  ## or not (shard_NN. is prepended to the basename). mkdir -p is applied.
  ## nThreads controls parallelism; pass 0 to use all CPUs.
  ## Output is always BGZF compressed.
  ## When clampShards is true, nShards is reduced to the number of available
  ## index entries if it would otherwise exceed it (instead of erroring).
  let actualThreads = if nThreads == 0: countProcessors() else: nThreads
  info(&"scatter: using {actualThreads} thread(s)")
  var tasks = computeShards(vcfPath, nShards, actualThreads, forceScan, format,
                            clampShards)
  let nShards = tasks.len  # may be < requested nShards if clamped
  for i in 0 ..< nShards:
    let outPath = shardOutputPath(outputTemplate, i, nShards)
    createDir(outPath.parentDir)
    tasks[i].outFd = posix.open(outPath.cstring,
                                O_WRONLY or O_CREAT or O_TRUNC,
                                0o666.Mode)
    if tasks[i].outFd < 0:
      stderr.writeLine &"error: could not create output file: {outPath}"
      quit(1)
    if verbose:
      let pre = tasks[i].headerBgzf.len + tasks[i].boundaryTail.len
      let raw = tasks[i].rawEnd - tasks[i].rawStart
      let app = tasks[i].boundaryHead.len
      let total = pre.int64 + raw + app.int64 + tasks[i].eofSeq.len.int64
      tasks[i].logLine = &"shard {i+1}/{nShards}: {outPath} " &
        &"(prepend: {pre}B, raw: {raw}B, append: {app}B, total: {total}B)"
  if actualThreads > 1:
    gScatterNext.store(0, moRelaxed)
    let nWorkers = min(actualThreads, nShards)
    let args = ScatterWorkerArgs(tasksPtr: addr tasks, nTotal: nShards)
    var threads = newSeq[Thread[ScatterWorkerArgs]](nWorkers)
    for i in 0 ..< nWorkers:
      createThread(threads[i], scatterWorkerThread, args)
    for i in 0 ..< nWorkers:
      joinThread(threads[i])
  else:
    for task in tasks:
      discard doWriteShard(task)
