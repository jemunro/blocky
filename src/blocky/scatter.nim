## scatter — split a bgzipped file into N shards.
##
## This module is focused on the sharding algorithm itself:
##   - sequential scatter (`computeShards`, `doWriteShard`, `scatter`)
##
## Format knowledge — BGZF I/O, TBI/CSI index parsing, header extraction
## (`extractBcfHeaderAndFirstOffset`, `getHeaderAndFirstBlock`), and
## block splitting — lives in `bgzf`.

import std/[algorithm, atomics, cpuinfo, os, posix, sequtils, strformat, strutils]
{.warning[Deprecated]: off.}
import std/threadpool
{.warning[Deprecated]: on.}
import bgzf

# ---------------------------------------------------------------------------
# Optional info-level logging (enabled by --info flag via main.nim)
# ---------------------------------------------------------------------------

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
  ## threads so each thread owns its own copy of the small prepend/boundary
  ## buffers; the bulk of the data is read directly from vcfPath.
  vcfPath:      string
  outFd*:       cint        ## open writable file descriptor (file or pipe write-end)
  prepend:      seq[byte]   ## recompressed header + first-block tail / boundary tail
  rawStart:     int64       ## start of middle raw-copy region
  rawEnd:       int64       ## end of middle raw-copy region (exclusive)
  boundaryHead: seq[byte]   ## head half of boundary split; empty for last shard
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
      let prependRaw = decompressBgzfBytes(task.prepend)
      discard f.writeBytes(prependRaw, 0, prependRaw.len)
      if task.rawEnd > task.rawStart:
        decompressCopyBytes(task.vcfPath, f, task.rawStart,
                            task.rawEnd - task.rawStart)
      if task.boundaryHead.len > 0:
        let headRaw = decompressBgzfBytes(task.boundaryHead)
        discard f.writeBytes(headRaw, 0, headRaw.len)
    except IOError:
      discard
    try: f.close() except IOError: discard
  else:
    # fd-level writes + sendfile — no File wrapper to avoid buffer conflicts.
    writeAllFd(task.outFd, task.prepend)
    if task.rawEnd > task.rawStart:
      copyRangeFromFile(task.vcfPath, task.outFd, task.rawStart,
                      task.rawEnd - task.rawStart)
    if task.boundaryHead.len > 0:
      writeAllFd(task.outFd, task.boundaryHead)
    writeAllFd(task.outFd, task.eofSeq)
    discard posix.close(task.outFd)
  if task.logLine.len > 0:
    stderr.writeLine "info: " & task.logLine
  return 0

# ---------------------------------------------------------------------------
# Bounded workers — atomic pull from shared task/boundary lists
# ---------------------------------------------------------------------------

var gScatterNext {.global.}: Atomic[int]

proc scatterWorker(tasksPtr: ptr seq[ShardTask]; nTotal: int): int {.gcsafe.} =
  ## Pull shard indices atomically and write each shard.  Exactly nWorkers
  ## of these run concurrently, bounding I/O concurrency on the input file.
  while true:
    let idx = gScatterNext.fetchAdd(1, moRelaxed)
    if idx >= nTotal: break
    discard doWriteShard(tasksPtr[][idx])
  return 0

type BoundarySplit = object
  head:    seq[byte]   # BGZF for bytes [0 ..< uOff]
  tail:    seq[byte]   # BGZF for bytes [uOff ..< len]
  blkSize: int64       # total BGZF block size at this offset

var gBoundaryNext {.global.}: Atomic[int]

proc boundaryWorker(pathPtr: ptr string; voffsPtr: ptr seq[(int64, int)];
                    splitsPtr: ptr seq[BoundarySplit];
                    nTotal: int): int {.gcsafe.} =
  ## Pull boundary indices atomically and split each boundary block.
  ## Each worker opens its own fd to avoid seek contention.
  let f = open(pathPtr[], fmRead)
  defer: f.close()
  while true:
    let bi = gBoundaryNext.fetchAdd(1, moRelaxed)
    if bi >= nTotal: break
    let (off, uOff) = voffsPtr[][bi]
    if uOff == 0:
      splitsPtr[][bi].blkSize = readBgzfBlockSize(f, off)
    else:
      let (h, t, sz) = splitBgzfBlockBothSides(f, off, uOff)
      splitsPtr[][bi] = BoundarySplit(head: h, tail: t, blkSize: sz)
  return 0

# ---------------------------------------------------------------------------
# Phase 4 — compute shard data and scatter entry point
# ---------------------------------------------------------------------------

proc computeShards*(vcfPath: string; nShards: int; nThreads: int = 1;
                    forceScan: bool = false;
                    format: FileFormat = ffVcf;
                    clampShards: bool = false): seq[ShardTask] =
  ## Compute per-shard byte ranges and prepend buffers for vcfPath.
  ## Returns nShards ShardTask objects with outFd = -1 and logLine = "".
  ## Caller must set task.outFd before calling doWriteShard.
  ##
  ## Unified virtual-offset approach for both BCF and VCF:
  ##   1. Get header bytes and the first data virtual offset.
  ##   2. Read index virtual offsets (CSI or TBI). If no index (VCF only),
  ##      fall back to scanning all BGZF block starts and treating each as
  ##      (block_off, 0).
  ##   3. Select nShards-1 boundary virtual offsets evenly spaced by index.
  ##   4. For each boundary with u_off > 0, split the block at u_off using
  ##      `splitBgzfBlockAtUOffset` (no decompression search needed).
  let fileSize = getFileSize(vcfPath)

  var headerBytes: seq[byte]
  var firstBlockOff: int64
  var firstUOff: int

  if format == ffBcf:
    # BCF: CSI index required; no auto-scan fallback.
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
    # Text/VCF: header from BGZF scan; first block offset.
    let (hb, fb) = getHeaderAndFirstBlock(vcfPath)
    headerBytes = hb
    firstBlockOff = fb
    firstUOff = 0  # VCF data block starts at byte 0

  # Build the list of virtual offsets used as candidate boundary points.
  var voffs: seq[(int64, int)]
  if forceScan and format != ffBcf:
    info("--force-scan: ignoring index, scanning all BGZF blocks")
    let starts = scanBgzfBlockStarts(vcfPath, startAt = firstBlockOff,
                                     endAt = fileSize - 28)
    info(&"scan: found {starts.len} data blocks")
    for off in starts:
      voffs.add((off, 0))
  else:
    voffs = readIndexVirtualOffsets(vcfPath)
    voffs.keepItIf(it[0] >= firstBlockOff)
    if voffs.len > 0:
      let indexType = if fileExists(vcfPath & ".csi"): "CSI" elif fileExists(vcfPath & ".tbi"): "TBI" else: "index"
      info(&"index: {indexType} with {voffs.len} offsets for {vcfPath}")
    if voffs.len == 0:
      # No TBI/CSI index — try .gzi as a scan shortcut, else full scan.
      let gziPath = vcfPath & ".gzi"
      if fileExists(gziPath):
        info(&"using GZI index: {gziPath}")
        let starts = parseGziBlockStarts(gziPath)
        info(&"GZI: {starts.len} block offsets")
        for off in starts:
          if off >= firstBlockOff:
            voffs.add((off, 0))
      else:
        stderr.writeLine &"warning: no index found for {vcfPath} — scanning BGZF blocks directly"
        stderr.writeLine "  (create an index with 'tabix -p vcf' for faster operation)"
        let starts = scanBgzfBlockStarts(vcfPath, startAt = firstBlockOff,
                                         endAt = fileSize - 28)
        info(&"scan: found {starts.len} data blocks")
        for off in starts:
          voffs.add((off, 0))

  # Ensure the first data virtual offset is in the list, then sort + dedupe.
  let firstVO = (firstBlockOff, firstUOff)
  if firstVO notin voffs: voffs.add(firstVO)
  voffs.sort(proc(a, b: (int64, int)): int =
    if a[0] != b[0]: cmp(a[0], b[0]) else: cmp(a[1], b[1]))
  let preDedup = voffs.len
  # Collapse to one entry per block_off, picking the middle u_off.  This
  # guarantees no two consecutive shard boundaries share a BGZF block (which
  # would cause data duplication in the tail+head concatenation), and the
  # middle u_off gives the most balanced head/tail split (~L/2 each).
  var deduped: seq[(int64, int)]
  var groupStart = 0
  for i in 0 ..< voffs.len:
    if i == voffs.len - 1 or voffs[i + 1][0] != voffs[i][0]:
      let mid = (groupStart + i) div 2
      deduped.add(voffs[mid])
      groupStart = i + 1
  voffs = deduped
  if preDedup != voffs.len:
    info(&"dedup: {preDedup} -> {voffs.len} unique block offsets")
  info(&"computeShards: {voffs.len} virtual offsets, firstData=({firstBlockOff},{firstUOff})")

  # If nShards > voffs.len there are not enough index entries to place shard
  # boundaries: each chunk needs at least one index entry. Either clamp the
  # shard count down (clampShards) or error out.
  var effN = nShards
  if nShards > voffs.len:
    if clampShards:
      stderr.writeLine &"info: --clamp-shards: reducing -n from {nShards} to {voffs.len} ({voffs.len} index entries available in {vcfPath})"
      effN = voffs.len
    else:
      stderr.writeLine &"error: requested {nShards} shards but only {voffs.len} index entries available in {vcfPath}"
      if format == ffVcf and not forceScan:
        stderr.writeLine &"  reduce -n to at most {voffs.len}, use --force-scan to scan all BGZF blocks, or pass --clamp-shards to reduce -n automatically"
      else:
        stderr.writeLine &"  reduce -n to at most {voffs.len} or pass --clamp-shards to reduce -n automatically"
      quit(1)

  # Select effN-1 boundary virtual offsets, evenly spaced by index.
  var boundaryVoffs: seq[(int64, int)]
  for i in 1 ..< effN:
    let idx = (i * voffs.len) div effN
    boundaryVoffs.add(voffs[idx])

  let eofStart = fileSize - 28
  let eofSeq: seq[byte] = @BGZF_EOF
  info(&"computeShards: {effN} shards, file size {fileSize} bytes, EOF at {eofStart}")

  # Precompute every boundary-block split in parallel.  Each boundary is
  # shared between shard i (uses .head as its boundaryHead) and shard i+1
  # (uses .tail as the start of its prepend), so doing it once per boundary
  # halves decompress + compress work at scatter boundaries.
  var splits = newSeq[BoundarySplit](boundaryVoffs.len)
  if boundaryVoffs.len > 0:
    gBoundaryNext.store(0, moRelaxed)
    let nWorkers = min(nThreads, boundaryVoffs.len)
    var bfvs = newSeq[FlowVar[int]](nWorkers)
    for i in 0 ..< nWorkers:
      bfvs[i] = spawn boundaryWorker(unsafeAddr vcfPath, addr boundaryVoffs,
                                      addr splits, boundaryVoffs.len)
    for fv in bfvs: discard ^fv
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
    var prepend: seq[byte]
    prepend.add(headerBytes)

    # Determine the start virtual offset for this shard.
    let (startBlockOff, startUOff) =
      if i == 0: (firstBlockOff, firstUOff) else: boundaryVoffs[i - 1]

    # If start block is mid-record (startUOff > 0), prepend the precomputed
    # tail of that block.  Otherwise the block is included cleanly in the
    # raw-copy region and no prepend is needed.
    if startUOff > 0:
      if i == 0:
        prepend.add(firstShardTail)
      else:
        prepend.add(splits[i - 1].tail)

    # rawStart: if startUOff > 0, skip past the split block; if 0, include it.
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
      boundaryHead = splits[i].head    # empty when endUOff == 0
    else:
      rawEnd = eofStart

    if rawEnd < rawStart: rawEnd = rawStart
    result[i] = ShardTask(vcfPath: vcfPath, outFd: -1, prepend: prepend,
                          rawStart: rawStart, rawEnd: rawEnd,
                          boundaryHead: boundaryHead, eofSeq: eofSeq,
                          logLine: "")

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
  setMaxPoolSize(actualThreads)
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
      let pre = tasks[i].prepend.len
      let raw = tasks[i].rawEnd - tasks[i].rawStart
      let app = tasks[i].boundaryHead.len
      let total = pre.int64 + raw + app.int64 + tasks[i].eofSeq.len.int64
      tasks[i].logLine = &"shard {i+1}/{nShards}: {outPath} " &
        &"(prepend: {pre}B, raw: {raw}B, append: {app}B, total: {total}B)"
  if actualThreads > 1:
    gScatterNext.store(0, moRelaxed)
    let nWorkers = min(actualThreads, nShards)
    var fvs = newSeq[FlowVar[int]](nWorkers)
    for i in 0 ..< nWorkers:
      fvs[i] = spawn scatterWorker(addr tasks, nShards)
    for fv in fvs:
      discard ^fv
  else:
    for task in tasks:
      discard doWriteShard(task)
