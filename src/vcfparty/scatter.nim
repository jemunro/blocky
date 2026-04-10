## scatter — split a bgzipped VCF/BCF into N shards.
##
## This module is focused on the sharding algorithm itself:
##   - sequential scatter (`computeShards`, `doWriteShard`, `scatter`)
##   - interleaved scatter (`interleavedBlockAssignment`, `writeInterleavedShard`,
##     the inbox handoff primitive for partial-record exchange between workers)
##
## VCF/BCF format knowledge — BGZF I/O, TBI/CSI index parsing, header
## extraction (`extractBcfHeader`, `getHeaderAndFirstBlock`), block-length
## maths and BCF record walking — lives in `vcf_utils`.

import std/[algorithm, atomics, cpuinfo, os, posix, sequtils, strformat, strutils]
{.warning[Deprecated]: off.}
import std/threadpool
{.warning[Deprecated]: on.}
import vcf_utils

# ---------------------------------------------------------------------------
# Optional info-level logging (enabled by --info flag via main.nim)
# ---------------------------------------------------------------------------

var verbose* = false

template info(msg: string) =
  if verbose: stderr.writeLine "info: " & msg

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

# FileFormat and Compression are imported from vcf_utils.

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

proc doWriteShard*(task: ShardTask): int {.gcsafe.} =
  ## Write one shard to its output fd.  Returns 0.  Designed for use with
  ## spawn so that multiple shards can be written in parallel.
  ## IOError (broken pipe) is caught silently: when writing to a pipe whose
  ## read-end has been closed by an early-exiting child, the failure is
  ## detected via waitpid's exit code rather than via a write error.
  ## When task.decompress is true, BGZF blocks are decompressed before writing
  ## so the receiver gets a raw uncompressed byte stream.
  if task.logLine.len > 0:
    stderr.writeLine "info: " & task.logLine
  var f: File
  discard open(f, FileHandle(task.outFd), fmWrite)
  try:
    if task.decompress:
      let prependRaw = decompressBgzfBytes(task.prepend)
      discard f.writeBytes(prependRaw, 0, prependRaw.len)
      if task.rawEnd > task.rawStart:
        decompressCopyBytes(task.vcfPath, f, task.rawStart,
                            task.rawEnd - task.rawStart)
      if task.boundaryHead.len > 0:
        let headRaw = decompressBgzfBytes(task.boundaryHead)
        discard f.writeBytes(headRaw, 0, headRaw.len)
      # No BGZF EOF block — the receiver gets a plain byte stream.
    else:
      discard f.writeBytes(task.prepend, 0, task.prepend.len)
      if task.rawEnd > task.rawStart:
        rawCopyBytes(task.vcfPath, f, task.rawStart, task.rawEnd - task.rawStart)
      if task.boundaryHead.len > 0:
        discard f.writeBytes(task.boundaryHead, 0, task.boundaryHead.len)
      discard f.writeBytes(task.eofSeq, 0, task.eofSeq.len)
  except IOError:
    discard  # broken pipe — child exited early; failure detected via waitpid
  try: f.close() except IOError: discard
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
    # VCF: header from BGZF scan; first block offset.
    let (hb, fb) = getHeaderAndFirstBlock(vcfPath)
    headerBytes = hb
    firstBlockOff = fb
    firstUOff = 0  # VCF data block starts at byte 0

  # Build the list of virtual offsets used as candidate boundary points.
  var voffs: seq[(int64, int)]
  if forceScan and format == ffVcf:
    info("--force-scan: ignoring index, scanning all BGZF blocks")
    let starts = scanBgzfBlockStarts(vcfPath, startAt = firstBlockOff,
                                     endAt = fileSize - 28)
    info(&"scan: found {starts.len} data blocks")
    for off in starts:
      voffs.add((off, 0))
  else:
    voffs = readIndexVirtualOffsets(vcfPath)
    voffs.keepItIf(it[0] >= firstBlockOff)
    if voffs.len == 0:
      # No index for VCF — fall back to scanning all blocks.
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
  var deduped: seq[(int64, int)]
  for i, v in voffs:
    if i == 0 or v != voffs[i - 1]: deduped.add(v)
  voffs = deduped
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

  result = newSeq[ShardTask](effN)
  for i in 0 ..< effN:
    var prepend: seq[byte]
    prepend.add(headerBytes)

    # Determine the start virtual offset for this shard.
    let (startBlockOff, startUOff) =
      if i == 0: (firstBlockOff, firstUOff) else: boundaryVoffs[i - 1]

    # If start block is mid-record (startUOff > 0), prepend the tail of
    # that block (bytes [startUOff, end]).  If startUOff == 0, the block
    # starts cleanly and is included in the raw-copy region.
    if startUOff > 0:
      let (_, tail) = splitBgzfBlockAtUOffset(vcfPath, startBlockOff, startUOff)
      prepend.add(tail)

    # rawStart: if startUOff > 0, skip past the split block; if 0, include it.
    var hdrBuf = newSeq[byte](18)
    block getSize:
      let fTmp = open(vcfPath, fmRead)
      fTmp.setFilePos(startBlockOff)
      discard readBytes(fTmp, hdrBuf, 0, 18)
      fTmp.close()
    let startBlkSize = bgzfBlockSize(hdrBuf).int64
    let rawStart: int64 =
      if startUOff > 0: startBlockOff + startBlkSize else: startBlockOff

    var rawEnd: int64
    var boundaryHead: seq[byte]
    if i < effN - 1:
      let (endBlockOff, endUOff) = boundaryVoffs[i]
      rawEnd = endBlockOff
      if endUOff > 0:
        let (head, _) = splitBgzfBlockAtUOffset(vcfPath, endBlockOff, endUOff)
        boundaryHead = head
    else:
      rawEnd = eofStart

    if rawEnd < rawStart: rawEnd = rawStart
    result[i] = ShardTask(vcfPath: vcfPath, outFd: -1, prepend: prepend,
                          rawStart: rawStart, rawEnd: rawEnd,
                          boundaryHead: boundaryHead, eofSeq: eofSeq,
                          logLine: "")

# ---------------------------------------------------------------------------
# Interleaved block assignment
# ---------------------------------------------------------------------------

proc interleavedBlockAssignment*(nBlocks, nShards, chunkSize: int): seq[seq[Slice[int]]] =
  ## Assign nBlocks data blocks to nShards shards in round-robin chunks of
  ## chunkSize.  Returns a seq of length nShards; each element is a list of
  ## Slice[int] index ranges into the block array.  Every block index in
  ## 0..<nBlocks appears in exactly one shard's list.
  result = newSeq[seq[Slice[int]]](nShards)
  var pos = 0
  var chunkIdx = 0
  while pos < nBlocks:
    let shard = chunkIdx mod nShards
    let hi = min(pos + chunkSize, nBlocks) - 1  # inclusive end
    result[shard].add(pos .. hi)
    pos = hi + 1
    inc chunkIdx

# ---------------------------------------------------------------------------
# Interleaved shard writer
# ---------------------------------------------------------------------------

type InboxSlot* = object
  ## Single-slot mailbox for cross-worker partial-record handoff.
  ## Writer deposits buf+len then sets ready=true; reader spins on ready.
  ## ready uses atomic operations for cross-thread visibility.
  buf*:   array[4 * 1024 * 1024, byte]
  len*:   int32
  ready*: Atomic[bool]

type InboxArray* = object
  ## Array of per-worker inbox slots, heap-allocated for GC safety.
  slots*: ptr UncheckedArray[InboxSlot]
  count*: int

proc newInboxArray*(n: int): InboxArray =
  ## Allocate n inbox slots, all zeroed (ready = false).
  let p = cast[ptr UncheckedArray[InboxSlot]](allocShared0(n * sizeof(InboxSlot)))
  InboxArray(slots: p, count: n)

proc freeInboxArray*(inboxes: InboxArray) =
  if inboxes.slots != nil:
    deallocShared(inboxes.slots)

proc deposit*(inboxes: InboxArray; workerIdx: int; data: openArray[byte]) =
  ## Deposit data into workerIdx's inbox. Spins until the slot is free (ready ==
  ## false), then writes data and sets ready = true as a release signal.
  let slot = addr inboxes.slots[workerIdx]
  # Backpressure: wait for previous drain to complete before overwriting.
  while slot.ready.load(moAcquire):
    discard  # spin — slot still occupied by previous cycle
  let n = min(data.len, slot.buf.len)
  if n > 0:
    copyMem(addr slot.buf[0], unsafeAddr data[0], n)
  slot.len = n.int32
  slot.ready.store(true, moRelease)

proc drain*(inboxes: InboxArray; workerIdx: int; timeoutMs: int = 10000): seq[byte] =
  ## Spin-wait until workerIdx's inbox is ready, then return the data and reset.
  ## Raises an error if timeout is reached (depositing worker likely died).
  let slot = addr inboxes.slots[workerIdx]
  var waited = 0
  while not slot.ready.load(moAcquire):
    sleep(1)
    waited += 1
    if waited >= timeoutMs:
      stderr.writeLine "error: inbox drain timeout for worker " & $workerIdx &
        " — depositing worker may have crashed (waited " & $waited & "ms)"
      quit(1)
  result = newSeq[byte](slot.len)
  if slot.len > 0:
    copyMem(addr result[0], addr slot.buf[0], slot.len)
  slot.len = 0
  slot.ready.store(false, moRelease)

proc depositEmpty*(inboxes: InboxArray; workerIdx: int) =
  ## Signal that no handoff is needed (record boundary fell cleanly).
  ## Spins until the slot is free before signalling.
  let slot = addr inboxes.slots[workerIdx]
  while slot.ready.load(moAcquire):
    discard  # spin — slot still occupied by previous cycle
  slot.len = 0
  slot.ready.store(true, moRelease)

type InterleavedTask* = object
  vcfPath*:      string
  outFd*:        cint
  headerBytes*:  seq[byte]        ## uncompressed VCF/BCF header
  blockStarts*:  ptr seq[int64]   ## shared: all data block file offsets
  blockSizes*:   ptr seq[int64]   ## shared: all data block byte sizes
  chunkIndices*: seq[Slice[int]]  ## this shard's index ranges into blockStarts
  format*:       FileFormat       ## ffVcf or ffBcf
  csiVoffs*:     ptr seq[(int64, int)]  ## shared: CSI virtual offsets (BCF only; nil for VCF)
  shardIdx*:     int              ## this worker's index (0-based)
  nShards*:      int              ## total number of workers
  chunkSize*:    int              ## blocks per chunk (for chunkOwner calculation)
  inboxes*:      ptr InboxArray   ## shared inbox array

proc readAndDecompressBlocks(vcfPath: string; starts: ptr seq[int64];
                              sizes: ptr seq[int64];
                              slice: Slice[int]): seq[byte] {.gcsafe.} =
  ## Read and decompress all BGZF blocks in starts[slice] from vcfPath.
  ## Each starts[i]..starts[i]+sizes[i] range may contain multiple BGZF blocks
  ## (when index entries span multiple blocks); decompress each one in sequence.
  let f = open(vcfPath, fmRead)
  defer: f.close()
  result = @[]
  for i in slice:
    let offset = starts[][i]
    let sz     = sizes[][i].int
    var raw = newSeq[byte](sz)
    f.setFilePos(offset)
    discard readBytes(f, raw, 0, sz)
    var pos = 0
    while pos < raw.len:
      let blkSize = bgzfBlockSize(raw.toOpenArray(pos, raw.high))
      if blkSize <= 0: break
      result.add(decompressBgzf(raw.toOpenArray(pos, pos + blkSize - 1)))
      pos += blkSize

proc findFirstNewline(data: openArray[byte]): int =
  ## Return index of first '\n' in data, or -1 if none.
  for i in 0 ..< data.len:
    if data[i] == byte('\n'): return i
  return -1

proc findLastNewline(data: openArray[byte]): int =
  ## Return index of last '\n' in data, or -1 if none.
  for i in countdown(data.len - 1, 0):
    if data[i] == byte('\n'): return i
  return -1

proc findBcfRecordEnd*(data: openArray[byte]): int =
  ## Walk BCF records from the start of data.  Return the index just past the
  ## last complete record, or 0 if not even one complete record fits.
  var pos = 0
  while pos + 8 <= data.len:
    let lShared = cast[ptr uint32](unsafeAddr data[pos])[]
    let lIndiv  = cast[ptr uint32](unsafeAddr data[pos + 4])[]
    let recLen  = 8 + lShared.int + lIndiv.int
    if pos + recLen > data.len:
      break  # incomplete record
    pos += recLen
  return pos

proc headSkipFromIndex*(blockOff: int64; voffs: ptr seq[(int64, int)]): int =
  ## Look up the index virtual offsets to find the head skip for a block.
  ## Returns the u_off of the virtual offset entry with matching block_off.
  ## Used for both BCF (CSI) and indexed VCF (TBI/CSI). The caller guarantees
  ## blockOff was placed at an index entry boundary, so the lookup always
  ## succeeds when the index is well-formed.
  let v = voffs[]
  var lo = 0
  var hi = v.len - 1
  while lo <= hi:
    let mid = (lo + hi) div 2
    if v[mid][0] < blockOff:
      lo = mid + 1
    elif v[mid][0] > blockOff:
      hi = mid - 1
    else:
      return v[mid][1]
  return 0  # not found — caller's blockOff wasn't at an index boundary

proc chunkOwner*(blockIdx, nShards, chunkSize: int): int {.inline.} =
  ## Return the worker index that owns the chunk containing blockIdx.
  (blockIdx div chunkSize) mod nShards

proc writeInterleavedShard*(task: InterleavedTask): int {.gcsafe.} =
  ## Write one interleaved shard using the inbox model.
  ## For each chunk: deposit head to previous worker's inbox, write complete
  ## records, then unconditionally drain own inbox.  Returns 0.
  let nTotalBlocks = task.blockStarts[].len
  var f: File
  discard open(f, FileHandle(task.outFd), fmWrite)
  var completedChunks = 0
  try:
    # Write header
    if task.headerBytes.len > 0:
      discard f.writeBytes(task.headerBytes, 0, task.headerBytes.len)

    for ci, slice in task.chunkIndices:
      let buf = readAndDecompressBlocks(task.vcfPath, task.blockStarts,
                                        task.blockSizes, slice)

      # --- Head split: bytes before first record boundary ---
      var headEnd = 0
      if slice.a > 0:
        if task.csiVoffs != nil:
          # Indexed (BCF or VCF with TBI/CSI): exact u_off lookup
          headEnd = headSkipFromIndex(task.blockStarts[][slice.a], task.csiVoffs)
        else:
          # Unindexed VCF fallback: scan for first newline
          let nlPos = findFirstNewline(buf)
          if nlPos >= 0:
            headEnd = nlPos + 1
          else:
            headEnd = buf.len  # entire chunk is one partial line

        let prevOwner = chunkOwner(slice.a - 1, task.nShards, task.chunkSize)
        if prevOwner == task.shardIdx:
          # Previous chunk is ours — write head directly (no inbox needed).
          if headEnd > 0:
            discard f.writeBytes(buf, 0, headEnd)
        else:
          # Deposit to the other worker's inbox.
          if headEnd > 0:
            task.inboxes[].deposit(prevOwner, buf.toOpenArray(0, headEnd - 1))
          else:
            task.inboxes[].depositEmpty(prevOwner)

      # --- Write records from buf[headEnd..] ---
      # Write complete records, then trailing partial + inbox data.
      if headEnd < buf.len:
        let records = buf[headEnd ..< buf.len]
        var recEnd: int
        if task.format == ffBcf:
          recEnd = findBcfRecordEnd(records)
        else:
          let nlPos = findLastNewline(records)
          recEnd = if nlPos >= 0: nlPos + 1 else: 0
        if recEnd > 0:
          discard f.writeBytes(records, 0, recEnd)
        # Write trailing partial (bytes after last complete record).
        if recEnd < records.len:
          discard f.writeBytes(records, recEnd, records.len - recEnd)

      # --- Drain own inbox if the next chunk belongs to a different worker ---
      if slice.b + 1 < nTotalBlocks:
        let nextOwner = chunkOwner(slice.b + 1, task.nShards, task.chunkSize)
        if nextOwner != task.shardIdx:
          let inboxData = task.inboxes[].drain(task.shardIdx)
          if inboxData.len > 0:
            discard f.writeBytes(inboxData, 0, inboxData.len)

      completedChunks = ci + 1

  except IOError:
    discard  # broken pipe — child exited early

  # Fulfill inbox obligations for any remaining unprocessed chunks so other
  # workers don't deadlock waiting for deposits that will never come.
  for ci in completedChunks ..< task.chunkIndices.len:
    let slice = task.chunkIndices[ci]
    if slice.a > 0:
      let prevOwner = chunkOwner(slice.a - 1, task.nShards, task.chunkSize)
      if prevOwner != task.shardIdx:
        task.inboxes[].depositEmpty(prevOwner)
    # Also drain our own inbox if expected, so the depositing worker isn't stuck.
    if slice.b + 1 < nTotalBlocks:
      let nextOwner = chunkOwner(slice.b + 1, task.nShards, task.chunkSize)
      if nextOwner != task.shardIdx:
        discard task.inboxes[].drain(task.shardIdx)

  try: f.close() except IOError: discard
  return 0

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
      let rs = tasks[i].rawStart; let re = tasks[i].rawEnd
      let bn = if i < nShards - 1: &", boundary head {tasks[i].boundaryHead.len} bytes"
               else: ""
      tasks[i].logLine = &"shard {i+1}/{nShards}: {outPath} " &
        &"(prepend {tasks[i].prepend.len}B, raw {rs}..{re} = {re-rs}B{bn})"
  if actualThreads > 1:
    var writeFVs = newSeq[FlowVar[int]](nShards)
    for i, task in tasks:
      writeFVs[i] = spawn doWriteShard(task)
    for fv in writeFVs:
      discard ^fv
  else:
    for task in tasks:
      discard doWriteShard(task)
