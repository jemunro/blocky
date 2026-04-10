## run — scatter a VCF into N shards and pipe each through a tool pipeline.
##
## This module is responsible for:
##   1. Parsing the "---"-separated argv into vcfparty args + pipeline stages.
##   2. Building the sh -c command string for each shard.
##   3. Mode inference from -o / {} flags.
##   4. Executing per-shard pipelines concurrently (all N shards run at once).

import std/[algorithm, cpuinfo, os, posix, sequtils, strformat, strutils]
{.warning[Deprecated]: off.}
import std/threadpool
{.warning[Deprecated]: on.}
import scatter
import gather
import vcf_utils
import std/locks

# ---------------------------------------------------------------------------
# Argv parsing
# ---------------------------------------------------------------------------

type TerminalOp* = enum
  topNone,    ## no terminal operator — tool manages output via {}
  topConcat,  ## +concat+  gather in genomic order via temp files
  topMerge,   ## +merge+   k-way merge sort (interleaved scatter, future)
  topCollect  ## +collect+ streaming gather in arrival order

proc toTerminalOp*(tok: string): TerminalOp {.inline.} =
  ## Return the TerminalOp for tok, or topNone if tok is not a terminal operator.
  case tok
  of "+concat+":  topConcat
  of "+merge+":   topMerge
  of "+collect+": topCollect
  else:           topNone

proc isSep(tok: string): bool {.inline.} =
  ## Return true for "---" or ":::" — both are valid pipeline stage separators.
  tok == "---" or tok == ":::"

proc parseRunArgv*(argv: seq[string]): (seq[string], seq[seq[string]], TerminalOp) =
  ## Split argv at "---" / ":::" separators and extract the terminal operator.
  ## Returns (vcfpartyArgs, stages, terminalOp).
  ## Terminal operators (+concat+, +merge+, +collect+) terminate the last stage;
  ## no tokens may follow the terminal operator.
  ## Exits 1 with a message if no separator is present, any stage is empty,
  ## multiple terminal operators are found, or tokens appear after the terminal op.
  var firstSep = -1
  for i, tok in argv:
    if isSep(tok):
      firstSep = i
      break
  if firstSep < 0:
    stderr.writeLine "vcfparty run: at least one --- stage is required"
    quit(1)
  let vcfpartyArgs = argv[0 ..< firstSep]

  # First pass: locate the terminal operator (if any).
  var termOpIdx = -1
  var termOp    = topNone
  for i in firstSep + 1 ..< argv.len:
    let op = toTerminalOp(argv[i])
    if op != topNone:
      if termOp != topNone:
        stderr.writeLine "vcfparty run: multiple terminal operators not allowed"
        quit(1)
      termOp    = op
      termOpIdx = i
  # Nothing may follow the terminal operator.
  if termOpIdx >= 0 and termOpIdx + 1 < argv.len:
    stderr.writeLine "vcfparty run: unexpected tokens after '" & argv[termOpIdx] &
                     "': " & argv[termOpIdx + 1]
    quit(1)

  # Second pass: parse stages up to (but not including) the terminal operator.
  let stageEnd = if termOpIdx >= 0: termOpIdx else: argv.len
  var stages: seq[seq[string]]
  var cur: seq[string]
  for i in firstSep + 1 ..< stageEnd:
    if isSep(argv[i]):
      if cur.len == 0:
        stderr.writeLine "vcfparty run: empty pipeline stage"
        quit(1)
      stages.add(cur)
      cur = @[]
    else:
      cur.add(argv[i])
  if cur.len == 0:
    stderr.writeLine "vcfparty run: empty pipeline stage"
    quit(1)
  stages.add(cur)
  result = (vcfpartyArgs, stages, termOp)

# ---------------------------------------------------------------------------
# Shell command construction
# ---------------------------------------------------------------------------

proc buildShellCmd*(stages: seq[seq[string]]): string =
  ## Build a sh -c command string from pipeline stages (no {} substitution).
  ## Each token is shell-quoted so special characters (< > | & etc.) in
  ## filter expressions are passed through safely.  Stages are joined with " | ".
  var parts: seq[string]
  for stage in stages:
    parts.add(stage.mapIt(quoteShell(it)).join(" "))
  result = parts.join(" | ")

# ---------------------------------------------------------------------------
# Mode inference
# ---------------------------------------------------------------------------

type RunMode* = enum
  rmNormal,      ## vcfparty writes shard output files via -o
  rmToolManaged  ## tool manages own output; vcfparty discards shard stdout

proc hasBracePlaceholder*(stages: seq[seq[string]]): bool =
  ## Return true if any token in any stage contains an unescaped {}.
  ## A \{} sequence is considered escaped and does NOT count.
  for stage in stages:
    for tok in stage:
      var i = 0
      while i < tok.len:
        if tok[i] == '\\' and i + 2 < tok.len and tok[i+1] == '{' and tok[i+2] == '}':
          i += 3  # skip \{}
        elif tok[i] == '{' and i + 1 < tok.len and tok[i+1] == '}':
          return true
        else:
          i += 1
  false

proc inferRunMode*(hasOutput: bool; hasBrace: bool): RunMode =
  ## Infer run mode from -o presence and {} in tool cmd.
  ## Emits a warning when -o is ignored (tool-managed mode).
  ## Calls quit(1) when no output of any kind is specified.
  if not hasOutput and not hasBrace:
    stderr.writeLine "error: no output specified: provide -o or {} in the tool command"
    quit(1)
  if hasBrace:
    if hasOutput:
      stderr.writeLine "warning: -o is ignored in tool-managed mode (tool command contains {})"
    return rmToolManaged
  return rmNormal

# ---------------------------------------------------------------------------
# {} substitution
# ---------------------------------------------------------------------------

proc substituteToken*(tok: string; shardNum: string): string =
  ## Replace each unescaped {} in tok with shardNum.
  ## Replace \{} with a literal {} (backslash consumed by vcfparty).
  ## Other characters are copied unchanged.
  var r = newStringOfCap(tok.len + shardNum.len)
  var i = 0
  while i < tok.len:
    if tok[i] == '\\' and i + 2 < tok.len and tok[i+1] == '{' and tok[i+2] == '}':
      r.add('{')
      r.add('}')
      i += 3
    elif tok[i] == '{' and i + 1 < tok.len and tok[i+1] == '}':
      r.add(shardNum)
      i += 2
    else:
      r.add(tok[i])
      i += 1
  r

proc buildShellCmdForShard*(stages: seq[seq[string]]; shardIdx: int; nShards: int): string =
  ## Build a per-shard shell command with {} replaced by the zero-padded shard number.
  ## \{} in tokens is replaced by a literal {} passed to the tool.
  let padded = align($(shardIdx + 1), len($nShards), '0')
  var parts: seq[string]
  for stage in stages:
    parts.add(stage.mapIt(quoteShell(substituteToken(it, padded))).join(" "))
  result = parts.join(" | ")

# ---------------------------------------------------------------------------
# Per-shard pipe execution (R3/R4)
# ---------------------------------------------------------------------------

type InFlight = object
  ## Tracks one active shard: child process + writer thread + optional interceptor/feeder.
  pid:      Pid
  writeFv:  FlowVar[int]
  extraFv:  FlowVar[int]  ## interceptor or feeder thread; nil if unused
  shardIdx: int
  tmpPath:  string         ## non-empty only for gather shards 1..N

proc forkExecSh(pipeReadFd: cint; pipeWriteFd: cint; stdoutFd: cint;
                shellCmd: string; shardIdx: int): Pid =
  ## Fork a child that runs sh -c shellCmd with stdin = pipeReadFd and
  ## stdout = stdoutFd.  stderr is inherited.  Returns child PID.
  ## pipeWriteFd is closed in the child so the child does not hold the
  ## write-end of its own stdin pipe (which would prevent EOF).
  let pid = posix.fork()
  if pid < 0:
    stderr.writeLine &"error: fork() failed for shard {shardIdx + 1}"
    quit(1)
  if pid == 0:
    # Child: rewire stdin and stdout, close the pipe write-end, exec shell.
    if posix.dup2(pipeReadFd, STDIN_FILENO) < 0 or
       posix.dup2(stdoutFd,   STDOUT_FILENO) < 0:
      exitnow(1)
    discard posix.close(pipeReadFd)
    discard posix.close(pipeWriteFd)
    discard posix.close(stdoutFd)
    let args = allocCStringArray(["sh", "-c", shellCmd])
    discard posix.execvp("sh", args)
    deallocCStringArray(args)
    exitnow(127)
  result = pid

proc killAll(running: seq[InFlight]) =
  ## Send SIGTERM to every in-flight child process.
  for s in running:
    discard posix.kill(s.pid, SIGTERM)

proc waitOne(running: var seq[InFlight]; failed: var bool) =
  ## Wait for any one child to finish; sync writer and optional extra thread; record failure.
  var status: cint
  let donePid = posix.waitpid(-1, status, 0)
  let code    = int((status shr 8) and 0xff)
  var j = 0
  while j < running.len:
    if running[j].pid == donePid:
      discard ^running[j].writeFv
      var ok = (code == 0)
      if running[j].extraFv != nil:
        ok = ok and ((^running[j].extraFv) == 0)
      if not ok:
        stderr.writeLine &"shard {running[j].shardIdx + 1}: pipeline exited with code {code}"
        failed = true
      running.del(j)
      return
    j += 1

# ---------------------------------------------------------------------------
# Shared helpers: pipe setup, shard resolution, common reap
# ---------------------------------------------------------------------------

type PipelineMode* = enum
  pmTool,     ## topNone — tool-managed or per-shard file output
  pmConcat,   ## +concat+
  pmCollect,  ## +collect+
  pmMerge     ## +merge+

type ShardPipes = object
  ## Per-shard pipe set. Unused fields are -1.
  ## After forkShardChild, the parent's copies of child-owned fds are closed
  ## and set to -1. Fields kept alive after fork:
  ##   pmTool:    stdinW (writer)
  ##   pmConcat:  stdinW (writer), stdoutR (interceptor)
  ##   pmCollect: stdinW (writer), stdoutR (interceptor)
  ##   pmMerge:   stdinW (writer), stdoutR (feeder), relayR (kWayMerge),
  ##              relayW (feeder)
  stdinR*, stdinW*:   cint
  stdoutR*, stdoutW*: cint
  relayR*, relayW*:   cint
  outFileFd*:         cint

proc initShardPipes(): ShardPipes =
  ShardPipes(stdinR: -1, stdinW: -1, stdoutR: -1, stdoutW: -1,
             relayR: -1, relayW: -1, outFileFd: -1)

proc openShardPipes(mode: PipelineMode; shardIdx, nShards: int;
                    outputTemplate: string; toolManaged: bool): ShardPipes =
  ## Allocate the pipe set and (for pmTool) the per-shard output fd.
  ## Sets FD_CLOEXEC on parent-retained fds and enlarges pipe buffers on Linux
  ## for pmMerge (avoids bidirectional stdin/stdout/relay pipe deadlock).
  result = initShardPipes()
  var stdinPipe: array[2, cint]
  if posix.pipe(stdinPipe) != 0:
    stderr.writeLine &"error: pipe() failed for shard {shardIdx + 1}"
    quit(1)
  discard posix.fcntl(stdinPipe[1], F_SETFD, FD_CLOEXEC)
  result.stdinR = stdinPipe[0]
  result.stdinW = stdinPipe[1]

  case mode
  of pmTool:
    if toolManaged:
      result.outFileFd = posix.open("/dev/null".cstring, O_WRONLY)
      if result.outFileFd < 0:
        stderr.writeLine &"error: could not open /dev/null for shard {shardIdx + 1}"
        quit(1)
    else:
      let outPath = shardOutputPath(outputTemplate, shardIdx, nShards)
      createDir(outPath.parentDir)
      result.outFileFd = posix.open(outPath.cstring,
                                    O_WRONLY or O_CREAT or O_TRUNC,
                                    0o666.Mode)
      if result.outFileFd < 0:
        stderr.writeLine &"error: could not create output file: {outPath}"
        quit(1)
  of pmConcat, pmCollect, pmMerge:
    var stdoutPipe: array[2, cint]
    if posix.pipe(stdoutPipe) != 0:
      stderr.writeLine &"error: pipe() failed for shard {shardIdx + 1}"
      quit(1)
    discard posix.fcntl(stdoutPipe[0], F_SETFD, FD_CLOEXEC)
    result.stdoutR = stdoutPipe[0]
    result.stdoutW = stdoutPipe[1]
    if mode == pmMerge:
      var relayPipe: array[2, cint]
      if posix.pipe(relayPipe) != 0:
        stderr.writeLine &"error: pipe() failed for shard {shardIdx + 1}"
        quit(1)
      discard posix.fcntl(relayPipe[0], F_SETFD, FD_CLOEXEC)
      discard posix.fcntl(relayPipe[1], F_SETFD, FD_CLOEXEC)
      result.relayR = relayPipe[0]
      result.relayW = relayPipe[1]
      # Enlarge pipe buffers to avoid bidirectional pipe deadlock: writer blocks
      # on stdin (full) while subprocess blocks on stdout (full) while feeder
      # blocks on relay (full). 1MB accommodates typical per-chunk data.
      when defined(linux):
        const F_SETPIPE_SZ = cint(1031)
        const PipeBufSize  = cint(1048576)  # 1 MB
        discard posix.fcntl(result.stdinR,  F_SETPIPE_SZ, PipeBufSize)
        discard posix.fcntl(result.stdoutR, F_SETPIPE_SZ, PipeBufSize)
        discard posix.fcntl(result.relayR,  F_SETPIPE_SZ, PipeBufSize)

proc forkShardChild(pipes: var ShardPipes; mode: PipelineMode;
                    shellCmd: string; shardIdx: int): Pid =
  ## Fork the child with the correct fd routing for `mode`. After the fork,
  ## the parent closes its copies of the child-owned fds (stdinR, and either
  ## outFileFd for pmTool or stdoutW for the other modes) and sets those
  ## fields to -1 so the caller does not accidentally double-close.
  let childStdout =
    case mode
    of pmTool:                       pipes.outFileFd
    of pmConcat, pmCollect, pmMerge: pipes.stdoutW
  result = forkExecSh(pipes.stdinR, pipes.stdinW, childStdout, shellCmd, shardIdx)
  discard posix.close(pipes.stdinR); pipes.stdinR = -1
  case mode
  of pmTool:
    discard posix.close(pipes.outFileFd); pipes.outFileFd = -1
  of pmConcat, pmCollect, pmMerge:
    discard posix.close(pipes.stdoutW); pipes.stdoutW = -1

proc resolveShards(vcfPath: string; nShards, nThreads: int;
                   forceScan, clampShards: bool):
    tuple[tasks: seq[ShardTask]; nShards: int] =
  ## Shared pmTool/pmConcat/pmCollect preamble: thread-pool sizing +
  ## sequential shard computation + post-clamp nShards. pmMerge does not
  ## use this (it needs its own interleaved block assembly).
  let actualThreads = if nThreads == 0: countProcessors() else: nThreads
  setMaxPoolSize(nShards * 2)
  let fmt = if vcfPath.endsWith(".bcf"): ffBcf else: ffVcf
  let tasks = computeShards(vcfPath, nShards, actualThreads, forceScan, fmt,
                            clampShards)
  (tasks: tasks, nShards: tasks.len)

proc reapAll(inFlight: var seq[InFlight]; noKill: bool; anyFailed: var bool) =
  ## Standard reap tail used by pmTool / pmConcat / pmCollect:
  ## if a failure has been observed and --no-kill is off, SIGTERM any
  ## still-running children, then drain every remaining child via waitOne.
  if anyFailed and not noKill:
    killAll(inFlight)
  while inFlight.len > 0:
    waitOne(inFlight, anyFailed)

template drainLaunch(nShardsVal: int; noKillVal: bool;
                     inFlightVar: var seq[InFlight]; anyFailedVar: var bool;
                     body: untyped) =
  ## Common drain-launch loop: iterate `i` from 0 to nShards-1, draining one
  ## finished shard whenever the in-flight set is full, and stopping early on
  ## failure unless --no-kill is set. The per-mode launch body runs once per
  ## iteration with `i` injected.
  for i {.inject.} in 0 ..< nShardsVal:
    if anyFailedVar and not noKillVal: break
    while inFlightVar.len >= nShardsVal:
      waitOne(inFlightVar, anyFailedVar)
      if anyFailedVar and not noKillVal: break
    if anyFailedVar and not noKillVal: break
    body

# ---------------------------------------------------------------------------
# +collect+ streaming interceptor (arrival-order, no temp files)
# ---------------------------------------------------------------------------

var gCollectLock {.global.}: Lock

proc writeUnderCollectLock(outFd: cint; data: seq[byte]) {.gcsafe.} =
  ## Write data to outFd under gCollectLock. No-op for empty data.
  if data.len == 0: return
  acquire(gCollectLock)
  var written = 0
  while written < data.len:
    let n = posix.write(outFd,
                        cast[pointer](unsafeAddr data[written]),
                        data.len - written)
    if n <= 0: break
    written += n
  release(gCollectLock)

proc lastVcfOrTextRecordEnd*(buf: seq[byte]): int {.gcsafe.} =
  ## Return index one past the last '\n' in buf, or 0 if none.
  for i in countdown(buf.len - 1, 0):
    if buf[i] == byte('\n'): return i + 1
  0

proc lastBcfRecordEnd*(buf: seq[byte]): int {.gcsafe.} =
  ## Return index one past the last complete BCF record in buf, or 0 if none.
  var pos = 0
  var last = 0
  while pos + 8 <= buf.len:
    let lS = buf[pos].uint32 or (buf[pos+1].uint32 shl 8) or
             (buf[pos+2].uint32 shl 16) or (buf[pos+3].uint32 shl 24)
    let lI = buf[pos+4].uint32 or (buf[pos+5].uint32 shl 8) or
             (buf[pos+6].uint32 shl 16) or (buf[pos+7].uint32 shl 24)
    let sz = 8 + lS.int + lI.int
    if pos + sz > buf.len: break
    pos += sz
    last = pos
  last

proc doCollectInterceptor*(shardIdx: int; inputFd: cint; outFd: cint): int {.gcsafe.} =
  ## Per-shard collect interceptor. Reads from inputFd in a streaming loop,
  ## writing complete records to outFd under gCollectLock on each iteration.
  ## Shard 0 writes the header then records; shards 1..N strip the header.
  ## Returns 0 on success.
  const ReadSize = 65536
  var raw     = newSeqUninit[byte](ReadSize)
  var fmt:    FileFormat
  var isBgzf: bool
  var pending:  seq[byte]   ## accumulated decompressed bytes not yet written
  var rawAccum: seq[byte]   ## raw bytes accumulated for BGZF block reassembly
  var bgzfPos = 0           ## offset into rawAccum of next unprocessed BGZF block

  # ── Format detection ───────────────────────────────────────────────────
  if shardIdx == 0:
    let n = posix.read(inputFd, cast[pointer](addr raw[0]), ReadSize)
    if n <= 0:
      discard posix.close(inputFd)
      gStreamProbe.chromLine.ready = true   # unblock shards 1..N even on empty shard 0
      return 0
    let (detFmt, detBgzf) = sniffStreamFormat(raw.toOpenArray(0, n.int - 1))
    fmt    = detFmt
    isBgzf = detBgzf
    appendReadToAccum(raw, n.int, isBgzf, rawAccum, bgzfPos, pending)
  else:
    while not gStreamProbe.chromLine.ready: sleep(1)
    fmt    = gStreamProbe.format
    isBgzf = gStreamProbe.isBgzf

  # ── Header accumulation ────────────────────────────────────────────────
  # Read until we have the full header in pending.
  var hEnd = -1
  while hEnd < 0:
    hEnd =
      case fmt
      of ffBcf:  findBcfHeaderEnd(pending)
      of ffVcf:  findVcfHeaderEnd(pending)
      of ffText: 0
    if hEnd >= 0: break
    let n = posix.read(inputFd, cast[pointer](addr raw[0]), ReadSize)
    if n <= 0: break
    appendReadToAccum(raw, n.int, isBgzf, rawAccum, bgzfPos, pending)
  if hEnd < 0: hEnd = pending.len

  if shardIdx == 0:
    # Write header first, then release shards 1..N.
    writeUnderCollectLock(outFd, pending[0 ..< hEnd])
    gStreamProbe.format = fmt
    gStreamProbe.isBgzf   = isBgzf
    gStreamProbe.chromLine.len   = 0
    gStreamProbe.chromLine.ready = true

  # Advance past header.
  pending = if hEnd < pending.len: pending[hEnd ..< pending.len] else: @[]

  # ── Streaming record writes ────────────────────────────────────────────
  # On each read(), find complete records and mutex-write them.
  while true:
    let eIdx =
      case fmt
      of ffVcf, ffText: lastVcfOrTextRecordEnd(pending)
      of ffBcf:         lastBcfRecordEnd(pending)
    if eIdx > 0:
      writeUnderCollectLock(outFd, pending[0 ..< eIdx])
      pending = if eIdx < pending.len: pending[eIdx ..< pending.len] else: @[]
    let n = posix.read(inputFd, cast[pointer](addr raw[0]), ReadSize)
    if n <= 0: break
    appendReadToAccum(raw, n.int, isBgzf, rawAccum, bgzfPos, pending)

  # Final flush of any trailing complete records.
  let eIdx =
    case fmt
    of ffVcf, ffText: lastVcfOrTextRecordEnd(pending)
    of ffBcf:         lastBcfRecordEnd(pending)
  if eIdx > 0:
    writeUnderCollectLock(outFd, pending[0 ..< eIdx])

  discard posix.close(inputFd)
  result = 0

# ---------------------------------------------------------------------------
# +merge+ feeder (relays post-header bytes to kWayMerge relay pipe)
# ---------------------------------------------------------------------------

proc doMergeFeeder(shardIdx: int; srcFd: cint; relayWriteFd: cint): int {.gcsafe.} =
  ## Read from srcFd (subprocess stdout), strip VCF/BCF header, relay
  ## post-header bytes (decompressed if BGZF) to relayWriteFd.
  ## Shard 0: sets gStreamProbe.format and gStreamProbe.header.ready when header is found.
  ## Closes relayWriteFd and srcFd before returning.
  const ReadSize = 65536
  var raw      = newSeqUninit[byte](ReadSize)
  var pending: seq[byte]
  var isBgzf   = false
  var fmt      = ffVcf
  var rawAccum: seq[byte]
  var bgzfPos  = 0

  # --- First read: format + BGZF detection ---
  let n0 = posix.read(srcFd, cast[pointer](addr raw[0]), ReadSize)
  if n0 <= 0:
    discard posix.close(relayWriteFd)
    if shardIdx == 0:
      gStreamProbe.format      = ffVcf
      gStreamProbe.header.ready = true
    discard posix.close(srcFd)
    return 0

  let (detFmt, detBgzf) = sniffStreamFormat(raw.toOpenArray(0, n0.int - 1))
  fmt    = detFmt
  isBgzf = detBgzf
  if isBgzf and not gStreamProbe.bgzfWarned and
     not isBgzfLevel0(raw.toOpenArray(0, n0.int - 1)):
    gStreamProbe.bgzfWarned = true
    stderr.writeLine "warning: +merge+ works best with uncompressed output (-Ou/-Ov) from the last pipeline stage"
  appendReadToAccum(raw, n0.int, isBgzf, rawAccum, bgzfPos, pending)

  # --- Header accumulation ---
  var hEnd = -1
  while hEnd < 0:
    hEnd =
      case fmt
      of ffBcf:  findBcfHeaderEnd(pending)
      of ffVcf:  findVcfHeaderEnd(pending)
      of ffText: 0
    if hEnd >= 0: break
    let n = posix.read(srcFd, cast[pointer](addr raw[0]), ReadSize)
    if n <= 0: break
    appendReadToAccum(raw, n.int, isBgzf, rawAccum, bgzfPos, pending)
  if hEnd < 0: hEnd = pending.len

  # --- Signal shard 0 format availability ---
  if shardIdx == 0:
    let sz = min(hEnd, gStreamProbe.header.buf.len)
    if sz > 0:
      copyMem(addr gStreamProbe.header.buf[0], unsafeAddr pending[0], sz)
    gStreamProbe.header.len   = sz.int32
    gStreamProbe.format      = fmt
    gStreamProbe.header.ready = true

  # --- Relay post-header records to relayWriteFd ---
  template relayBytes(data: openArray[byte]) =
    var w = 0
    while w < data.len:
      let nw = posix.write(relayWriteFd, cast[pointer](unsafeAddr data[w]),
                           data.len - w)
      if nw <= 0:
        discard posix.close(relayWriteFd)
        discard posix.close(srcFd)
        return 0
      w += nw

  # Flush already-buffered post-header bytes.
  if hEnd < pending.len:
    relayBytes(pending.toOpenArray(hEnd, pending.high))
  pending = @[]

  # Continue reading and relaying until srcFd EOF.
  while true:
    let n = posix.read(srcFd, cast[pointer](addr raw[0]), ReadSize)
    if n <= 0: break
    if isBgzf:
      rawAccum.add(raw.toOpenArray(0, n.int - 1))
      flushBgzfAccum(rawAccum, bgzfPos, pending)
      if pending.len > 0:
        relayBytes(pending.toOpenArray(0, pending.high))
        pending = @[]
    else:
      relayBytes(raw.toOpenArray(0, n - 1))

  discard posix.close(relayWriteFd)
  discard posix.close(srcFd)
  result = 0

# ---------------------------------------------------------------------------
# Public unified entry point: runPipeline + RunPipelineCfg
# ---------------------------------------------------------------------------

type RunPipelineCfg* = object
  ## Single configuration record for every terminal-operator mode. Unused
  ## fields per mode are ignored. Built once in main.nim and passed to
  ## runPipeline, which dispatches on `mode`.
  vcfPath*:      string
  nShards*:      int
  nThreads*:     int
  forceScan*:    bool
  stages*:       seq[seq[string]]
  noKill*:       bool
  clampShards*:  bool
  mode*:         PipelineMode

  # pmTool only
  outputTemplate*: string
  toolManaged*:    bool

  # pmConcat / pmCollect / pmMerge
  outputPath*:     string   ## "" or "/dev/stdout" => stdout
  toStdout*:       bool

  # pmConcat only
  gather*:         GatherConfig

proc runPipelineTool(cfg: RunPipelineCfg) =
  ## topNone: scatter into N shards, pipe each through the pipeline. Either
  ## discards shard stdout to /dev/null (tool-managed) or writes per-shard
  ## output files via cfg.outputTemplate. No gather.
  let (tasks, nShards) = resolveShards(cfg.vcfPath, cfg.nShards, cfg.nThreads,
                                       cfg.forceScan, cfg.clampShards)
  var anyFailed = false
  var inFlight: seq[InFlight]
  drainLaunch(nShards, cfg.noKill, inFlight, anyFailed):
    var pipes = openShardPipes(pmTool, i, nShards,
                               cfg.outputTemplate, cfg.toolManaged)
    let writerOutFd = pipes.stdinW
    let shardCmd = buildShellCmdForShard(cfg.stages, i, nShards)
    let pid = forkShardChild(pipes, pmTool, shardCmd, i)
    var task = tasks[i]
    task.outFd = writerOutFd
    task.decompress = true
    let writeFv = spawn doWriteShard(task)
    inFlight.add(InFlight(pid: pid, writeFv: writeFv, shardIdx: i))
  reapAll(inFlight, cfg.noKill, anyFailed)
  if anyFailed: quit(1)

proc runPipelineConcat(cfg: RunPipelineCfg) =
  ## +concat+: scatter into N shards, pipe each through the pipeline, capture
  ## stdout via interceptor threads into per-shard temp files, then concat
  ## into cfg.gather.outputPath in genomic order.
  let (tasks, nShards) = resolveShards(cfg.vcfPath, cfg.nShards, cfg.nThreads,
                                       cfg.forceScan, cfg.clampShards)
  createDir(cfg.gather.tmpDir)
  resetInterceptor(gStreamProbe)
  var anyFailed = false
  var inFlight:    seq[InFlight]
  var allTmpPaths: seq[string]
  drainLaunch(nShards, cfg.noKill, inFlight, anyFailed):
    let tmpPath =
      if i == 0: cfg.gather.outputPath
      else:
        let shardBase =
          shardOutputPath(cfg.gather.outputPath, i, nShards).lastPathPart
        cfg.gather.tmpDir / "vcfparty_" & shardBase & ".tmp"
    if i > 0:
      allTmpPaths.add(tmpPath)
    var pipes = openShardPipes(pmConcat, i, nShards, "", false)
    let writerOutFd = pipes.stdinW
    let interceptFd = pipes.stdoutR
    let shardCmd = buildShellCmdForShard(cfg.stages, i, nShards)
    let pid = forkShardChild(pipes, pmConcat, shardCmd, i)
    var task = tasks[i]
    task.outFd = writerOutFd
    task.decompress = true
    let writeFv = spawn doWriteShard(task)
    var cfgCopy = cfg.gather
    let extraFv = spawn runInterceptor(cfgCopy, i, interceptFd, tmpPath)
    inFlight.add(InFlight(pid: pid, writeFv: writeFv, extraFv: extraFv,
                          shardIdx: i, tmpPath: tmpPath))
  reapAll(inFlight, cfg.noKill, anyFailed)
  if anyFailed:
    cleanupTempDir(cfg.gather.tmpDir, allTmpPaths, false)
    quit(1)
  concatenateShards(cfg.gather, allTmpPaths)

proc runPipelineCollect(cfg: RunPipelineCfg) =
  ## +collect+: scatter into N shards, pipe each through the pipeline, stream
  ## complete records to cfg.outputPath (or stdout) in arrival order under
  ## gCollectLock. No temp files, no ordering guarantee.
  let (tasks, nShards) = resolveShards(cfg.vcfPath, cfg.nShards, cfg.nThreads,
                                       cfg.forceScan, cfg.clampShards)
  resetInterceptor(gStreamProbe)
  initLock(gCollectLock)
  let outFd: cint =
    if cfg.toStdout: STDOUT_FILENO
    else:
      let fd = posix.open(cfg.outputPath.cstring,
                          O_WRONLY or O_CREAT or O_TRUNC, 0o666.Mode)
      if fd < 0:
        stderr.writeLine "error: could not create output file: " & cfg.outputPath
        quit(1)
      fd
  var anyFailed = false
  var inFlight: seq[InFlight]
  drainLaunch(nShards, cfg.noKill, inFlight, anyFailed):
    var pipes = openShardPipes(pmCollect, i, nShards, "", false)
    let writerOutFd = pipes.stdinW
    let interceptFd = pipes.stdoutR
    let shardCmd = buildShellCmdForShard(cfg.stages, i, nShards)
    let pid = forkShardChild(pipes, pmCollect, shardCmd, i)
    var task = tasks[i]
    task.outFd = writerOutFd
    task.decompress = true
    let writeFv = spawn doWriteShard(task)
    let extraFv = spawn doCollectInterceptor(i, interceptFd, outFd)
    inFlight.add(InFlight(pid: pid, writeFv: writeFv, extraFv: extraFv,
                          shardIdx: i))
  reapAll(inFlight, cfg.noKill, anyFailed)
  deinitLock(gCollectLock)
  if not cfg.toStdout: discard posix.close(outFd)
  if anyFailed: quit(1)

type MergePlan = object
  ## Precomputed interleaved scatter plan for +merge+.
  headerBytes: seq[byte]
  starts:      seq[int64]
  sizes:       seq[int64]
  voffs:       seq[(int64, int)]
  assignment:  seq[seq[Slice[int]]]
  nShards:     int
  chunkSize:   int
  fmt:         FileFormat

proc computeInterleavedPlan(cfg: RunPipelineCfg): MergePlan =
  ## Interleaved scatter assembly for +merge+. Prefer index virtual offsets
  ## (CSI/TBI) as chunk boundaries; fall back to a full BGZF block scan when
  ## no index exists (VCF only — BCF without CSI is rejected in main.nim).
  result.fmt = if cfg.vcfPath.endsWith(".bcf"): ffBcf else: ffVcf
  let fileSize = getFileSize(cfg.vcfPath)

  var firstDataBlockOff: int64
  var firstUOff: int
  if result.fmt == ffBcf:
    let (hdr, fdbo, uOff) = extractBcfHeaderAndFirstOffset(cfg.vcfPath)
    result.headerBytes = hdr
    firstDataBlockOff = fdbo
    firstUOff = uOff
  else:
    let (hb, fb) = getHeaderAndFirstBlock(cfg.vcfPath)
    result.headerBytes = decompressBgzfBytes(hb)
    firstDataBlockOff = fb
    firstUOff = 0  # VCF data block starts at byte 0

  result.voffs = readIndexVirtualOffsets(cfg.vcfPath)
  result.voffs.keepItIf(it[0] >= firstDataBlockOff)
  let firstVO = (firstDataBlockOff, firstUOff)
  if firstVO notin result.voffs: result.voffs.add(firstVO)
  result.voffs.sort(proc(a, b: (int64, int)): int =
    if a[0] != b[0]: cmp(a[0], b[0]) else: cmp(a[1], b[1]))

  if result.voffs.len > 1:
    # Indexed: chunk boundaries only at index entry block offsets.
    var uniq: seq[int64]
    for v in result.voffs:
      if uniq.len == 0 or uniq[^1] != v[0]:
        uniq.add(v[0])
    result.starts = uniq
  else:
    # No index: scan all BGZF blocks.
    result.starts = scanBgzfBlockStarts(cfg.vcfPath,
                                        startAt = firstDataBlockOff,
                                        endAt = fileSize - 28)
    if scatter.verbose:
      stderr.writeLine &"info: scan: found {result.starts.len} data blocks"

  result.sizes = getLengths(result.starts, fileSize - 28)
  let nDataBlocks = result.starts.len
  var nShards = cfg.nShards
  if nShards > nDataBlocks:
    if cfg.clampShards:
      stderr.writeLine &"info: --clamp-shards: reducing -n from {nShards} to {nDataBlocks} ({nDataBlocks} index entries available in {cfg.vcfPath})"
      nShards = nDataBlocks
    else:
      stderr.writeLine &"error: requested {nShards} shards but only {nDataBlocks} index entries available in {cfg.vcfPath}"
      if result.fmt == ffVcf and not cfg.forceScan:
        stderr.writeLine &"  reduce -n to at most {nDataBlocks}, use --force-scan to scan all BGZF blocks, or pass --clamp-shards to reduce -n automatically"
      else:
        stderr.writeLine &"  reduce -n to at most {nDataBlocks} or pass --clamp-shards to reduce -n automatically"
      quit(1)
  result.nShards   = nShards
  result.chunkSize = max(1, (nDataBlocks + nShards * 10 - 1) div (nShards * 10))
  result.assignment = interleavedBlockAssignment(nDataBlocks, nShards,
                                                 result.chunkSize)

proc runPipelineMerge(cfg: RunPipelineCfg) =
  ## +merge+: interleaved scatter, k-way merge to cfg.outputPath (or stdout)
  ## in genomic order. Uses a two-phase launch — all feeders must be running
  ## before any writer starts to avoid bidirectional pipe deadlock.
  # Need nShards writers + nShards feeders running concurrently to avoid pipe
  # deadlock (writer blocks on stdin if feeder isn't draining stdout).
  setMaxPoolSize(cfg.nShards * 2)
  var plan = computeInterleavedPlan(cfg)
  let nShards = plan.nShards
  resetMerge(gStreamProbe, plan.fmt)

  # Allocate per-worker inboxes for partial-record handoff.
  var inboxes = newInboxArray(nShards)

  var inFlight:     seq[InFlight]
  var relayReadFds: seq[cint]
  var writerTasks:  seq[InterleavedTask]

  # Phase 1: create all pipes, fork all children, spawn all feeders.
  # Feeders must be running before writers start to prevent pipe deadlock:
  # writer fills subprocess stdin → subprocess fills stdout → feeder drains.
  for i in 0 ..< nShards:
    var pipes = openShardPipes(pmMerge, i, nShards, "", false)
    let writerOutFd = pipes.stdinW
    let feederSrcFd = pipes.stdoutR
    let relayReadFd = pipes.relayR
    let relayWriteFd = pipes.relayW
    let shardCmd = buildShellCmdForShard(cfg.stages, i, nShards)
    let pid = forkShardChild(pipes, pmMerge, shardCmd, i)
    # Spawn feeder immediately — it blocks on read until subprocess produces output.
    let extraFv = spawn doMergeFeeder(i, feederSrcFd, relayWriteFd)
    relayReadFds.add(relayReadFd)
    writerTasks.add(InterleavedTask(
      vcfPath: cfg.vcfPath, outFd: writerOutFd,
      headerBytes: plan.headerBytes,
      blockStarts: addr plan.starts, blockSizes: addr plan.sizes,
      chunkIndices: plan.assignment[i], format: plan.fmt,
      csiVoffs: if plan.voffs.len > 1: addr plan.voffs else: nil,
      shardIdx: i, nShards: nShards, chunkSize: plan.chunkSize,
      inboxes: addr inboxes))
    inFlight.add(InFlight(pid: pid, writeFv: nil, extraFv: extraFv, shardIdx: i))

  # Phase 2: spawn all writers now that feeders are draining subprocess stdout.
  for i in 0 ..< nShards:
    inFlight[i].writeFv = spawn writeInterleavedShard(writerTasks[i])

  # Wait for shard 0 feeder to detect format and capture header.
  while not gStreamProbe.header.ready: sleep(1)

  # Build contig table from the subprocess's output header (VCF and BCF).
  let hdrSlice = @(gStreamProbe.header.buf[0 ..< gStreamProbe.header.len])
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

  # Write the subprocess header to outFd (matches pipeline output format exactly).
  var hw = 0
  while hw < gStreamProbe.header.len.int:
    let n = posix.write(outFd, cast[pointer](addr gStreamProbe.header.buf[hw]),
                        gStreamProbe.header.len.int - hw)
    if n <= 0: break
    hw += n

  # k-way merge: reads from all N relay pipes concurrently, emits sorted records.
  kWayMerge(relayReadFds, outFd, gStreamProbe.format, contigTable)

  for fd in relayReadFds:
    discard posix.close(fd)
  if not cfg.toStdout: discard posix.close(outFd)

  # Reap all subprocesses. kWayMerge has drained the relay pipes so children
  # should naturally be at EOF; kill-on-first-failure is a safety net for a
  # subprocess that hangs on exit.
  var anyFailed = false
  while inFlight.len > 0:
    waitOne(inFlight, anyFailed)
    if anyFailed and not cfg.noKill:
      killAll(inFlight)
      break
  while inFlight.len > 0:
    waitOne(inFlight, anyFailed)
  freeInboxArray(inboxes)
  if anyFailed: quit(1)

proc runPipeline*(cfg: RunPipelineCfg) =
  ## Single public entry point for the `run` subcommand. Dispatches on
  ## cfg.mode to one of four per-mode helpers that share the pipe / fork /
  ## drain-launch / reap scaffolding.
  case cfg.mode
  of pmTool:    runPipelineTool(cfg)
  of pmConcat:  runPipelineConcat(cfg)
  of pmCollect: runPipelineCollect(cfg)
  of pmMerge:   runPipelineMerge(cfg)
