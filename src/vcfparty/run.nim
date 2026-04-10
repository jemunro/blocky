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

proc runShards*(vcfPath: string; nShards: int; outputTemplate: string;
                nThreads: int; forceScan: bool;
                stages: seq[seq[string]]; noKill: bool = false;
                toolManaged: bool = false; clampShards: bool = false) =
  ## Scatter vcfPath into nShards shards and pipe each through the tool pipeline.
  ## outputTemplate may contain {} or use shard_NN. prefix naming.
  ## All N shards run concurrently. On failure, siblings are killed (SIGTERM)
  ## unless noKill is true. If any shard pipeline exits non-zero, prints to
  ## stderr and exits 1 at end.
  ## When toolManaged is true, shard stdout is discarded (/dev/null); the tool
  ## is expected to write its own output files using {} in the command.
  let actualThreads = if nThreads == 0: countProcessors() else: nThreads
  setMaxPoolSize(nShards * 2)
  let fmt      = if vcfPath.endsWith(".bcf"): ffBcf else: ffVcf
  let tasks    = computeShards(vcfPath, nShards, actualThreads, forceScan, fmt,
                               clampShards)
  let nShards  = tasks.len  # may be < requested nShards if clamped
  var anyFailed = false
  var inFlight: seq[InFlight]
  for i in 0 ..< nShards:
    # Stop launching new shards if a failure has been detected and !noKill.
    if anyFailed and not noKill: break
    # Drain one finished shard if all N shards are already in flight.
    while inFlight.len >= nShards:
      waitOne(inFlight, anyFailed)
      if anyFailed and not noKill: break
    if anyFailed and not noKill: break
    # Create pipe: [0] = read-end (child stdin), [1] = write-end (shard writer).
    var pipeFds: array[2, cint]
    if posix.pipe(pipeFds) != 0:
      stderr.writeLine &"error: pipe() failed for shard {i + 1}"
      quit(1)
    discard posix.fcntl(pipeFds[1], F_SETFD, FD_CLOEXEC)
    var outFileFd: cint
    if toolManaged:
      outFileFd = posix.open("/dev/null".cstring, O_WRONLY)
      if outFileFd < 0:
        stderr.writeLine &"error: could not open /dev/null for shard {i + 1}"
        quit(1)
    else:
      let outPath = shardOutputPath(outputTemplate, i, nShards)
      createDir(outPath.parentDir)
      outFileFd = posix.open(outPath.cstring,
                             O_WRONLY or O_CREAT or O_TRUNC,
                             0o666.Mode)
      if outFileFd < 0:
        stderr.writeLine &"error: could not create output file: {outPath}"
        quit(1)
    let shardCmd = buildShellCmdForShard(stages, i, nShards)
    let pid = forkExecSh(pipeFds[0], pipeFds[1], outFileFd, shardCmd, i)
    # Parent closes the fds the child owns.
    discard posix.close(pipeFds[0])
    discard posix.close(outFileFd)
    # Assign pipe write-end to the shard task and spawn the writer thread.
    var task = tasks[i]
    task.outFd = pipeFds[1]
    task.decompress = true
    let writeFv = spawn doWriteShard(task)
    inFlight.add(InFlight(pid: pid, writeFv: writeFv, shardIdx: i))
  # On failure without --no-kill: terminate all remaining children, then reap.
  if anyFailed and not noKill:
    killAll(inFlight)
  while inFlight.len > 0:
    waitOne(inFlight, anyFailed)
  if anyFailed:
    quit(1)

# ---------------------------------------------------------------------------
# G6 — gather path: interceptor threads + concatenation
# ---------------------------------------------------------------------------

proc runShardsGather*(vcfPath: string; nShards: int; outputTemplate: string;
                      nThreads: int; forceScan: bool;
                      stages: seq[seq[string]]; noKill: bool; cfg: GatherConfig;
                      clampShards: bool = false) =
  ## Scatter vcfPath into nShards shards, pipe each through the tool pipeline,
  ## capture stdout via interceptor threads, then concatenate into cfg.outputPath.
  ## {} in stage tokens is substituted with the zero-padded shard number.
  ## All N shards run concurrently.
  let actualThreads = if nThreads == 0: countProcessors() else: nThreads
  # Pool must accommodate scatter writer threads and interceptor threads simultaneously.
  setMaxPoolSize(nShards * 2)
  let fmt   = if vcfPath.endsWith(".bcf"): ffBcf else: ffVcf
  let tasks = computeShards(vcfPath, nShards, actualThreads, forceScan, fmt,
                            clampShards)
  let nShards = tasks.len  # may be < requested nShards if clamped
  createDir(cfg.tmpDir)
  var anyFailed = false
  var inFlight:    seq[InFlight]
  var allTmpPaths: seq[string]
  for i in 0 ..< nShards:
    if anyFailed and not noKill: break
    while inFlight.len >= nShards:
      waitOne(inFlight, anyFailed)
      if anyFailed and not noKill: break
    if anyFailed and not noKill: break
    let tmpPath =
      if i == 0: cfg.outputPath
      else:
        let shardBase = shardOutputPath(cfg.outputPath, i, nShards).lastPathPart
        cfg.tmpDir / "vcfparty_" & shardBase & ".tmp"
    if i > 0:
      allTmpPaths.add(tmpPath)
    # stdin pipe:  shard writer → shell stdin
    # stdout pipe: shell stdout → interceptor
    var stdinPipe:  array[2, cint]
    var stdoutPipe: array[2, cint]
    if posix.pipe(stdinPipe) != 0 or posix.pipe(stdoutPipe) != 0:
      stderr.writeLine &"error: pipe() failed for shard {i + 1}"
      quit(1)
    # Prevent write-end and read-end from being inherited by future fork children.
    discard posix.fcntl(stdinPipe[1],  F_SETFD, FD_CLOEXEC)
    discard posix.fcntl(stdoutPipe[0], F_SETFD, FD_CLOEXEC)
    let shardCmd = buildShellCmdForShard(stages, i, nShards)
    let pid = forkExecSh(stdinPipe[0], stdinPipe[1], stdoutPipe[1], shardCmd, i)
    # Parent closes fds that now belong to the child.
    discard posix.close(stdinPipe[0])
    discard posix.close(stdoutPipe[1])
    # Spawn shard writer (writes shard bytes to stdinPipe[1]).
    var task = tasks[i]
    task.outFd = stdinPipe[1]
    task.decompress = true
    let writeFv = spawn doWriteShard(task)
    # Spawn interceptor (reads shell stdout from stdoutPipe[0], writes to tmpPath).
    let interceptFd = stdoutPipe[0]
    var cfgCopy = cfg
    let extraFv = spawn runInterceptor(cfgCopy, i, interceptFd, tmpPath)
    inFlight.add(InFlight(pid: pid, writeFv: writeFv, extraFv: extraFv,
                          shardIdx: i, tmpPath: tmpPath))
  if anyFailed and not noKill:
    killAll(inFlight)
  while inFlight.len > 0:
    waitOne(inFlight, anyFailed)
  if anyFailed:
    cleanupTempDir(cfg.tmpDir, allTmpPaths, false)
    quit(1)
  concatenateShards(cfg, allTmpPaths)

# ---------------------------------------------------------------------------
# I4 — +collect+ streaming output (arrival-order gather, no temp files)
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
  var raw     = newSeq[byte](ReadSize)
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
      gChromLine.ready = true   # unblock shards 1..N even on empty shard 0
      return 0
    let (detFmt, detBgzf) = sniffStreamFormat(raw[0 ..< n])
    fmt    = detFmt
    isBgzf = detBgzf
    appendReadToAccum(raw, n.int, isBgzf, rawAccum, bgzfPos, pending)
  else:
    while not gChromLine.ready: sleep(1)
    fmt    = gDetectedFormat
    isBgzf = gStreamIsBgzf

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
    gDetectedFormat = fmt
    gStreamIsBgzf   = isBgzf
    gChromLine.len   = 0
    gChromLine.ready = true

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

proc runShardsCollect*(vcfPath: string; nShards: int; outputPath: string;
                       nThreads: int; forceScan: bool;
                       stages: seq[seq[string]]; noKill: bool; toStdout: bool;
                       clampShards: bool = false) =
  ## +collect+: scatter into N shards, pipe each through the pipeline,
  ## stream complete records to outputPath (or stdout) in arrival order.
  ## No temp files. No ordering guarantee.
  let actualThreads = if nThreads == 0: countProcessors() else: nThreads
  # Pool must accommodate scatter writer threads and interceptor threads.
  setMaxPoolSize(nShards * 2)
  let fmt   = if vcfPath.endsWith(".bcf"): ffBcf else: ffVcf
  let tasks = computeShards(vcfPath, nShards, actualThreads, forceScan, fmt,
                            clampShards)
  let nShards = tasks.len  # may be < requested nShards if clamped

  # Reset format-detection globals (shared with gather).
  gChromLine.ready = false
  gDetectedFormat = ffText
  gStreamIsBgzf   = false
  gChromLine.len   = 0

  initLock(gCollectLock)

  # Open output fd (or use stdout).
  let outFd: cint =
    if toStdout: STDOUT_FILENO
    else:
      let fd = posix.open(outputPath.cstring,
                          O_WRONLY or O_CREAT or O_TRUNC, 0o666.Mode)
      if fd < 0:
        stderr.writeLine "error: could not create output file: " & outputPath
        quit(1)
      fd

  var anyFailed = false
  var inFlight: seq[InFlight]
  for i in 0 ..< nShards:
    if anyFailed and not noKill: break
    while inFlight.len >= nShards:
      waitOne(inFlight, anyFailed)
      if anyFailed and not noKill: break
    if anyFailed and not noKill: break
    # stdin pipe: shard writer → shell stdin
    # stdout pipe: shell stdout → collect interceptor
    var stdinPipe:  array[2, cint]
    var stdoutPipe: array[2, cint]
    if posix.pipe(stdinPipe) != 0 or posix.pipe(stdoutPipe) != 0:
      stderr.writeLine &"error: pipe() failed for shard {i + 1}"
      quit(1)
    # Prevent write-end and read-end from being inherited by future fork children.
    discard posix.fcntl(stdinPipe[1],  F_SETFD, FD_CLOEXEC)
    discard posix.fcntl(stdoutPipe[0], F_SETFD, FD_CLOEXEC)
    let shardCmd = buildShellCmdForShard(stages, i, nShards)
    let pid = forkExecSh(stdinPipe[0], stdinPipe[1], stdoutPipe[1], shardCmd, i)
    discard posix.close(stdinPipe[0])
    discard posix.close(stdoutPipe[1])
    var task = tasks[i]
    task.outFd = stdinPipe[1]
    task.decompress = true
    let writeFv = spawn doWriteShard(task)
    let extraFv = spawn doCollectInterceptor(i, stdoutPipe[0], outFd)
    inFlight.add(InFlight(pid: pid, writeFv: writeFv, extraFv: extraFv, shardIdx: i))

  if anyFailed and not noKill:
    killAll(inFlight)
  while inFlight.len > 0:
    waitOne(inFlight, anyFailed)

  deinitLock(gCollectLock)
  if not toStdout: discard posix.close(outFd)
  if anyFailed: quit(1)

# ---------------------------------------------------------------------------
# M5 — +merge+ k-way merge output (sequential scatter)
# ---------------------------------------------------------------------------

proc doMergeFeeder(shardIdx: int; srcFd: cint; relayWriteFd: cint): int {.gcsafe.} =
  ## Read from srcFd (subprocess stdout), strip VCF/BCF header, relay
  ## post-header bytes (decompressed if BGZF) to relayWriteFd.
  ## Shard 0: sets gMergeFormat and gMergeHeader.ready when header is found.
  ## Closes relayWriteFd and srcFd before returning.
  const ReadSize = 65536
  var raw      = newSeq[byte](ReadSize)
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
      gMergeFormat      = ffVcf
      gMergeHeader.ready = true
    discard posix.close(srcFd)
    return 0

  let (detFmt, detBgzf) = sniffStreamFormat(raw[0 ..< n0])
  fmt    = detFmt
  isBgzf = detBgzf
  if isBgzf and not gMergeBgzfWarned:
    gMergeBgzfWarned = true
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
    let sz = min(hEnd, gMergeHeader.buf.len)
    if sz > 0:
      copyMem(addr gMergeHeader.buf[0], unsafeAddr pending[0], sz)
    gMergeHeader.len   = sz.int32
    gMergeFormat      = fmt
    gMergeHeader.ready = true

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
      rawAccum.add(raw[0 ..< n])
      flushBgzfAccum(rawAccum, bgzfPos, pending)
      if pending.len > 0:
        relayBytes(pending.toOpenArray(0, pending.high))
        pending = @[]
    else:
      relayBytes(raw.toOpenArray(0, n - 1))

  discard posix.close(relayWriteFd)
  discard posix.close(srcFd)
  result = 0

proc runShardsMerge*(vcfPath: string; nShards: int; outputPath: string;
                     nThreads: int; forceScan: bool;
                     stages: seq[seq[string]]; noKill: bool; toStdout: bool;
                     clampShards: bool = false) =
  ## +merge+: scatter vcfPath into nShards shards, pipe each through the pipeline,
  ## strip headers, k-way merge records to outputPath (or stdout) in genomic order.
  ## Uses interleaved scatter: blocks assigned round-robin so all shards receive
  ## data concurrently, enabling stall-free merge.
  ## No temp files. Output is uncompressed VCF or BCF.
  let actualThreads = if nThreads == 0: countProcessors() else: nThreads
  # Need nShards writers + nShards feeders running concurrently to avoid
  # pipe deadlock (writer blocks on stdin pipe if feeder isn't draining stdout).
  setMaxPoolSize(nShards * 2)
  let fmt   = if vcfPath.endsWith(".bcf"): ffBcf else: ffVcf
  let fileSize = getFileSize(vcfPath)

  # --- Compute interleaved block assignment ---
  # For both BCF and VCF, prefer index virtual offsets (CSI/TBI) as chunk
  # boundaries: each entry has a known (block_off, u_off) so head splits are
  # exact. Fall back to scanning all BGZF blocks only when no index exists.
  var headerBytes: seq[byte]
  var starts: seq[int64]
  var voffs: seq[(int64, int)]
  var firstDataBlockOff: int64
  var firstUOff: int

  if fmt == ffBcf:
    let (hdr, fdbo, uOff) = extractBcfHeaderAndFirstOffset(vcfPath)
    headerBytes = hdr
    firstDataBlockOff = fdbo
    firstUOff = uOff
  else:
    let (hb, fb) = getHeaderAndFirstBlock(vcfPath)
    headerBytes = decompressBgzfBytes(hb)
    firstDataBlockOff = fb
    firstUOff = 0  # VCF data block starts at byte 0

  voffs = readIndexVirtualOffsets(vcfPath)
  # Drop any voffs that point before the first data block (header region).
  voffs.keepItIf(it[0] >= firstDataBlockOff)
  # Ensure the first data block is in the voffs list.
  let firstVO = (firstDataBlockOff, firstUOff)
  if firstVO notin voffs: voffs.add(firstVO)
  voffs.sort(proc(a, b: (int64, int)): int =
    if a[0] != b[0]: cmp(a[0], b[0]) else: cmp(a[1], b[1]))

  if voffs.len > 1:
    # Indexed mode: chunk boundaries only at index entry block offsets.
    # Each starts[i] may span multiple BGZF blocks (up to starts[i+1]).
    var uniq: seq[int64]
    for v in voffs:
      if uniq.len == 0 or uniq[^1] != v[0]:
        uniq.add(v[0])
    starts = uniq
  else:
    # No index: scan all BGZF blocks. VCF only — BCF without CSI is rejected
    # earlier in main.nim.
    starts = scanBgzfBlockStarts(vcfPath, startAt = firstDataBlockOff,
                                 endAt = fileSize - 28)
    if scatter.verbose:
      stderr.writeLine &"info: scan: found {starts.len} data blocks"

  # Compute sizes; exclude the 28-byte BGZF EOF block from the last entry.
  var sizes = getLengths(starts, fileSize - 28)
  let nDataBlocks = starts.len
  # If nShards > nDataBlocks each chunk would have less than one entry: clamp
  # down (clampShards) or reject.
  var nShards = nShards  # shadow parameter; may be reduced if clamping
  if nShards > nDataBlocks:
    if clampShards:
      stderr.writeLine &"info: --clamp-shards: reducing -n from {nShards} to {nDataBlocks} ({nDataBlocks} index entries available in {vcfPath})"
      nShards = nDataBlocks
    else:
      stderr.writeLine &"error: requested {nShards} shards but only {nDataBlocks} index entries available in {vcfPath}"
      if fmt == ffVcf and not forceScan:
        stderr.writeLine &"  reduce -n to at most {nDataBlocks}, use --force-scan to scan all BGZF blocks, or pass --clamp-shards to reduce -n automatically"
      else:
        stderr.writeLine &"  reduce -n to at most {nDataBlocks} or pass --clamp-shards to reduce -n automatically"
      quit(1)
  let chunkSize = max(1, (nDataBlocks + nShards * 10 - 1) div (nShards * 10))
  let assignment = interleavedBlockAssignment(nDataBlocks, nShards, chunkSize)

  # Reset merge globals.
  gMergeHeader.ready = false
  gMergeHeader.len   = 0
  gMergeFormat      = fmt
  gMergeBgzfWarned  = false

  # Allocate per-worker inboxes for partial-record handoff.
  var inboxes = newInboxArray(nShards)

  var inFlight:    seq[InFlight]
  var relayReadFds: seq[cint]
  var writerTasks: seq[InterleavedTask]
  var stdinWriteFds: seq[cint]

  # Phase 1: create all pipes, fork all children, spawn all feeders.
  # Feeders must be running before writers start to prevent pipe deadlock:
  # writer fills subprocess stdin → subprocess fills stdout → feeder drains.
  # If feeders aren't draining stdout, the whole pipeline backs up.
  for i in 0 ..< nShards:
    var stdinPipe:  array[2, cint]
    var stdoutPipe: array[2, cint]
    var relayPipe:  array[2, cint]
    if posix.pipe(stdinPipe) != 0 or posix.pipe(stdoutPipe) != 0 or
       posix.pipe(relayPipe) != 0:
      stderr.writeLine &"error: pipe() failed for shard {i + 1}"
      quit(1)
    # Enlarge pipe buffers to avoid bidirectional pipe deadlock: writer blocks on
    # stdinPipe (full) while subprocess blocks on stdoutPipe (full) while feeder
    # blocks on relayPipe (full). 1MB per pipe accommodates typical per-chunk data.
    when defined(linux):
      const F_SETPIPE_SZ = cint(1031)
      const PipeBufSize = cint(1048576)  # 1MB
      discard posix.fcntl(stdinPipe[0],  F_SETPIPE_SZ, PipeBufSize)
      discard posix.fcntl(stdoutPipe[0], F_SETPIPE_SZ, PipeBufSize)
      discard posix.fcntl(relayPipe[0],  F_SETPIPE_SZ, PipeBufSize)
    discard posix.fcntl(stdinPipe[1],  F_SETFD, FD_CLOEXEC)
    discard posix.fcntl(relayPipe[0],  F_SETFD, FD_CLOEXEC)
    discard posix.fcntl(relayPipe[1],  F_SETFD, FD_CLOEXEC)
    discard posix.fcntl(stdoutPipe[0], F_SETFD, FD_CLOEXEC)
    let shardCmd = buildShellCmdForShard(stages, i, nShards)
    let pid = forkExecSh(stdinPipe[0], stdinPipe[1], stdoutPipe[1], shardCmd, i)
    discard posix.close(stdinPipe[0])
    discard posix.close(stdoutPipe[1])
    # Spawn feeder immediately — it blocks on read until subprocess produces output.
    let extraFv = spawn doMergeFeeder(i, stdoutPipe[0], relayPipe[1])
    relayReadFds.add(relayPipe[0])
    stdinWriteFds.add(stdinPipe[1])
    writerTasks.add(InterleavedTask(
      vcfPath: vcfPath, outFd: stdinPipe[1],
      headerBytes: headerBytes,
      blockStarts: addr starts, blockSizes: addr sizes,
      chunkIndices: assignment[i], format: fmt,
      csiVoffs: if voffs.len > 1: addr voffs else: nil,
      shardIdx: i, nShards: nShards, chunkSize: chunkSize,
      inboxes: addr inboxes))
    inFlight.add(InFlight(pid: pid, writeFv: nil, extraFv: extraFv, shardIdx: i))

  # Phase 2: spawn all writers now that feeders are ready to drain stdout.
  # Brief yield to let feeder threads start their blocking read() calls.
  for i in 0 ..< nShards:
    inFlight[i].writeFv = spawn writeInterleavedShard(writerTasks[i])

  # Wait for shard 0 feeder to detect format and capture header.
  while not gMergeHeader.ready: sleep(1)

  # Build contig table from subprocess's output header (works for VCF and BCF pipelines).
  let hdrSlice = @(gMergeHeader.buf[0 ..< gMergeHeader.len])
  let contigTable = extractContigTable(hdrSlice)

  # Open output fd.
  let outFd: cint =
    if toStdout: STDOUT_FILENO
    else:
      let fd = posix.open(outputPath.cstring,
                          O_WRONLY or O_CREAT or O_TRUNC, 0o666.Mode)
      if fd < 0:
        stderr.writeLine "error: could not create output file: " & outputPath
        quit(1)
      fd

  # Write the subprocess header to outFd (matches pipeline output format exactly).
  var hw = 0
  while hw < gMergeHeader.len.int:
    let n = posix.write(outFd, cast[pointer](addr gMergeHeader.buf[hw]),
                        gMergeHeader.len.int - hw)
    if n <= 0: break
    hw += n

  # k-way merge: reads from all N relay pipes concurrently, emits sorted records.
  kWayMerge(relayReadFds, outFd, gMergeFormat, contigTable)

  for fd in relayReadFds:
    discard posix.close(fd)
  if not toStdout: discard posix.close(outFd)

  # Reap all subprocesses (all have exited since relay pipes are at EOF).
  # kWayMerge already drained relay pipes so subprocesses are naturally finished;
  # kill-all is a safety net for the rare case a subprocess hangs on exit.
  var anyFailed = false
  while inFlight.len > 0:
    waitOne(inFlight, anyFailed)
    if anyFailed and not noKill:
      killAll(inFlight)
      break
  while inFlight.len > 0:
    waitOne(inFlight, anyFailed)
  freeInboxArray(inboxes)
  if anyFailed: quit(1)
