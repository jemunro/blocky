## run — scatter a bgzipped file into N shards and pipe each through a tool pipeline.
##
## This module is responsible for:
##   1. Parsing the "---"-separated argv into blocky args + pipeline stages.
##   2. Building the sh -c command string for each shard.
##   3. Mode inference from -o / {} flags.
##   4. Executing per-shard pipelines via a bounded worker pool.

import std/[atomics, cpuinfo, os, posix, sequtils, strformat, strutils, tempfiles]
import scatter
import bgzf
import gather

# ---------------------------------------------------------------------------
# Argv parsing
# ---------------------------------------------------------------------------

proc isSep(tok: string): bool {.inline.} =
  ## Return true for "---" or ":::" — both are valid pipeline stage separators.
  tok == "---" or tok == ":::"

proc parseRunArgv*(argv: seq[string]): (seq[string], seq[seq[string]]) =
  ## Split argv at "---" / ":::" separators.
  ## Returns (blockyArgs, stages).
  ## Exits 1 with a message if no separator is present or any stage is empty.
  var firstSep = -1
  for i, tok in argv:
    if isSep(tok):
      firstSep = i
      break
  if firstSep < 0:
    stderr.writeLine "blocky run: at least one --- stage is required"
    quit(1)
  let blockyArgs = argv[0 ..< firstSep]

  var stages: seq[seq[string]]
  var cur: seq[string]
  for i in firstSep + 1 ..< argv.len:
    if isSep(argv[i]):
      if cur.len == 0:
        stderr.writeLine "blocky run: empty pipeline stage"
        quit(1)
      stages.add(cur)
      cur = @[]
    else:
      cur.add(argv[i])
  if cur.len == 0:
    stderr.writeLine "blocky run: empty pipeline stage"
    quit(1)
  stages.add(cur)
  result = (blockyArgs, stages)

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
  rmFile,        ## concat thread writes to -o output file
  rmStdout,      ## concat thread writes to stdout (no -o)
  rmDiscard      ## --discard: stdout → /dev/null, tool manages own output

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

proc inferRunMode*(hasOutput: bool; hasDiscard: bool): RunMode =
  ## Infer run mode from -o and --discard flags.
  ## --discard: stdout discarded (tool manages own output).
  ## -o present: file mode (concat thread writes to -o).
  ## Neither: stdout mode (concat thread writes to stdout).
  if hasDiscard:
    return rmDiscard
  if hasOutput:
    return rmFile
  return rmStdout

# ---------------------------------------------------------------------------
# {} substitution
# ---------------------------------------------------------------------------

proc substituteToken*(tok: string; shardNum: string): string =
  ## Replace each unescaped {} in tok with shardNum.
  ## Replace \{} with a literal {} (backslash consumed by blocky).
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
# Shard queue (atomic pull counter) and deposit queue (ordered tmp file handoff)
# ---------------------------------------------------------------------------

var gNextShard* {.global.}: Atomic[int]
  ## Atomic pull counter for the shard work queue. Workers call
  ## fetchAdd(1) to claim the next shard index. When the returned
  ## value >= nTotalShards, the worker has no more work and exits.

type DepositSlot* = object
  ## One slot in the deposit queue. Workers write the tmp file path
  ## then set ready=true (release). The concat thread spin-waits on
  ## ready (acquire) before reading the path.
  path*:  array[4096, char]
  len*:   int32
  ready*: Atomic[bool]

type DepositQueue* = object
  ## Fixed-size array of deposit slots, one per shard. Heap-allocated
  ## with allocShared0 for cross-thread access.
  slots*: ptr UncheckedArray[DepositSlot]
  count*: int

proc newDepositQueue*(n: int): DepositQueue =
  ## Allocate n deposit slots, all zeroed (ready = false).
  let p = cast[ptr UncheckedArray[DepositSlot]](allocShared0(n * sizeof(DepositSlot)))
  DepositQueue(slots: p, count: n)

proc freeDepositQueue*(q: DepositQueue) =
  if q.slots != nil:
    deallocShared(q.slots)

proc deposit*(q: DepositQueue; idx: int; path: string) =
  ## Deposit a tmp file path at slot idx. Sets ready=true as a release
  ## signal to the concat thread. The slot must not already be ready.
  let slot = addr q.slots[idx]
  let n = min(path.len, slot.path.len)
  if n > 0:
    copyMem(addr slot.path[0], unsafeAddr path[0], n)
  slot.len = n.int32
  slot.ready.store(true, moRelease)

proc waitFor*(q: DepositQueue; idx: int): string =
  ## Block until slot idx is ready, then return the path and reset
  ## the slot. Used by the concat thread to consume deposits in order.
  let slot = addr q.slots[idx]
  while not slot.ready.load(moAcquire):
    sleep(1)
  result = newString(slot.len)
  if slot.len > 0:
    copyMem(addr result[0], addr slot.path[0], slot.len)
  slot.len = 0
  slot.ready.store(false, moRelease)

# ---------------------------------------------------------------------------
# Subprocess fork/exec
# ---------------------------------------------------------------------------

proc forkExecSh(pipeReadFd: cint; pipeWriteFd: cint; stdoutFd: cint;
                shellCmd: string; shardIdx: int;
                stderrFd: cint = -1): Pid =
  ## Fork a child that runs sh -c shellCmd with stdin = pipeReadFd and
  ## stdout = stdoutFd.  If stderrFd >= 0, redirect stderr to it;
  ## otherwise stderr is inherited.  Returns child PID.
  ## All fds should have FD_CLOEXEC set by the caller so that
  ## concurrent workers' children do not inherit stale pipe ends.
  let pid = posix.fork()
  if pid < 0:
    stderr.writeLine &"error: fork() failed for shard {shardIdx + 1}"
    quit(1)
  if pid == 0:
    # Child: rewire stdin, stdout, and optionally stderr.
    if posix.dup2(pipeReadFd, STDIN_FILENO) < 0 or
       posix.dup2(stdoutFd,   STDOUT_FILENO) < 0:
      exitnow(1)
    if stderrFd >= 0:
      if posix.dup2(stderrFd, STDERR_FILENO) < 0:
        exitnow(1)
      discard posix.close(stderrFd)
    discard posix.close(pipeReadFd)
    discard posix.close(pipeWriteFd)
    discard posix.close(stdoutFd)
    let args = allocCStringArray(["sh", "-c", shellCmd])
    discard posix.execvp("sh", args)
    deallocCStringArray(args)
    exitnow(127)
  result = pid

# ---------------------------------------------------------------------------
# Worker proc — pulls shards, forks subprocesses, collects output
# ---------------------------------------------------------------------------

var gAnyFailed* {.global.}: Atomic[bool]
  ## Set by any worker that observes a non-zero child exit code.
  ## Checked by other workers to abort early (unless --no-kill).

var gShardsWithOutput* {.global.}: Atomic[int]
  ## Incremented by interceptors that receive at least one byte of stdout.
  ## Checked after run completes when {} is present without --discard.

type InterceptArgs = object
  shardIdx: int
  inputFd: cint
  tmpPath: string
  chromBufPtr: ptr SharedBuf
  directFd: cint
  directUncompress: bool
  compressLevel: int
  outputCounter: ptr Atomic[int]
  rc: ptr Atomic[int]

proc interceptThread(args: InterceptArgs) {.thread.} =
  let rc = interceptShard(args.shardIdx, args.inputFd, args.tmpPath,
                          args.chromBufPtr, args.directFd,
                          args.directUncompress, args.compressLevel,
                          args.outputCounter)
  args.rc[].store(rc, moRelease)

type WorkerArgs* = object
  tasksPtr*: ptr seq[ShardTask]
  nTotalShards*: int
  stagesPtr*: ptr seq[seq[string]]
  tmpDirPtr*: ptr string
  depositsPtr*: ptr DepositQueue
  chromBufPtr*: ptr SharedBuf
  discardStdout*: bool
  discardStderr*: bool
  noKill*: bool
  outFd*: cint
  forceUncompress*: bool
  outputCounterPtr*: ptr Atomic[int]

proc workerThread*(args: WorkerArgs) {.thread.} =
  ## Worker loop: atomically pull shard indices from gNextShard, fork a
  ## subprocess for each, write decompressed shard data to its stdin,
  ## intercept stdout (strip headers, BGZF-compress) to a tmp file,
  ## and deposit the path to the concat queue.
  ## Shard 0 writes directly to outFd when outFd >= 0.
  while true:
    if gAnyFailed.load(moRelaxed) and not args.noKill:
      break
    let idx = gNextShard.fetchAdd(1, moRelaxed)
    if idx >= args.nTotalShards:
      break

    var tmpPath: string
    if not args.discardStdout:
      tmpPath = args.tmpDirPtr[] / &"shard_{idx}.tmp"

    # Subprocess stdout goes to a pipe (interceptor reads the other end)
    # or /dev/null for tool-managed mode.
    var stdoutFd: cint
    var stdoutPipeR: cint = -1
    if args.discardStdout:
      stdoutFd = posix.open("/dev/null".cstring, O_WRONLY)
      if stdoutFd < 0:
        stderr.writeLine &"error: could not open /dev/null for shard {idx + 1}"
        gAnyFailed.store(true, moRelease)
        continue
      discard posix.fcntl(stdoutFd, F_SETFD, FD_CLOEXEC)
    else:
      var stdoutPipe: array[2, cint]
      if posix.pipe(stdoutPipe) != 0:
        stderr.writeLine &"error: pipe() failed for shard {idx + 1}"
        gAnyFailed.store(true, moRelease)
        continue
      discard posix.fcntl(stdoutPipe[0], F_SETFD, FD_CLOEXEC)
      discard posix.fcntl(stdoutPipe[1], F_SETFD, FD_CLOEXEC)
      stdoutPipeR = stdoutPipe[0]  # interceptor reads this
      stdoutFd = stdoutPipe[1]     # child writes to this

    # Create stdin pipe.
    var stdinPipe: array[2, cint]
    if posix.pipe(stdinPipe) != 0:
      stderr.writeLine &"error: pipe() failed for shard {idx + 1}"
      discard posix.close(stdoutFd)
      if stdoutPipeR >= 0: discard posix.close(stdoutPipeR)
      gAnyFailed.store(true, moRelease)
      continue
    discard posix.fcntl(stdinPipe[0], F_SETFD, FD_CLOEXEC)
    discard posix.fcntl(stdinPipe[1], F_SETFD, FD_CLOEXEC)

    # Fork subprocess.
    var stderrFd: cint = -1
    if args.discardStderr:
      stderrFd = posix.open("/dev/null".cstring, O_WRONLY)
      if stderrFd >= 0:
        discard posix.fcntl(stderrFd, F_SETFD, FD_CLOEXEC)
    let shardCmd = buildShellCmdForShard(args.stagesPtr[], idx, args.nTotalShards)
    info(&"shard {idx + 1}/{args.nTotalShards}: {shardCmd}")
    let pid = forkExecSh(stdinPipe[0], stdinPipe[1], stdoutFd, shardCmd, idx,
                          stderrFd)
    discard posix.close(stdinPipe[0])
    discard posix.close(stdoutFd)
    if stderrFd >= 0: discard posix.close(stderrFd)

    # Create interceptor thread (reads subprocess stdout, writes tmp/output).
    # Using createThread instead of threadpool avoids pool-capacity starvation
    # and fork()+threadpool deadlocks.
    var iThread: Thread[InterceptArgs]
    var iRc: Atomic[int]
    iRc.store(0, moRelaxed)
    if not args.discardStdout:
      var iArgs = InterceptArgs(shardIdx: idx, inputFd: stdoutPipeR,
                                tmpPath: tmpPath, chromBufPtr: args.chromBufPtr,
                                directFd: -1, directUncompress: false,
                                compressLevel: 6,
                                outputCounter: args.outputCounterPtr,
                                rc: addr iRc)
      if idx == 0 and args.outFd >= 0:
        iArgs.directFd = args.outFd
        iArgs.directUncompress = args.forceUncompress
      elif args.forceUncompress:
        iArgs.compressLevel = 1
      createThread(iThread, interceptThread, iArgs)

    # Write decompressed shard data to subprocess stdin (synchronous).
    var task = args.tasksPtr[][idx]
    task.outFd = stdinPipe[1]
    task.decompress = true
    discard doWriteShard(task)

    # Wait for subprocess to exit.
    var status: cint
    discard posix.waitpid(pid, status, 0)
    let code = int((status shr 8) and 0xff)
    info(&"shard {idx + 1}: exit {code}")
    if code != 0:
      stderr.writeLine &"shard {idx + 1}: pipeline exited with code {code}"
      gAnyFailed.store(true, moRelease)

    # Wait for interceptor to finish writing tmp file.
    if not args.discardStdout:
      joinThread(iThread)
      if iRc.load(moRelaxed) != 0:
        gAnyFailed.store(true, moRelease)

    # Deposit tmp path for concat thread (sentinel "" for shard 0 direct write).
    if not args.discardStdout:
      let depositPath = if idx == 0 and args.outFd >= 0: "" else: tmpPath
      args.depositsPtr[].deposit(idx, depositPath)

# ---------------------------------------------------------------------------
# Concat thread — appends tmp files to output fd in shard order
# ---------------------------------------------------------------------------

type ConcatArgs* = object
  depositsPtr*: ptr DepositQueue
  outFd*: cint
  nTotalShards*: int
  forceUncompress*: bool

proc concatThread*(args: ConcatArgs) {.thread.} =
  ## Wait for each shard's BGZF tmp file in order, copy to outFd, delete.
  ## Skips trailing 28-byte BGZF EOF from each tmp; writes single EOF at end.
  ## If forceUncompress: decompresses BGZF before writing (for -u flag).
  for i in 0 ..< args.nTotalShards:
    let tmpPath = args.depositsPtr[].waitFor(i)
    if tmpPath.len == 0:
      continue  # shard 0 wrote directly to outFd
    let fd = posix.open(tmpPath.cstring, O_RDONLY)
    if fd < 0:
      stderr.writeLine &"error: could not open tmp file: {tmpPath}"
      continue
    var st: Stat
    discard fstat(fd, st)
    let fileSize = st.st_size
    if args.forceUncompress:
      # Decompress BGZF blocks and write raw bytes.
      const BufSize = 65536
      var buf = newSeqUninit[byte](BufSize)
      var carry: seq[byte]
      var decompBuf: seq[byte]
      while true:
        let n = posix.read(fd, addr buf[0], BufSize)
        if n <= 0: break
        carry.add(buf.toOpenArray(0, n.int - 1))
        var p = 0
        while p + 18 <= carry.len:
          let blkSize = bgzfBlockSize(carry.toOpenArray(p, carry.high))
          if blkSize <= 0: break
          if p + blkSize > carry.len: break
          decompressBgzfInto(carry.toOpenArray(p, p + blkSize - 1), decompBuf)
          if decompBuf.len > 0:
            var w = 0
            while w < decompBuf.len:
              let nw = posix.write(args.outFd, unsafeAddr decompBuf[w],
                                   decompBuf.len - w)
              if nw <= 0: break
              w += nw
          p += blkSize
        if p > 0:
          if p < carry.len:
            moveMem(addr carry[0], addr carry[p], carry.len - p)
            carry.setLen(carry.len - p)
          else:
            carry.setLen(0)
    else:
      # Copy fileSize - 28 bytes (skip trailing BGZF EOF block).
      let copySize = if fileSize >= 28: fileSize - 28 else: fileSize
      copyRange(args.outFd, fd, copySize)
    discard posix.close(fd)
    try: removeFile(tmpPath) except OSError: discard

  # Write single BGZF EOF block at the end (unless -u).
  if not args.forceUncompress:
    var w = 0
    while w < BGZF_EOF.len:
      let n = posix.write(args.outFd, unsafeAddr BGZF_EOF[w], BGZF_EOF.len - w)
      if n <= 0: break
      w += n

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

type RunPipelineCfg* = object
  ## Configuration for the run subcommand.
  vcfPath*:             string
  nWorkers*:            int
  maxShardsPerWorker*:  int
  nThreads*:            int
  forceScan*:           bool
  stages*:              seq[seq[string]]
  noKill*:              bool
  clampShards*:         bool
  outputPath*:          string   ## single output file path; "" when toStdout
  toStdout*:            bool     ## concat thread writes to stdout
  discardStdout*:         bool     ## --discard: stdout → /dev/null, no concat
  discardStderr*:       bool     ## --discard-stderr: tool stderr → /dev/null
  forceUncompress*:     bool     ## -u: decompress BGZF tmp files for final output
  warnBraceNoDiscard*:  bool     ## {} present without --discard: warn + check

proc runPipeline*(cfg: RunPipelineCfg) =
  ## Scatter into n*m shards, run n workers concurrently, concat output
  ## in shard order to cfg.outputPath (or stdout).
  ## Tool-managed mode: workers discard stdout, no concat thread.
  let nTotalShards = cfg.nWorkers * cfg.maxShardsPerWorker

  # Compute shard tasks.
  let actualThreads = if cfg.nThreads == 0: countProcessors() else: cfg.nThreads
  let fmt = if cfg.vcfPath.endsWith(".bcf"): ffBcf else: ffVcf
  var tasks = computeShards(cfg.vcfPath, nTotalShards, actualThreads,
                            cfg.forceScan, fmt, cfg.clampShards)
  let nShards = tasks.len  # may be < nTotalShards after clamping
  let nWorkers = min(cfg.nWorkers, nShards)

  # Create tmp dir for shard output files.
  let tmpDir = createTempDir("blocky_", "")
  info(&"run: {nShards} shards, {nWorkers} workers, tmpDir={tmpDir}")

  # Reset global state.
  gNextShard.store(0, moRelaxed)
  gAnyFailed.store(false, moRelaxed)
  gShardsWithOutput.store(0, moRelaxed)

  # Allocate shared #CHROM validation buffer.
  let chromBuf = cast[ptr SharedBuf](allocShared0(sizeof(SharedBuf)))

  # Open output fd for concat thread (unless tool-managed).
  var outFd: cint = -1
  if not cfg.discardStdout:
    if cfg.toStdout:
      outFd = STDOUT_FILENO
    else:
      outFd = posix.open(cfg.outputPath.cstring,
                          O_WRONLY or O_CREAT or O_TRUNC, 0o666.Mode)
      if outFd < 0:
        stderr.writeLine "error: could not create output file: " & cfg.outputPath
        quit(1)

  # Start concat thread (unless tool-managed).
  if cfg.discardStdout: info("run: --discard mode, no concat thread")
  elif cfg.toStdout: info("run: concat to stdout")
  else: info(&"run: concat to {cfg.outputPath}")
  var deposits = if cfg.discardStdout: DepositQueue() else: newDepositQueue(nShards)
  var cThread: Thread[ConcatArgs]
  if not cfg.discardStdout:
    createThread(cThread, concatThread,
                 ConcatArgs(depositsPtr: addr deposits, outFd: outFd,
                            nTotalShards: nShards,
                            forceUncompress: cfg.forceUncompress))

  # Start worker threads.
  var stages = cfg.stages
  let counterPtr = if cfg.warnBraceNoDiscard: addr gShardsWithOutput else: nil
  let wArgs = WorkerArgs(tasksPtr: addr tasks, nTotalShards: nShards,
                         stagesPtr: addr stages, tmpDirPtr: unsafeAddr tmpDir,
                         depositsPtr: addr deposits, chromBufPtr: chromBuf,
                         discardStdout: cfg.discardStdout,
                         discardStderr: cfg.discardStderr, noKill: cfg.noKill,
                         outFd: outFd, forceUncompress: cfg.forceUncompress,
                         outputCounterPtr: counterPtr)
  var wThreads = newSeq[Thread[WorkerArgs]](nWorkers)
  for i in 0 ..< nWorkers:
    createThread(wThreads[i], workerThread, wArgs)

  # Wait for all workers to finish.
  for i in 0 ..< nWorkers:
    joinThread(wThreads[i])

  # Wait for concat thread to finish.
  if not cfg.discardStdout:
    joinThread(cThread)
    freeDepositQueue(deposits)
    if not cfg.toStdout:
      discard posix.close(outFd)

  # Clean up.
  deallocShared(chromBuf)
  try: removeDir(tmpDir) except OSError: discard

  if gAnyFailed.load(moRelaxed):
    info("run: pipeline failed")
    quit(1)

  # Check for {} without --discard: if no shard produced output, likely user error.
  if cfg.warnBraceNoDiscard and gShardsWithOutput.load(moRelaxed) == 0:
    stderr.writeLine "error: no output received from any shard — did you mean --discard?"
    quit(1)

  info(&"run: complete, {nShards} shards processed")
