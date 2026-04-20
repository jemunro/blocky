## blocky CLI — argument parsing and subcommand dispatch.
## Entry point is src/blocky.nim which includes this file.

import std/[os, parseopt, posix, strformat, strutils]
import bgzf
import scatter
import run
import gather

proc warnFormatMismatch(inputPath: string; outputPath: string) =
  ## Warn if input and output format extensions disagree (VCF↔BCF mismatch).
  let inVcf  = inputPath.endsWith(".vcf.gz")
  let inBcf  = inputPath.endsWith(".bcf")
  let outBcf = outputPath.endsWith(".bcf")
  let outVcf = outputPath.endsWith(".vcf.gz") or outputPath.endsWith(".vcf")
  if (inVcf and outBcf) or (inBcf and outVcf):
    stderr.writeLine "warning: input and output formats differ; " &
      "format conversion is the pipeline's responsibility"

const NimblePkgVersion {.strdefine.} = "dev"
const VERSION = NimblePkgVersion

proc usage(code: int = 1) =
  ## Print top-level usage and exit with code (0 for --help, 1 for errors).
  let f = if code == 0: stdout else: stderr
  f.writeLine "blocky v" & VERSION
  f.writeLine ""
  f.writeLine "Usage: blocky <subcommand> [options]"
  f.writeLine ""
  f.writeLine "Subcommands:"
  f.writeLine "  scatter      Split a bgzipped file into N shards"
  f.writeLine "  run          Scatter, pipe each shard through a tool pipeline"
  f.writeLine "  gather       Concatenate pre-existing shard files into a single output"
  f.writeLine "  compress     BGZF-compress a file (like bgzip)"
  f.writeLine "  decompress   Decompress a BGZF file"
  f.writeLine ""
  f.writeLine "Run 'blocky <subcommand> --help' for subcommand options."
  quit(code)

proc scatterUsage(code: int = 1) =
  let f = if code == 0: stdout else: stderr
  f.writeLine "Usage: blocky scatter -n <n_shards> -o <prefix> [options] <input>"
  f.writeLine ""
  f.writeLine "Options:"
  f.writeLine "  -n, --n-shards <int>      number of output shards (required, >= 1)"
  f.writeLine "  -o, --output <str>        output file prefix (required)"
  f.writeLine "  -t, --max-threads <int>   max threads for scan/split/write (default: min(n, 8))"
  f.writeLine "      --scan                scan BGZF blocks (ignore index even if present)"
  f.writeLine "      --clamp               reduce -n if fewer split points available"
  f.writeLine "  -v, --verbose             print progress info to stderr"
  f.writeLine "  -h, --help                show this help"
  quit(code)

## Short flags that DO NOT take a value. parseopt uses this set to correctly
## parse attached values like `-n50` (otherwise it splits into `-n`, `-5`, `-0`).
const ShortNoVal = {'c', 'u', 'v', 'h'}

proc nextVal(p: var OptParser; flag: string): string =
  ## Return the value for a flag, consuming the next argv token if the value
  ## was not attached (i.e. '-n 4' rather than '-n=4' or '-n4').
  if p.val != "":
    return p.val
  p.next()
  if p.kind == cmdArgument:
    return p.key
  stderr.writeLine "error: -" & flag & " requires a value"
  quit(1)

proc runScatter(rawArgs: seq[string]) =
  ## Parse scatter subcommand arguments and call scatter().
  var nShards      = 0
  var nShardsSet   = false
  var outPrefix    = ""
  var inputFile    = ""
  var nThreads     = 0
  var nThreadsSet  = false
  var forceScan    = false
  var clampShards  = false
  var p = initOptParser(rawArgs, shortNoVal = ShortNoVal)
  while true:
    p.next()
    case p.kind
    of cmdEnd: break
    of cmdShortOption, cmdLongOption:
      case p.key
      of "n", "n-shards":
        let v = nextVal(p, "n")
        try:
          nShards = v.parseInt
        except ValueError:
          stderr.writeLine "error: -n must be an integer, got: " & v
          quit(1)
        nShardsSet = true
      of "o", "output":
        outPrefix = nextVal(p, "o")
      of "t", "max-threads":
        let v = nextVal(p, "t")
        try:
          nThreads = v.parseInt
        except ValueError:
          stderr.writeLine "error: -t must be an integer, got: " & v
          quit(1)
        if nThreads < 0:
          stderr.writeLine "error: -t must be >= 0, got: " & $nThreads
          quit(1)
        nThreadsSet = true
      of "scan", "force-scan":
        forceScan = true
      of "clamp", "clamp-shards":
        clampShards = true
      of "v", "verbose":
        bgzf.verbose = true
      of "h", "help":
        scatterUsage(0)
      else:
        stderr.writeLine "error: unknown option: -" & p.key
        scatterUsage()
    of cmdArgument:
      if inputFile != "":
        stderr.writeLine "error: unexpected argument: " & p.key
        scatterUsage()
      inputFile = p.key
  if not nShardsSet:
    stderr.writeLine "error: -n/--n-shards is required"
    scatterUsage()
  if nShards < 1:
    stderr.writeLine "error: -n must be >= 1, got: " & $nShards
    scatterUsage()
  if outPrefix == "":
    stderr.writeLine "error: -o/--output is required"
    scatterUsage()
  if inputFile == "":
    stderr.writeLine "error: input file is required"
    scatterUsage()
  if not fileExists(inputFile):
    stderr.writeLine "error: input file not found: " & inputFile
    scatterUsage()
  let fmt = inferInputFormat(inputFile)
  info(&"scatter: input={inputFile}, format={fmt}")
  if fmt == ffBcf and forceScan:
    stderr.writeLine "error: --scan is not supported for BCF input"
    scatterUsage()
  if not nThreadsSet:
    nThreads = min(nShards, 8)
  warnFormatMismatch(inputFile, outPrefix)
  scatter(inputFile, nShards, outPrefix, nThreads, forceScan, fmt, clampShards)

proc runUsage(code: int = 1) =
  let f = if code == 0: stdout else: stderr
  f.writeLine "Usage: blocky run -n <n_workers> [-m <shards_per_worker>] [options] <input> (--- | :::) <cmd> [args...]"
  f.writeLine ""
  f.writeLine "Options:"
  f.writeLine "  -n, --n-workers <int>              number of concurrent worker pipelines (required, >= 1)"
  f.writeLine "  -m, --max-shards-per-worker <int>  max shards each worker processes (default: 1)"
  f.writeLine "  -o, --output <str>                 output path (default: stdout)"
  f.writeLine "  -u, --uncompressed                 force uncompressed file output"
  f.writeLine "      --discard                      discard subprocess stdout (tool manages own output)"
  f.writeLine "      --discard-stderr               discard subprocess stderr"
  f.writeLine "  -t, --max-threads <int>            max scatter threads (default: min(n-workers, 8))"
  f.writeLine "      --scan                         scan BGZF blocks (ignore index even if present)"
  f.writeLine "      --clamp                        reduce total shards if fewer split points available"
  f.writeLine "      --no-kill                      on failure, let sibling shards finish"
  f.writeLine "  -v, --verbose                      print per-shard progress to stderr"
  f.writeLine "  -h, --help                         show this help"
  f.writeLine ""
  f.writeLine "Separate pipeline stages with ::: or ---."
  f.writeLine "  blocky run -n 4 -o out.vcf input.vcf.gz ::: bcftools view -Ov"
  f.writeLine "  blocky run -n 4 -o out.bcf input.vcf.gz ::: bcftools +fill-tags -Ou ::: bcftools view -Ob"
  f.writeLine "  blocky run -n 4 --discard input.vcf.gz ::: bcftools view -Oz -o out.{}.vcf.gz"
  quit(code)

proc runRun(rawArgs: seq[string]) =
  ## Parse `run` subcommand arguments, build a RunPipelineCfg, and dispatch
  ## to run.runPipeline.
  ## Everything before the first --- is parsed as blocky options.
  ## Everything from --- onward is the pipeline stage definition.
  var firstSep = -1
  for i, tok in rawArgs:
    if tok == "---" or tok == ":::":
      firstSep = i
      break
  # Parse blocky options from the slice before --- (or all args if no --- found;
  # parseRunArgv will emit the appropriate error when called below).
  let blockyPart = if firstSep < 0: rawArgs else: rawArgs[0 ..< firstSep]
  var nWorkers        = 0
  var nWorkersSet     = false
  var maxShards       = 1
  var outPrefix       = ""
  var inputFile       = ""
  var nThreads        = 0
  var nThreadsSet     = false
  var forceScan       = false
  var clampShards     = false
  var forceUncompress = false
  var discardStdout   = false
  var discardStderr   = false
  var noKill          = false
  var p = initOptParser(blockyPart, shortNoVal = ShortNoVal)
  while true:
    p.next()
    case p.kind
    of cmdEnd: break
    of cmdShortOption, cmdLongOption:
      case p.key
      of "n", "n-workers":
        let v = nextVal(p, "n")
        try:
          nWorkers = v.parseInt
        except ValueError:
          stderr.writeLine "error: -n must be an integer, got: " & v
          quit(1)
        nWorkersSet = true
      of "m", "max-shards-per-worker":
        let v = nextVal(p, "m")
        try:
          maxShards = v.parseInt
        except ValueError:
          stderr.writeLine "error: -m must be an integer, got: " & v
          quit(1)
        if maxShards < 1:
          stderr.writeLine "error: -m must be >= 1, got: " & $maxShards
          quit(1)
      of "o", "output":
        outPrefix = nextVal(p, "o")
      of "u", "uncompressed":
        forceUncompress = true
      of "t", "max-threads":
        let v = nextVal(p, "t")
        try:
          nThreads = v.parseInt
        except ValueError:
          stderr.writeLine "error: -t must be an integer, got: " & v
          runUsage()
        if nThreads < 0:
          stderr.writeLine "error: -t must be >= 0, got: " & $nThreads
          runUsage()
        nThreadsSet = true
      of "scan", "force-scan":
        forceScan = true
      of "clamp", "clamp-shards":
        clampShards = true
      of "discard":
        discardStdout = true
      of "discard-stderr":
        discardStderr = true
      of "no-kill":
        noKill = true
      of "v", "verbose":
        bgzf.verbose = true
      of "h", "help":
        runUsage(0)
      else:
        stderr.writeLine "error: unknown option: -" & p.key
        runUsage()
    of cmdArgument:
      if inputFile != "":
        stderr.writeLine "error: unexpected argument: " & p.key
        runUsage()
      inputFile = p.key
  if not nWorkersSet:
    stderr.writeLine "error: -n/--n-workers is required"
    runUsage()
  if nWorkers < 1:
    stderr.writeLine "error: -n must be >= 1, got: " & $nWorkers
    runUsage()
  if inputFile == "":
    stderr.writeLine "error: input file is required"
    runUsage()
  if not fileExists(inputFile):
    stderr.writeLine "error: input file not found: " & inputFile
    runUsage()
  let fmt = inferInputFormat(inputFile)
  info(&"run: input={inputFile}, format={fmt}")
  if fmt == ffBcf and forceScan:
    stderr.writeLine "error: --scan is not supported for BCF input"
    runUsage()
  if not nThreadsSet:
    nThreads = min(nWorkers, 8)
  let (_, stages) = parseRunArgv(rawArgs)
  let hasBrace    = hasBracePlaceholder(stages)
  if discardStdout and outPrefix != "":
    stderr.writeLine "error: --discard and -o are mutually exclusive"
    runUsage()
  if discardStdout and forceUncompress:
    stderr.writeLine "error: --discard and -u/--uncompressed are mutually exclusive"
    runUsage()
  let mode = inferRunMode(outPrefix != "", discardStdout)
  info(&"run: mode={mode}, stages={stages.len}")
  # Warn if {} present without --discard (user may have forgotten --discard).
  let warnBrace = hasBrace and not discardStdout
  if warnBrace:
    stderr.writeLine "warning: {} found in tool command but stdout is not discarded"
    stderr.writeLine "  (use --discard if the tool manages its own output files)"
  if mode == rmFile:
    warnFormatMismatch(inputFile, outPrefix)
  runPipeline(RunPipelineCfg(
    vcfPath:             inputFile,
    nWorkers:            nWorkers,
    maxShardsPerWorker:  maxShards,
    nThreads:            nThreads,
    forceScan:           forceScan,
    stages:              stages,
    noKill:              noKill,
    clampShards:         clampShards,
    outputPath:          outPrefix,
    toStdout:            (mode == rmStdout),
    discardStdout:       (mode == rmDiscard),
    discardStderr:       discardStderr,
    forceUncompress:     forceUncompress,
    warnBraceNoDiscard:  warnBrace))

proc gatherUsage(code: int = 1) =
  let f = if code == 0: stdout else: stderr
  f.writeLine "Usage: blocky gather [-o <output>] [options] <shard1> [<shard2> ...]"
  f.writeLine ""
  f.writeLine "Options:"
  f.writeLine "  -o, --output <str>           gather output path (default: stdout)"
  f.writeLine "  -v, --verbose                print progress to stderr"
  f.writeLine "  -h, --help                   show this help"
  quit(code)

proc runGather(rawArgs: seq[string]) =
  ## Parse gather subcommand arguments and concatenate pre-existing shard files.
  var outPath: string = ""
  var inputFiles: seq[string]
  var p = initOptParser(rawArgs, shortNoVal = ShortNoVal)
  while true:
    p.next()
    case p.kind
    of cmdEnd: break
    of cmdShortOption, cmdLongOption:
      case p.key
      of "o", "output":
        outPath = nextVal(p, "o")
      of "v", "verbose":
        bgzf.verbose = true
      of "h", "help":
        gatherUsage(0)
      else:
        stderr.writeLine "error: unknown option: -" & p.key
        gatherUsage()
    of cmdArgument:
      inputFiles.add(p.key)
  if inputFiles.len == 0:
    stderr.writeLine "error: at least one input shard file is required"
    gatherUsage()
  for f in inputFiles:
    if not fileExists(f):
      stderr.writeLine "error: input file not found: " & f
      quit(1)
  let isStdout = (outPath == "" or outPath == "/dev/stdout")
  let (gFmt, gComp) = inferFileFormat(outPath, "")
  let cfg = GatherConfig(
    format:      gFmt,
    compression: if isStdout: compNone else: gComp,
    outputPath:  if isStdout: "" else: outPath,
    shardCount:  inputFiles.len,
    toStdout:    isStdout)
  if not isStdout:
    let outDir = outPath.parentDir
    if outDir != "":
      createDir(outDir)
  gatherFiles(cfg, inputFiles)

proc compressUsage(code: int = 1) =
  let f = if code == 0: stdout else: stderr
  f.writeLine "Usage: blocky compress [-c] [file]"
  f.writeLine ""
  f.writeLine "Compress input to BGZF format."
  f.writeLine ""
  f.writeLine "Options:"
  f.writeLine "  -c, --stdout    write to stdout, keep original file"
  f.writeLine "  -h, --help      show this help"
  f.writeLine ""
  f.writeLine "If no file is given, reads from stdin and writes to stdout."
  quit(code)

proc decompressUsage(code: int = 1) =
  let f = if code == 0: stdout else: stderr
  f.writeLine "Usage: blocky decompress [-c] [file]"
  f.writeLine ""
  f.writeLine "Decompress BGZF input to raw bytes."
  f.writeLine ""
  f.writeLine "Options:"
  f.writeLine "  -c, --stdout    write to stdout, keep original file"
  f.writeLine "  -h, --help      show this help"
  f.writeLine ""
  f.writeLine "If no file is given, reads from stdin and writes to stdout."
  quit(code)

proc runCompress(rawArgs: seq[string]) =
  var toStdout = false
  var inputFile = ""
  var p = initOptParser(rawArgs, shortNoVal = ShortNoVal)
  while true:
    p.next()
    case p.kind
    of cmdEnd: break
    of cmdShortOption, cmdLongOption:
      case p.key
      of "c", "stdout": toStdout = true
      of "h", "help": compressUsage(0)
      else:
        stderr.writeLine "error: unknown option: -" & p.key
        quit(1)
    of cmdArgument:
      if inputFile != "":
        stderr.writeLine "error: unexpected argument: " & p.key
        quit(1)
      inputFile = p.key

  var inFile: File
  var outFile: File
  var outPath = ""

  if inputFile == "":
    # stdin mode — must be a pipe, not a terminal.
    if isatty(0) != 0:
      stderr.writeLine "error: no input file and stdin is a terminal"
      quit(1)
    inFile = stdin
    toStdout = true
  else:
    if not fileExists(inputFile):
      stderr.writeLine "error: input file not found: " & inputFile
      quit(1)
    inFile = open(inputFile, fmRead)
    # Warn if input looks already compressed.
    var peek: array[4, byte]
    let n = readBytes(inFile, peek, 0, 4)
    if n >= 2 and peek[0] == 0x1f and peek[1] == 0x8b:
      stderr.writeLine "warning: input appears to be gzip/BGZF compressed already"
    inFile.setFilePos(0)

  if toStdout:
    outFile = stdout
  else:
    outPath = inputFile & ".gz"
    if fileExists(outPath):
      stderr.writeLine "error: output file already exists: " & outPath
      quit(1)
    outFile = open(outPath, fmWrite)

  bgzfCompressStream(inFile, outFile)

  if inputFile != "":
    inFile.close()
  if not toStdout:
    outFile.close()
    removeFile(inputFile)

proc runDecompress(rawArgs: seq[string]) =
  var toStdout = false
  var inputFile = ""
  var p = initOptParser(rawArgs, shortNoVal = ShortNoVal)
  while true:
    p.next()
    case p.kind
    of cmdEnd: break
    of cmdShortOption, cmdLongOption:
      case p.key
      of "c", "stdout": toStdout = true
      of "h", "help": decompressUsage(0)
      else:
        stderr.writeLine "error: unknown option: -" & p.key
        quit(1)
    of cmdArgument:
      if inputFile != "":
        stderr.writeLine "error: unexpected argument: " & p.key
        quit(1)
      inputFile = p.key

  var inFile: File
  var outFile: File
  var outPath = ""

  if inputFile == "":
    if isatty(0) != 0:
      stderr.writeLine "error: no input file and stdin is a terminal"
      quit(1)
    inFile = stdin
    toStdout = true
  else:
    if not fileExists(inputFile):
      stderr.writeLine "error: input file not found: " & inputFile
      quit(1)
    inFile = open(inputFile, fmRead)
    # Warn if input does NOT look like BGZF/gzip.
    var peek: array[4, byte]
    let n = readBytes(inFile, peek, 0, 4)
    if n < 4 or peek[0] != 0x1f or peek[1] != 0x8b:
      stderr.writeLine "warning: input does not appear to be gzip/BGZF compressed"
    inFile.setFilePos(0)
    if not toStdout:
      if not inputFile.endsWith(".gz"):
        stderr.writeLine "error: cannot infer output name (input does not end in .gz); use -c for stdout"
        quit(1)
      outPath = inputFile[0 ..< inputFile.len - 3]

  if toStdout:
    outFile = stdout
  else:
    if fileExists(outPath):
      stderr.writeLine "error: output file already exists: " & outPath
      quit(1)
    outFile = open(outPath, fmWrite)

  bgzfDecompressStream(inFile, outFile)

  if inputFile != "":
    inFile.close()
  if not toStdout:
    outFile.close()
    removeFile(inputFile)

proc mainEntry*() =
  ## Top-level entry point: dispatch to the appropriate subcommand.
  let args = commandLineParams()
  if args.len == 0:
    usage()
  case args[0]
  of "scatter":
    runScatter(args[1 .. ^1])
  of "run":
    runRun(args[1 .. ^1])
  of "gather":
    runGather(args[1 .. ^1])
  of "compress":
    runCompress(args[1 .. ^1])
  of "decompress":
    runDecompress(args[1 .. ^1])
  of "--version":
    echo "blocky v" & VERSION
  of "--help", "-h":
    usage(0)
  else:
    stderr.writeLine "error: unknown subcommand '" & args[0] & "'"
    usage()
