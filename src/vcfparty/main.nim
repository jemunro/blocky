## vcfparty CLI — argument parsing and subcommand dispatch.
## Entry point is src/vcfparty.nim which includes this file.

import std/[options, os, parseopt, strutils]
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

proc detectFormat(path: string): FileFormat =
  ## Detect file format from extension.
  ## .vcf.gz → Vcf; .bcf → Bcf; anything else → exit 1.
  if path.endsWith(".vcf.gz"):
    result = FileFormat.Vcf
  elif path.endsWith(".bcf"):
    result = FileFormat.Bcf
  else:
    let ext = if path.rfind('.') >= 0: path[path.rfind('.')..^1] else: path
    stderr.writeLine "error: unknown file format '" & ext &
      "' (expected .vcf.gz or .bcf): " & path
    quit(1)

const VERSION = "0.1.0"

proc usage() =
  ## Print top-level usage to stderr and exit 1.
  stderr.writeLine "vcfparty v" & VERSION
  stderr.writeLine ""
  stderr.writeLine "Usage: vcfparty <subcommand> [options]"
  stderr.writeLine ""
  stderr.writeLine "Subcommands:"
  stderr.writeLine "  scatter   Split a bgzipped VCF/BCF into N shards"
  stderr.writeLine "  run       Scatter, pipe each shard through a tool pipeline"
  stderr.writeLine "  gather    Concatenate pre-existing shard files into a single output"
  stderr.writeLine ""
  stderr.writeLine "Run 'vcfparty <subcommand> --help' for subcommand options."
  quit(1)

proc scatterUsage() =
  ## Print scatter subcommand usage to stderr and exit 1.
  stderr.writeLine "Usage: vcfparty scatter -n <n_shards> -o <prefix> [options] <input.vcf.gz>"
  stderr.writeLine ""
  stderr.writeLine "Options:"
  stderr.writeLine "  -n, --n-shards <int>      number of output shards (required, >= 1)"
  stderr.writeLine "  -o, --output <str>        output file prefix (required)"
  stderr.writeLine "  -t, --max-threads <int>   max threads for scan/split/write (default: min(n-shards, 8))"
  stderr.writeLine "      --force-scan          always scan BGZF blocks (ignore index even if present)"
  stderr.writeLine "  -v, --verbose             print progress info to stderr (block offsets, boundaries, shards)"
  stderr.writeLine "  -h, --help                show this help"
  quit(1)

proc nextVal(p: var OptParser; flag: string): string =
  ## Return the value for a flag, consuming the next argv token if the value
  ## was not attached (i.e. '-n 4' rather than '-n=4').
  ## Also handles the -j2 style: Nim's parseopt splits -j2 into two short
  ## options (key=j, key=2); we recover the value when the second token is
  ## all-digit and therefore cannot be a valid flag name.
  if p.val != "":
    return p.val
  p.next()
  if p.kind == cmdArgument:
    return p.key
  if p.kind == cmdShortOption and p.key.allCharsInSet({'0'..'9'}):
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
  var p = initOptParser(rawArgs)
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
      of "force-scan":
        forceScan = true
      of "v", "verbose":
        scatter.verbose = true
      of "h", "help":
        scatterUsage()
      else:
        stderr.writeLine "error: unknown option: -" & p.key
        quit(1)
    of cmdArgument:
      if inputFile != "":
        stderr.writeLine "error: unexpected argument: " & p.key
        quit(1)
      inputFile = p.key
  if not nShardsSet:
    stderr.writeLine "error: -n/--n-shards is required"
    quit(1)
  if nShards < 1:
    stderr.writeLine "error: -n must be >= 1, got: " & $nShards
    quit(1)
  if outPrefix == "":
    stderr.writeLine "error: -o/--output is required"
    quit(1)
  if inputFile == "":
    stderr.writeLine "error: input VCF file is required"
    quit(1)
  if not fileExists(inputFile):
    stderr.writeLine "error: input file not found: " & inputFile
    quit(1)
  let fmt = detectFormat(inputFile)
  if fmt == FileFormat.Bcf and forceScan:
    stderr.writeLine "error: vcfparty: --force-scan is not supported for BCF input"
    quit(1)
  if not nThreadsSet:
    nThreads = min(nShards, 8)
  warnFormatMismatch(inputFile, outPrefix)
  scatter(inputFile, nShards, outPrefix, nThreads, forceScan, fmt)

proc runUsage() =
  ## Print run subcommand usage to stderr and exit 1.
  stderr.writeLine "Usage: vcfparty run -n <n_shards> -o <output> [options] <input.vcf.gz> (--- | :::) <cmd> [args...] [(--- | :::) <cmd2> ...]"
  stderr.writeLine ""
  stderr.writeLine "Options:"
  stderr.writeLine "  -n, --n-shards <int>         number of shards (required, >= 1)"
  stderr.writeLine "  -o, --output <str>           output path or prefix (required; optional with --gather, defaults to stdout)"
  stderr.writeLine "  -j, --max-jobs <int>         max concurrent shard pipelines (default: n-shards)"
  stderr.writeLine "  -t, --max-threads <int>      max threads for scatter/validation (default: min(max-jobs, 8))"
  stderr.writeLine "      --force-scan             always scan BGZF blocks (ignore index even if present)"
  stderr.writeLine "      --no-kill                on failure, let sibling shards finish (default: kill them)"
  stderr.writeLine "      --gather                 gather shard outputs into single -o file"
  stderr.writeLine "      --header-pattern <pat>   strip lines starting with pat from shards 2..N (text format only)"
  stderr.writeLine "      --header-n <n>           strip the first n lines from shards 2..N (text format only)"
  stderr.writeLine "      --tmp-dir <dir>          temp dir for gather shard files (default: $TMPDIR/vcfparty)"
  stderr.writeLine "  -v, --verbose                print per-shard progress to stderr"
  stderr.writeLine "  -h, --help                   show this help"
  stderr.writeLine ""
  stderr.writeLine "Separate pipeline stages with --- or ::::"
  stderr.writeLine "  vcfparty run -n 8 -o out.vcf.gz input.vcf.gz --- bcftools view -i \"GT='alt'\" -Oz"
  stderr.writeLine "  vcfparty run -n 8 --gather -o out.vcf.gz input.vcf.gz --- bcftools view -Oz"
  quit(1)

proc runRun(rawArgs: seq[string]) =
  ## Parse run subcommand arguments and call runShards() or runShardsGather().
  ## Everything before the first --- is parsed as paravar options.
  ## Everything from --- onward is the pipeline stage definition.
  var firstSep = -1
  for i, tok in rawArgs:
    if tok == "---" or tok == ":::":
      firstSep = i
      break
  # Parse paravar options from the slice before --- (or all args if no --- found;
  # parseRunArgv will emit the appropriate error when called below).
  let paravarPart = if firstSep < 0: rawArgs else: rawArgs[0 ..< firstSep]
  var nShards         = 0
  var nShardsSet      = false
  var outPrefix       = ""
  var inputFile       = ""
  var nJobs           = 0
  var nJobsSet        = false
  var nThreads        = 0
  var nThreadsSet     = false
  var forceScan       = false
  var noKill          = false
  var gatherMode      = false
  var headerPattern   = ""
  var headerPatternSet = false
  var headerN         = 0
  var headerNSet      = false
  var tmpDir          = ""
  var p = initOptParser(paravarPart)
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
      of "j", "max-jobs":
        let v = nextVal(p, "j")
        try:
          nJobs = v.parseInt
        except ValueError:
          stderr.writeLine "error: --max-jobs must be an integer, got: " & v
          quit(1)
        if nJobs < 0:
          stderr.writeLine "error: --max-jobs must be >= 0, got: " & $nJobs
          quit(1)
        nJobsSet = true
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
      of "force-scan":
        forceScan = true
      of "no-kill":
        noKill = true
      of "gather":
        gatherMode = true
      of "header-pattern":
        headerPattern    = nextVal(p, "header-pattern")
        headerPatternSet = true
      of "header-n":
        let v = nextVal(p, "header-n")
        try:
          headerN = v.parseInt
        except ValueError:
          stderr.writeLine "error: --header-n must be an integer, got: " & v
          quit(1)
        if headerN < 0:
          stderr.writeLine "error: --header-n must be >= 0, got: " & $headerN
          quit(1)
        headerNSet = true
      of "tmp-dir":
        tmpDir = nextVal(p, "tmp-dir")
      of "v", "verbose":
        scatter.verbose = true
      of "h", "help":
        runUsage()
      else:
        stderr.writeLine "error: unknown option: -" & p.key
        quit(1)
    of cmdArgument:
      if inputFile != "":
        stderr.writeLine "error: unexpected argument: " & p.key
        quit(1)
      inputFile = p.key
  if not nShardsSet:
    stderr.writeLine "error: -n/--n-shards is required"
    quit(1)
  if nShards < 1:
    stderr.writeLine "error: -n must be >= 1, got: " & $nShards
    quit(1)
  if inputFile == "":
    stderr.writeLine "error: input VCF file is required"
    quit(1)
  if not fileExists(inputFile):
    stderr.writeLine "error: input file not found: " & inputFile
    quit(1)
  let fmt = detectFormat(inputFile)
  if fmt == FileFormat.Bcf and forceScan:
    stderr.writeLine "error: vcfparty: --force-scan is not supported for BCF input"
    quit(1)
  if not nJobsSet:
    nJobs = nShards
  if not nThreadsSet:
    nThreads = min(nJobs, 8)
  let (_, stages) = parseRunArgv(rawArgs)
  let hasBrace    = hasBracePlaceholder(stages)
  let mode        = inferRunMode(outPrefix != "", hasBrace, gatherMode)
  case mode
  of rmGatherFile, rmGatherStdout, rmGatherTool:
    let isStdout = (outPrefix == "" or outPrefix == "/dev/stdout")
    let (gFmt, gComp) = inferGatherFormat(outPrefix, "")
    let resolvedTmpDir =
      if tmpDir != "": tmpDir
      else: getEnv("TMPDIR", "/tmp") / "vcfparty"
    var cfg = GatherConfig(
      format:      gFmt,
      compression: if isStdout: gcUncompressed else: gComp,
      outputPath:  if isStdout: "" else: outPrefix,
      tmpDir:      resolvedTmpDir,
      shardCount:  nShards,
      toStdout:    isStdout)
    if headerPatternSet:
      cfg.headerPattern = some(headerPattern)
    if headerNSet:
      cfg.headerN = some(headerN)
    validateGatherConfig(cfg)
    if not isStdout:
      warnFormatMismatch(inputFile, outPrefix)
    runShardsGather(inputFile, nShards, outPrefix, nThreads, forceScan, nJobs,
                    stages, noKill, cfg)
  of rmToolManaged:
    runShards(inputFile, nShards, outPrefix, nThreads, forceScan, nJobs,
              stages, noKill, toolManaged = true)
  of rmNormal:
    warnFormatMismatch(inputFile, outPrefix)
    runShards(inputFile, nShards, outPrefix, nThreads, forceScan, nJobs,
              stages, noKill)

proc gatherUsage() =
  ## Print gather subcommand usage to stderr and exit 1.
  stderr.writeLine "Usage: vcfparty gather [-o <output>] [options] <shard1> [<shard2> ...]"
  stderr.writeLine ""
  stderr.writeLine "Options:"
  stderr.writeLine "  -o, --output <str>           gather output path (default: stdout)"
  stderr.writeLine "      --header-pattern <pat>   strip lines starting with pat from shards 2..N (text format only)"
  stderr.writeLine "      --header-n <n>           strip the first n lines from shards 2..N (text format only)"
  stderr.writeLine "  -v, --verbose                print progress to stderr"
  stderr.writeLine "  -h, --help                   show this help"
  quit(1)

proc runGather(rawArgs: seq[string]) =
  ## Parse gather subcommand arguments and concatenate pre-existing shard files.
  var outPath          = ""
  var headerPattern    = ""
  var headerPatternSet = false
  var headerN          = 0
  var headerNSet       = false
  var inputFiles: seq[string]
  var p = initOptParser(rawArgs)
  while true:
    p.next()
    case p.kind
    of cmdEnd: break
    of cmdShortOption, cmdLongOption:
      case p.key
      of "o", "output":
        outPath = nextVal(p, "o")
      of "header-pattern":
        headerPattern    = nextVal(p, "header-pattern")
        headerPatternSet = true
      of "header-n":
        let v = nextVal(p, "header-n")
        try:
          headerN = v.parseInt
        except ValueError:
          stderr.writeLine "error: --header-n must be an integer, got: " & v
          quit(1)
        if headerN < 0:
          stderr.writeLine "error: --header-n must be >= 0, got: " & $headerN
          quit(1)
        headerNSet = true
      of "v", "verbose":
        scatter.verbose = true
      of "h", "help":
        gatherUsage()
      else:
        stderr.writeLine "error: unknown option: -" & p.key
        quit(1)
    of cmdArgument:
      inputFiles.add(p.key)
  if inputFiles.len == 0:
    stderr.writeLine "error: at least one input shard file is required"
    quit(1)
  for f in inputFiles:
    if not fileExists(f):
      stderr.writeLine "error: input file not found: " & f
      quit(1)
  let isStdout = (outPath == "" or outPath == "/dev/stdout")
  let (gFmt, gComp) = inferGatherFormat(outPath, "")
  var cfg = GatherConfig(
    format:      gFmt,
    compression: if isStdout: gcUncompressed else: gComp,
    outputPath:  if isStdout: "" else: outPath,
    shardCount:  inputFiles.len,
    toStdout:    isStdout)
  if headerPatternSet:
    cfg.headerPattern = some(headerPattern)
  if headerNSet:
    cfg.headerN = some(headerN)
  validateGatherConfig(cfg)
  if not isStdout:
    let outDir = outPath.parentDir
    if outDir != "":
      createDir(outDir)
  gatherFiles(cfg, inputFiles)

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
  of "--version":
    echo "vcfparty v" & VERSION
  of "--help", "-h":
    usage()
  else:
    stderr.writeLine "error: unknown subcommand '" & args[0] & "'"
    usage()
