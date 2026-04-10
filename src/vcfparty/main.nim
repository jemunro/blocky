## vcfparty CLI — argument parsing and subcommand dispatch.
## Entry point is src/vcfparty.nim which includes this file.

import std/[options, os, parseopt, strutils, tempfiles]
import vcf_utils
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
  stderr.writeLine "      --clamp-shards        if -n exceeds available index entries, reduce -n instead of erroring"
  stderr.writeLine "  -v, --verbose             print progress info to stderr (block offsets, boundaries, shards)"
  stderr.writeLine "  -h, --help                show this help"
  quit(1)

## Short flags that DO NOT take a value. parseopt uses this set to correctly
## parse attached values like `-n50` (otherwise it splits into `-n`, `-5`, `-0`).
const ShortNoVal = {'u', 'v', 'h'}

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
      of "force-scan":
        forceScan = true
      of "clamp-shards":
        clampShards = true
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
  let fmt = inferInputFormat(inputFile)
  if fmt == ffBcf and forceScan:
    stderr.writeLine "error: vcfparty: --force-scan is not supported for BCF input"
    quit(1)
  if not nThreadsSet:
    nThreads = min(nShards, 8)
  warnFormatMismatch(inputFile, outPrefix)
  scatter(inputFile, nShards, outPrefix, nThreads, forceScan, fmt, clampShards)

proc runUsage() =
  ## Print run subcommand usage to stderr and exit 1.
  stderr.writeLine "Usage: vcfparty run -n <n_shards> -o <output> [options] <input.vcf.gz> (--- | :::) <cmd> [args...] [(--- | :::) <cmd2> ...]"
  stderr.writeLine ""
  stderr.writeLine "Options:"
  stderr.writeLine "  -n, --n-shards <int>         number of shards (required, >= 1); controls both shard count and concurrency"
  stderr.writeLine "  -o, --output <str>           output path or prefix (default: stdout)"
  stderr.writeLine "  -u                           force uncompressed file output (error with tool-managed {})"
  stderr.writeLine "  -t, --max-threads <int>      max threads for scatter/validation (default: min(n-shards, 8))"
  stderr.writeLine "      --force-scan             always scan BGZF blocks (ignore index even if present)"
  stderr.writeLine "      --clamp-shards           if -n exceeds available index entries, reduce -n instead of erroring"
  stderr.writeLine "      --no-kill                on failure, let sibling shards finish (default: kill them)"
  stderr.writeLine "      --header-pattern <pat>   strip lines starting with pat from shards 2..N (text format only)"
  stderr.writeLine "      --header-n <n>           strip the first n lines from shards 2..N (text format only)"
  stderr.writeLine "  -v, --verbose                print per-shard progress to stderr"
  stderr.writeLine "  -h, --help                   show this help"
  stderr.writeLine ""
  stderr.writeLine "Separate pipeline stages with --- or :::. Append a terminal operator to gather output:"
  stderr.writeLine "  vcfparty run -n 8 input.vcf.gz ::: bcftools view -i \"GT='alt'\" -Oz +concat+"
  stderr.writeLine "  vcfparty run -n 8 -o out.vcf.gz input.vcf.gz ::: bcftools view -Oz +concat+"
  stderr.writeLine "  vcfparty run -n 8 input.vcf.gz ::: bcftools view -Oz -o out.{}.vcf.gz"
  stderr.writeLine ""
  stderr.writeLine "Terminal operators (appended after last stage):"
  stderr.writeLine "  +concat+    gather in genomic order via temp files (-o required)"
  stderr.writeLine "  +merge+     k-way merge sort, interleaved scatter (-o required)"
  stderr.writeLine "  +collect+   streaming gather in arrival order (-o required)"
  stderr.writeLine "  (none)      tool manages output via {} substitution in command"
  quit(1)

proc runRun(rawArgs: seq[string]) =
  ## Parse `run` subcommand arguments, build a RunPipelineCfg, and dispatch
  ## to run.runPipeline.
  ## Everything before the first --- is parsed as vcfparty options.
  ## Everything from --- onward is the pipeline stage definition.
  var firstSep = -1
  for i, tok in rawArgs:
    if tok == "---" or tok == ":::":
      firstSep = i
      break
  # Parse vcfparty options from the slice before --- (or all args if no --- found;
  # parseRunArgv will emit the appropriate error when called below).
  let vcfpartyPart = if firstSep < 0: rawArgs else: rawArgs[0 ..< firstSep]
  var nShards         = 0
  var nShardsSet      = false
  var outPrefix       = ""
  var inputFile       = ""
  var nThreads        = 0
  var nThreadsSet     = false
  var forceScan       = false
  var clampShards     = false
  var forceUncompress = false
  var noKill          = false
  var headerPattern   = ""
  var headerPatternSet = false
  var headerN         = 0
  var headerNSet      = false
  var p = initOptParser(vcfpartyPart, shortNoVal = ShortNoVal)
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
      of "u":
        forceUncompress = true
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
      of "clamp-shards":
        clampShards = true
      of "no-kill":
        noKill = true
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
  let fmt = inferInputFormat(inputFile)
  if fmt == ffBcf and forceScan:
    stderr.writeLine "error: vcfparty: --force-scan is not supported for BCF input"
    quit(1)
  if not nThreadsSet:
    nThreads = min(nShards, 8)
  let (_, stages, termOp) = parseRunArgv(rawArgs)
  let hasBrace             = hasBracePlaceholder(stages)

  var cfg = RunPipelineCfg(
    vcfPath:     inputFile,
    nShards:     nShards,
    nThreads:    nThreads,
    forceScan:   forceScan,
    stages:      stages,
    noKill:      noKill,
    clampShards: clampShards)
  case termOp
  of topConcat:
    let isStdout = (outPrefix == "" or outPrefix == "/dev/stdout")
    let (gFmt, gComp) = inferFileFormat(outPrefix, "")
    let finalComp =
      if isStdout: compNone
      elif forceUncompress: compNone
      else: gComp
    if forceUncompress and not isStdout and gComp == compBgzf:
      stderr.writeLine "warning: -u forces uncompressed output but -o extension suggests compressed"
    let resolvedTmpDir = createTempDir("vcfparty_", "")
    var gcfg = GatherConfig(
      format:      gFmt,
      compression: finalComp,
      outputPath:  if isStdout: "" else: outPrefix,
      tmpDir:      resolvedTmpDir,
      shardCount:  nShards,
      toStdout:    isStdout)
    if headerPatternSet:
      gcfg.headerPattern = some(headerPattern)
    if headerNSet:
      gcfg.headerN = some(headerN)
    validateGatherConfig(gcfg)
    if not isStdout:
      warnFormatMismatch(inputFile, outPrefix)
    cfg.mode       = pmConcat
    cfg.outputPath = outPrefix
    cfg.toStdout   = isStdout
    cfg.gather     = gcfg
  of topCollect:
    let isStdout = (outPrefix == "" or outPrefix == "/dev/stdout")
    if not isStdout:
      warnFormatMismatch(inputFile, outPrefix)
    cfg.mode       = pmCollect
    cfg.outputPath = outPrefix
    cfg.toStdout   = isStdout
  of topMerge:
    let isStdout = (outPrefix == "" or outPrefix == "/dev/stdout")
    if not isStdout:
      warnFormatMismatch(inputFile, outPrefix)
    cfg.mode       = pmMerge
    cfg.outputPath = outPrefix
    cfg.toStdout   = isStdout
  of topNone:
    let mode = inferRunMode(outPrefix != "", hasBrace)
    if mode == rmToolManaged and forceUncompress:
      stderr.writeLine "error: -u cannot be used with tool-managed output ({}); vcfparty does not control that output"
      quit(1)
    if mode == rmNormal:
      warnFormatMismatch(inputFile, outPrefix)
    cfg.mode           = pmTool
    cfg.outputTemplate = outPrefix
    cfg.toolManaged    = (mode == rmToolManaged)
  runPipeline(cfg)

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
  var outPath: string      = ""
  var headerPattern        = ""
  var headerPatternSet     = false
  var headerN              = 0
  var headerNSet           = false
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
  let (gFmt, gComp) = inferFileFormat(outPath, "")
  var cfg = GatherConfig(
    format:      gFmt,
    compression: if isStdout: compNone else: gComp,
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
