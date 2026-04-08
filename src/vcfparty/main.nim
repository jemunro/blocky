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
  stderr.writeLine "  -s, --sequential          sequential (contiguous) scatter — default for indexed files"
  stderr.writeLine "  -i, --interleave          interleaved scatter — not yet implemented; emits warning and uses sequential"
  stderr.writeLine "  -Oz, -Ov, -Ob, -Ou"
  stderr.writeLine "  -O <z|v|b|u>, --output-type <z|v|b|u>"
  stderr.writeLine "                            output format: z=VCF BGZF, v=VCF, b=BCF BGZF, u=BCF"
  stderr.writeLine "                            format must match input; error if mismatch (sniffed, not guessed)"
  stderr.writeLine "  -t, --max-threads <int>   max threads for scan/split/write (default: min(n-shards, 8))"
  stderr.writeLine "      --force-scan          always scan BGZF blocks (ignore index even if present)"
  stderr.writeLine "  -v, --verbose             print progress info to stderr (block offsets, boundaries, shards)"
  stderr.writeLine "  -h, --help                show this help"
  quit(1)

proc parseFmtLetter(flag: string; p: var OptParser): char =
  ## Parse one of z/v/b/u from -Oz, -O z, --output-type z, -Pv, -P v, --pipe-type v.
  ## flag is "O" or "P" (used in error messages).
  ## Accepts all four bcftools format letters.
  const valid = {'z', 'v', 'b', 'u'}
  let errBad = "error: -" & flag & ": accepted letters are z (VCF BGZF), " &
               "v (VCF uncompressed), b (BCF BGZF), u (BCF uncompressed)"
  var v = ""
  if p.val != "":
    v = p.val
  else:
    p.next()
    if p.kind == cmdArgument and p.key.len == 1 and p.key[0] in valid:
      v = p.key
    elif p.kind in {cmdShortOption, cmdLongOption} and p.key.len == 1 and
         p.key[0] in valid:
      v = p.key
    else:
      stderr.writeLine errBad
      quit(1)
  if v.len != 1 or v[0] notin valid:
    stderr.writeLine errBad
    quit(1)
  result = v[0]

proc letterComp(c: char): Compression =
  ## Map format letter to output compression: z/b → BGZF; v/u → uncompressed.
  if c in {'z', 'b'}: compBgzf else: compNone

proc letterDecompress(c: char): bool =
  ## Map format letter to inputUncompress flag: v/u → true; z/b → false.
  c in {'v', 'u'}

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
  var interleave   = false
  var outputFmt: char = '\0'
  var outputFmtSet = false
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
      of "s", "sequential":
        discard  # default; accepted and ignored
      of "i", "interleave":
        interleave = true
      of "d", "decompress":
        stderr.writeLine "error: -d/--decompress is retired; use -Ou for uncompressed shard output"
        quit(1)
      of "O", "output-type":
        if outputFmtSet:
          stderr.writeLine "error: only one -O/--output-type flag is allowed"
          quit(1)
        outputFmt    = parseFmtLetter("O", p)
        outputFmtSet = true
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
  let (fmt, _) = sniffFileFormat(inputFile)
  if fmt == ffBcf and forceScan:
    stderr.writeLine "error: vcfparty: --force-scan is not supported for BCF input"
    quit(1)
  if not nThreadsSet:
    nThreads = min(nShards, 8)
  if interleave:
    stderr.writeLine "warning: -i/--interleave is not yet implemented; using sequential scatter"
  # Validate -O letter format against sniffed input format.
  if outputFmtSet:
    let flagFmt = if outputFmt in {'z', 'v'}: ffVcf else: ffBcf
    if flagFmt != fmt:
      let inName = if fmt == ffVcf: "VCF" else: "BCF"
      stderr.writeLine "error: -O" & outputFmt & ": input is " & inName &
        "; format conversion is the pipeline's responsibility"
      quit(1)
  warnFormatMismatch(inputFile, outPrefix)
  # Determine output compression: flag overrides extension inference.
  let extBgzf = outPrefix.endsWith(".vcf.gz") or outPrefix.endsWith(".vcf.bgz") or
                outPrefix.endsWith(".bcf") or outPrefix.endsWith(".bgz")
  let extUncompressed = outPrefix.endsWith(".vcf") and not outPrefix.endsWith(".vcf.gz") and
                        not outPrefix.endsWith(".vcf.bgz")
  let decompress =
    if outputFmtSet:
      let flagIsUncomp = letterDecompress(outputFmt)
      if flagIsUncomp and extBgzf:
        stderr.writeLine "warning: -O" & outputFmt &
          " disagrees with output extension (extension implies BGZF)"
      elif not flagIsUncomp and extUncompressed:
        stderr.writeLine "warning: -O" & outputFmt &
          " disagrees with output extension (extension implies uncompressed)"
      flagIsUncomp
    else:
      extUncompressed  # .vcf → uncompressed; .vcf.gz/.bcf/unknown → BGZF
  scatter(inputFile, nShards, outPrefix, nThreads, forceScan, fmt, decompress)

proc runUsage() =
  ## Print run subcommand usage to stderr and exit 1.
  stderr.writeLine "Usage: vcfparty run -n <n_shards> -o <output> [options] <input.vcf.gz> (--- | :::) <cmd> [args...] [(--- | :::) <cmd2> ...]"
  stderr.writeLine ""
  stderr.writeLine "Options:"
  stderr.writeLine "  -n, --n-shards <int>         number of shards (required, >= 1); controls both shard count and concurrency"
  stderr.writeLine "  -o, --output <str>           output path or prefix (default: stdout)"
  stderr.writeLine "  -s, --sequential             sequential (contiguous) scatter — default for indexed files"
  stderr.writeLine "  -i, --interleave             interleaved scatter — not yet implemented; emits warning and uses sequential"
  stderr.writeLine "  -Oz, -Ov, -Ob, -Ou"
  stderr.writeLine "  -O <z|v|b|u>, --output-type <z|v|b|u>"
  stderr.writeLine "                               output format: z=VCF BGZF, v=VCF, b=BCF BGZF, u=BCF"
  stderr.writeLine "                               validated against pipeline output at runtime"
  stderr.writeLine "  -Pz, -Pv, -Pb, -Pu"
  stderr.writeLine "  -P <z|v|b|u>, --pipe-type <z|v|b|u>"
  stderr.writeLine "                               format/compression sent to subprocess stdin"
  stderr.writeLine "                               format validated against input file (sniffed); v/u decompress BGZF"
  stderr.writeLine "  -t, --max-threads <int>      max threads for scatter/validation (default: min(n-shards, 8))"
  stderr.writeLine "      --force-scan             always scan BGZF blocks (ignore index even if present)"
  stderr.writeLine "      --no-kill                on failure, let sibling shards finish (default: kill them)"
  stderr.writeLine "      --header-pattern <pat>   strip lines starting with pat from shards 2..N (text format only)"
  stderr.writeLine "      --header-n <n>           strip the first n lines from shards 2..N (text format only)"
  stderr.writeLine "      --tmp-dir <dir>          temp dir for gather shard files (default: $TMPDIR/vcfparty)"
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
  ## Parse run subcommand arguments and call runShards() or runShardsGather().
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
  var noKill          = false
  var headerPattern   = ""
  var headerPatternSet = false
  var headerN         = 0
  var headerNSet      = false
  var tmpDir          = ""
  var interleave      = false
  var inputUncompress = false
  var pipeFmt: char   = '\0'
  var pipeFmtSet      = false
  var outputFmt: char = '\0'
  var outputFmtSet    = false
  var p = initOptParser(vcfpartyPart)
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
      of "s", "sequential":
        discard  # default; accepted and ignored
      of "i", "interleave":
        interleave = true
      of "d", "decompress":
        stderr.writeLine "error: -d/--decompress is retired; use -Pv (VCF uncompressed) or -Pu (BCF uncompressed)"
        quit(1)
      of "I", "input-uncompress":
        stderr.writeLine "error: -I/--input-uncompress is retired; " &
          "use -Pv (VCF uncompressed) or -Pu (BCF uncompressed)"
        quit(1)
      of "P", "pipe-type":
        if pipeFmtSet:
          stderr.writeLine "error: only one -P/--pipe-type flag is allowed"
          quit(1)
        pipeFmt    = parseFmtLetter("P", p)
        pipeFmtSet = true
        inputUncompress = letterDecompress(pipeFmt)
      of "O", "output-type":
        if outputFmtSet:
          stderr.writeLine "error: only one -O/--output-type flag is allowed"
          quit(1)
        outputFmt    = parseFmtLetter("O", p)
        outputFmtSet = true
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
  let (fmt, _) = sniffFileFormat(inputFile)
  if fmt == ffBcf and forceScan:
    stderr.writeLine "error: vcfparty: --force-scan is not supported for BCF input"
    quit(1)
  if not nThreadsSet:
    nThreads = min(nShards, 8)
  if interleave:
    stderr.writeLine "warning: -i/--interleave is not yet implemented; using sequential scatter"
  if pipeFmtSet and interleave:
    stderr.writeLine "warning: -P is ignored with interleaved scatter (input is already uncompressed)"
    inputUncompress = false
  # Validate -P letter format against sniffed input file format.
  if pipeFmtSet:
    let flagFmt = if pipeFmt in {'z', 'v'}: ffVcf else: ffBcf
    if flagFmt != fmt:
      let inName = if fmt == ffVcf: "VCF" else: "BCF"
      stderr.writeLine "error: -P" & pipeFmt & ": input is " & inName &
        "; format conversion is the pipeline's responsibility"
      quit(1)
  # Set gExpectedOutputFmt for runtime validation of pipeline output format.
  if outputFmtSet:
    gExpectedOutputFmt = outputFmt
  let (_, stages, termOp) = parseRunArgv(rawArgs)
  let hasBrace             = hasBracePlaceholder(stages)

  case termOp
  of topConcat:
    let isStdout = (outPrefix == "" or outPrefix == "/dev/stdout")
    var (gFmt, gComp) = inferFileFormat(outPrefix, "")
    if outputFmtSet:
      let flagComp = letterComp(outputFmt)
      if isStdout and outputFmt in {'z', 'b'}:
        stderr.writeLine "warning: -O" & outputFmt &
          " ignored for stdout output (stdout is always uncompressed)"
      elif not isStdout:
        if letterDecompress(outputFmt) != (gComp == compNone):
          stderr.writeLine "warning: -O" & outputFmt & " disagrees with output extension"
        gComp = flagComp
    let resolvedTmpDir =
      if tmpDir != "": tmpDir
      else: getEnv("TMPDIR", "/tmp") / "vcfparty"
    var cfg = GatherConfig(
      format:      gFmt,
      compression: if isStdout: compNone else: gComp,
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
    runShardsGather(inputFile, nShards, outPrefix, nThreads, forceScan,
                    stages, noKill, cfg, inputUncompress)
  of topCollect:
    let isStdout = (outPrefix == "" or outPrefix == "/dev/stdout")
    if outputFmtSet:
      if isStdout and outputFmt in {'z', 'b'}:
        stderr.writeLine "warning: -O" & outputFmt & " ignored for stdout output"
      elif not isStdout:
        let (_, extComp) = inferFileFormat(outPrefix, "")
        if letterDecompress(outputFmt) != (extComp == compNone):
          stderr.writeLine "warning: -O" & outputFmt & " disagrees with output extension"
    if not isStdout:
      warnFormatMismatch(inputFile, outPrefix)
    runShardsCollect(inputFile, nShards, outPrefix, nThreads, forceScan,
                     stages, noKill, isStdout, inputUncompress)
  of topMerge:
    if outputFmtSet and outputFmt in {'z', 'b'}:
      stderr.writeLine "warning: -O" & outputFmt &
        " ignored for +merge+; merge output is always uncompressed"
    let isStdout = (outPrefix == "" or outPrefix == "/dev/stdout")
    if not isStdout:
      warnFormatMismatch(inputFile, outPrefix)
    runShardsMerge(inputFile, nShards, outPrefix, nThreads, forceScan,
                   stages, noKill, isStdout, inputUncompress)
  of topNone:
    let mode = inferRunMode(outPrefix != "", hasBrace)
    case mode
    of rmToolManaged:
      runShards(inputFile, nShards, outPrefix, nThreads, forceScan,
                stages, noKill, toolManaged = true, inputUncompress)
    of rmNormal:
      warnFormatMismatch(inputFile, outPrefix)
      runShards(inputFile, nShards, outPrefix, nThreads, forceScan,
                stages, noKill, inputUncompress = inputUncompress)

proc gatherUsage() =
  ## Print gather subcommand usage to stderr and exit 1.
  stderr.writeLine "Usage: vcfparty gather [-o <output>] [options] <shard1> [<shard2> ...]"
  stderr.writeLine ""
  stderr.writeLine "Options:"
  stderr.writeLine "  -o, --output <str>           gather output path (default: stdout)"
  stderr.writeLine "      --concat                 concatenate shards in genomic order (default)"
  stderr.writeLine "      --merge                  k-way merge sort output in genomic order"
  stderr.writeLine "  -Oz, -Ov, -Ob, -Ou"
  stderr.writeLine "  -O <z|v|b|u>, --output-type <z|v|b|u>"
  stderr.writeLine "                               output format: z=VCF BGZF, v=VCF, b=BCF BGZF, u=BCF"
  stderr.writeLine "      --header-pattern <pat>   strip lines starting with pat from shards 2..N (text format only)"
  stderr.writeLine "      --header-n <n>           strip the first n lines from shards 2..N (text format only)"
  stderr.writeLine "  -v, --verbose                print progress to stderr"
  stderr.writeLine "  -h, --help                   show this help"
  quit(1)

proc runGather(rawArgs: seq[string]) =
  ## Parse gather subcommand arguments and concatenate pre-existing shard files.
  var outPath: string      = ""
  var outputFmt: char      = '\0'
  var outputFmtSet         = false
  var headerPattern        = ""
  var headerPatternSet     = false
  var headerN              = 0
  var headerNSet           = false
  var useMerge             = false
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
      of "concat":
        discard  # default; accepted and ignored
      of "merge":
        useMerge = true
      of "O", "output-type":
        if outputFmtSet:
          stderr.writeLine "error: only one -O/--output-type flag is allowed"
          quit(1)
        outputFmt    = parseFmtLetter("O", p)
        outputFmtSet = true
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
  let (gFmt, gComp0) = inferFileFormat(outPath, "")
  var gComp = gComp0
  if outputFmtSet:
    let flagComp = letterComp(outputFmt)
    if isStdout and outputFmt in {'z', 'b'}:
      stderr.writeLine "warning: -O" & outputFmt &
        " ignored for stdout output (stdout is always uncompressed)"
    else:
      if letterDecompress(outputFmt) != (gComp == compNone):
        stderr.writeLine "warning: -O" & outputFmt & " disagrees with output extension"
      if not isStdout:
        gComp = flagComp
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
  if useMerge:
    gatherFilesMerge(cfg, inputFiles)
  else:
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
