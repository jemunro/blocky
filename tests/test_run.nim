## Tests for run.nim — argv parsing (R1), buildShellCmd (R2), runShards API (R3),
## CLI integration (R4), inferRunMode (R5), placeholders (R6), tool-managed (R7).
## Run from project root: nim c -r tests/test_run.nim
## Requires: tests/data/small.vcf.gz, tests/data/small.bcf (run generate_fixtures.sh once)

import std/[os, osproc, strformat, strutils]
import "../src/vcfparty/scatter"
import "../src/vcfparty/run"

const DataDir  = "tests/data"
const SmallVcf = DataDir / "small.vcf.gz"
const SmallBcf = DataDir / "small.bcf"

# ===========================================================================
# R1–R6 — parseRunArgv: separator parsing and stage extraction
# ===========================================================================

# ---------------------------------------------------------------------------
# R1 — testSingleStage: single --- stage; vcfparty args and stage tokens correct
# ---------------------------------------------------------------------------
block testSingleStage:
  let argv = @["--shards", "4", "-o", "out", "input.vcf.gz",
               "---", "bcftools", "view", "-Oz"]
  let (pArgs, stages, termOp) = parseRunArgv(argv)
  doAssert pArgs == @["--shards", "4", "-o", "out", "input.vcf.gz"],
    "single stage: wrong vcfparty args"
  doAssert stages.len == 1, "single stage: expected 1 stage, got " & $stages.len
  doAssert stages[0] == @["bcftools", "view", "-Oz"],
    "single stage: wrong stage tokens"
  doAssert termOp == topNone, "single stage: expected topNone terminal op"
  echo "PASS R1.1 parseRunArgv: single stage"

# ---------------------------------------------------------------------------
# R2 — testMultiStage: multi-stage pipeline; inner -- is a plain token
# ---------------------------------------------------------------------------
block testMultiStage:
  let argv = @["-n", "10", "---",
               "bcftools", "+split-vep", "-Ou", "--", "-f", "%SYMBOL",
               "---",
               "bcftools", "view", "-s", "Sample", "-Oz"]
  let (pArgs, stages, _) = parseRunArgv(argv)
  doAssert pArgs == @["-n", "10"], "multi-stage: wrong vcfparty args"
  doAssert stages.len == 2, "multi-stage: expected 2 stages, got " & $stages.len
  doAssert stages[0] == @["bcftools", "+split-vep", "-Ou", "--", "-f", "%SYMBOL"],
    "multi-stage: wrong stage 0 tokens"
  doAssert stages[1] == @["bcftools", "view", "-s", "Sample", "-Oz"],
    "multi-stage: wrong stage 1 tokens"
  echo "PASS R1.2 parseRunArgv: multi-stage pipeline"

# ---------------------------------------------------------------------------
# R3 — testSepFirst: --- at argv[0]; empty partyvcf args, one stage
# ---------------------------------------------------------------------------
block testSepFirst:
  let argv = @["---", "cat"]
  let (pArgs, stages, _) = parseRunArgv(argv)
  doAssert pArgs.len == 0, "sep-first: vcfparty args should be empty"
  doAssert stages.len == 1, "sep-first: expected 1 stage"
  doAssert stages[0] == @["cat"], "sep-first: wrong stage tokens"
  echo "PASS R1.3 parseRunArgv: --- first, no vcfparty args"

# ---------------------------------------------------------------------------
# R4 — testDashDashPassthrough: -- inside stage is a plain token, not a separator
# ---------------------------------------------------------------------------
block testDashDashPassthrough:
  let argv = @["---", "bcftools", "+fill-tags", "-Ou", "--", "-t", "AF"]
  let (_, stages, _) = parseRunArgv(argv)
  doAssert stages.len == 1, "dash-dash: expected 1 stage"
  doAssert stages[0] == @["bcftools", "+fill-tags", "-Ou", "--", "-t", "AF"],
    "dash-dash: -- should be a plain token"
  echo "PASS R1.4 parseRunArgv: -- inside stage is a plain token"

# ---------------------------------------------------------------------------
# R5 — testColonColonSep: ::: is an alias for ---; behaviour identical
# ---------------------------------------------------------------------------
block testColonColonSep:
  let argv = @["-n", "4", ":::", "bcftools", "view", "-Oz"]
  let (pArgs, stages, _) = parseRunArgv(argv)
  doAssert pArgs == @["-n", "4"], "::: sep: wrong vcfparty args"
  doAssert stages.len == 1, "::: sep: expected 1 stage"
  doAssert stages[0] == @["bcftools", "view", "-Oz"], "::: sep: wrong stage tokens"
  echo "PASS R1.5 parseRunArgv: ::: is alias for ---"

# ---------------------------------------------------------------------------
# R6 — testMixedSeparators: --- and ::: may be mixed freely in one invocation
# ---------------------------------------------------------------------------
block testMixedSeparators:
  let argv = @["---", "cmd1", "a", ":::", "cmd2", "b", "---", "cmd3", "c"]
  let (_, stages, _) = parseRunArgv(argv)
  doAssert stages.len == 3, "mixed seps: expected 3 stages, got " & $stages.len
  doAssert stages[0] == @["cmd1", "a"], "mixed: wrong stage 0"
  doAssert stages[1] == @["cmd2", "b"], "mixed: wrong stage 1"
  doAssert stages[2] == @["cmd3", "c"], "mixed: wrong stage 2"
  echo "PASS R1.6 parseRunArgv: ::: and --- may be mixed"

# ===========================================================================
# R1.7–R1.15 — terminal operator parsing
# ===========================================================================

# ---------------------------------------------------------------------------
# R1.7: +concat+ recognised, stage tokens stop before it
# ---------------------------------------------------------------------------
block testTermOpConcat:
  let argv = @["---", "bcftools", "view", "-Oz", "+concat+"]
  let (_, stages, termOp) = parseRunArgv(argv)
  doAssert termOp == topConcat, "concat: expected topConcat, got " & $termOp
  doAssert stages.len == 1, "concat: expected 1 stage"
  doAssert stages[0] == @["bcftools", "view", "-Oz"],
    "concat: +concat+ must not appear in stage tokens"
  echo "PASS R1.7 parseRunArgv: +concat+ terminal operator"

# ---------------------------------------------------------------------------
# R1.8: +merge+ recognised
# ---------------------------------------------------------------------------
block testTermOpMerge:
  let argv = @["---", "cat", "+merge+"]
  let (_, stages, termOp) = parseRunArgv(argv)
  doAssert termOp == topMerge, "merge: expected topMerge"
  doAssert stages[0] == @["cat"], "merge: stage should be just [cat]"
  echo "PASS R1.8 parseRunArgv: +merge+ terminal operator"

# ---------------------------------------------------------------------------
# R1.9: +collect+ recognised
# ---------------------------------------------------------------------------
block testTermOpCollect:
  let argv = @["---", "cat", "+collect+"]
  let (_, stages, termOp) = parseRunArgv(argv)
  doAssert termOp == topCollect, "collect: expected topCollect"
  doAssert stages[0] == @["cat"], "collect: stage should be just [cat]"
  echo "PASS R1.9 parseRunArgv: +collect+ terminal operator"

# ---------------------------------------------------------------------------
# R1.10: terminal op after multi-stage pipeline
# ---------------------------------------------------------------------------
block testTermOpMultiStage:
  let argv = @["---", "cmd1", "---", "cmd2", "+concat+"]
  let (_, stages, termOp) = parseRunArgv(argv)
  doAssert termOp == topConcat, "multi+concat: expected topConcat"
  doAssert stages.len == 2, "multi+concat: expected 2 stages"
  doAssert stages[0] == @["cmd1"], "multi+concat: wrong stage 0"
  doAssert stages[1] == @["cmd2"], "multi+concat: wrong stage 1"
  echo "PASS R1.10 parseRunArgv: terminal op after multi-stage pipeline"

# ---------------------------------------------------------------------------
# R1.11: no terminal op → topNone (backward compat)
# ---------------------------------------------------------------------------
block testTermOpNone:
  let argv = @["---", "cat"]
  let (_, _, termOp) = parseRunArgv(argv)
  doAssert termOp == topNone, "no-op: expected topNone"
  echo "PASS R1.11 parseRunArgv: no terminal operator → topNone"

# ---------------------------------------------------------------------------
# R1.12: toTerminalOp helper
# ---------------------------------------------------------------------------
block testToTerminalOp:
  doAssert toTerminalOp("+concat+")  == topConcat,  "toTerminalOp +concat+"
  doAssert toTerminalOp("+merge+")   == topMerge,   "toTerminalOp +merge+"
  doAssert toTerminalOp("+collect+") == topCollect, "toTerminalOp +collect+"
  doAssert toTerminalOp("cat")       == topNone,    "toTerminalOp plain token"
  doAssert toTerminalOp("+CONCAT+")  == topNone,    "toTerminalOp case sensitive"
  doAssert toTerminalOp("")          == topNone,    "toTerminalOp empty"
  echo "PASS R1.12 toTerminalOp: all cases"

const BinPathEarly = "./vcfparty"

# ---------------------------------------------------------------------------
# R1.13: multiple terminal operators → exit 1
# ---------------------------------------------------------------------------
block testTermOpMultipleError:
  let (outp, code) = execCmdEx(
    BinPathEarly & " run -n 1 " & SmallVcf &
    " ::: cat +concat+ +merge+ 2>&1")
  doAssert code != 0, "multiple terminal ops should exit non-zero"
  doAssert "multiple terminal operators" in outp,
    "expected 'multiple terminal operators' in output, got:\n" & outp
  echo "PASS R1.13 parseRunArgv: multiple terminal operators → error"

# ---------------------------------------------------------------------------
# R1.14: tokens after terminal operator → exit 1
# ---------------------------------------------------------------------------
block testTermOpTrailingTokenError:
  let (outp, code) = execCmdEx(
    BinPathEarly & " run -n 1 " & SmallVcf &
    " ::: cat +concat+ extra 2>&1")
  doAssert code != 0, "tokens after terminal op should exit non-zero"
  doAssert "unexpected tokens after" in outp,
    "expected 'unexpected tokens after' in output, got:\n" & outp
  echo "PASS R1.14 parseRunArgv: tokens after terminal operator → error"

# ---------------------------------------------------------------------------
# R1.15: topNone with no -o and no {} → exit 1 (no output specified)
# ---------------------------------------------------------------------------
block testTopNoneNoOutputError:
  let (outp, code) = execCmdEx(
    BinPathEarly & " run -n 1 " & SmallVcf &
    " ::: cat 2>&1")
  doAssert code != 0, "no -o and no {} should exit non-zero"
  doAssert "no output" in outp.toLowerAscii or "output" in outp.toLowerAscii,
    "expected output-related error, got:\n" & outp
  echo "PASS R1.15 CLI run: no -o and no {} → error"

# ===========================================================================
# R2 — buildShellCmd: stage list to shell command string
# ===========================================================================

# ---------------------------------------------------------------------------
# R2.1 — testBuildSingleStage: single stage produces no pipes
# ---------------------------------------------------------------------------
block testBuildSingleStage:
  let cmd = buildShellCmd(@[@["bcftools", "view", "-Oz"]])
  doAssert cmd == "bcftools view -Oz", "build single: got " & cmd
  echo "PASS R2.1 buildShellCmd: single stage"

# ---------------------------------------------------------------------------
# R2.2 — testBuildMultiStage: two stages joined with |
# ---------------------------------------------------------------------------
block testBuildMultiStage:
  let cmd = buildShellCmd(@[
    @["bcftools", "+split-vep", "-Ou", "--", "-f", "%SYMBOL"],
    @["bcftools", "view", "-s", "Sample", "-Oz"]
  ])
  doAssert cmd == "bcftools +split-vep -Ou -- -f %SYMBOL | bcftools view -s Sample -Oz",
    "build multi: got " & cmd
  echo "PASS R2.2 buildShellCmd: multi-stage pipeline"

# ---------------------------------------------------------------------------
# R2.3 — testBuildThreeStages: three stages produce two pipes
# ---------------------------------------------------------------------------
block testBuildThreeStages:
  let cmd = buildShellCmd(@[@["cmd1", "a"], @["cmd2", "b"], @["cmd3", "c"]])
  doAssert cmd == "cmd1 a | cmd2 b | cmd3 c", "build three: got " & cmd
  echo "PASS R2.3 buildShellCmd: three stages"

# ===========================================================================
# R3 — runShards unit tests (direct API calls, no binary)
# ===========================================================================

proc countRecords(path: string): int =
  let (o, _) = execCmdEx("bcftools view -HG " & path & " 2>/dev/null | wc -l")
  o.strip.parseInt

proc recordsHash(paths: seq[string]): string =
  ## Concatenate records from paths in order (bcftools view -H, full genotypes),
  ## write to temp file, return sha256sum hex digest.
  let tmp = getTempDir() / "vcfparty_hash_" & $getCurrentProcessId() & ".txt"
  var f = open(tmp, fmWrite)
  for p in paths:
    let (o, _) = execCmdEx("bcftools view -H " & p & " 2>/dev/null")
    f.write(o)
  f.close()
  let (h, _) = execCmdEx("sha256sum " & tmp)
  removeFile(tmp)
  h.split(" ")[0]

# ---------------------------------------------------------------------------
# R3.1 — testRunSingle1Shard: 1 shard, cat pipeline; output valid, record count matches
# ---------------------------------------------------------------------------
block testRunSingle1Shard:
  doAssert fileExists(SmallVcf), "fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "vcfparty_run_r10_1shard"
  createDir(tmpDir)
  let tmpl = tmpDir / "out.{}.vcf.gz"
  runShards(SmallVcf, 1, tmpl, 1, false, @[@["cat"]])
  let outPath = shardOutputPath(tmpl, 0, 1)
  doAssert fileExists(outPath), "output missing: " & outPath
  let (_, bcCode) = execCmdEx("bcftools view -HG " & outPath & " > /dev/null 2>&1")
  doAssert bcCode == 0, "bcftools rejected 1-shard run output"
  let orig = countRecords(SmallVcf)
  let got  = countRecords(outPath)
  doAssert got == orig, &"1-shard record count mismatch: got {got}, expected {orig}"
  removeDir(tmpDir)
  echo &"PASS R3.1 runShards: 1 shard, cat stage, {orig} records"

# ---------------------------------------------------------------------------
# R3.2 — testRun4Shards: 4 shards concurrent; all valid, record count + content hash match
# ---------------------------------------------------------------------------
block testRun4Shards:
  doAssert fileExists(SmallVcf), "fixture missing"
  let tmpDir = getTempDir() / "vcfparty_run_r11_4shards"
  createDir(tmpDir)
  let tmpl = tmpDir / "out.{}.vcf.gz"
  runShards(SmallVcf, 4, tmpl, 1, false, @[@["cat"]])
  var total = 0
  var shardPaths: seq[string]
  for i in 0..3:
    let p = shardOutputPath(tmpl, i, 4)
    doAssert fileExists(p), &"shard {i+1} missing"
    let (_, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
    doAssert bc == 0, &"bcftools rejected shard {i+1}"
    total += countRecords(p)
    shardPaths.add(p)
  let orig = countRecords(SmallVcf)
  doAssert total == orig, &"4-shard record count mismatch: {total} vs {orig}"
  doAssert recordsHash(shardPaths) == recordsHash(@[SmallVcf]),
    "4-shard content hash mismatch: record corruption or reordering detected"
  removeDir(tmpDir)
  echo &"PASS R3.2 runShards: 4 shards concurrent, {total} records, content hash matches"

# ---------------------------------------------------------------------------
# R3.3 — testBcfRun4Shards: BCF runShards 4 shards, bcftools view -Ob; hash matches
# ---------------------------------------------------------------------------
block testBcfRun4Shards:
  doAssert fileExists(SmallBcf), "BCF fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "vcfparty_run_r14_bcf4"
  createDir(tmpDir)
  let tmpl = tmpDir / "out.{}.bcf"
  runShards(SmallBcf, 4, tmpl, 1, false, @[@["bcftools", "view", "-Ob"]])
  var total = 0
  var bcfShardPaths: seq[string]
  for i in 0..3:
    let p = shardOutputPath(tmpl, i, 4)
    doAssert fileExists(p), &"BCF shard {i+1} missing: {p}"
    let (_, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
    doAssert bc == 0, &"bcftools rejected BCF shard {i+1}"
    total += countRecords(p)
    bcfShardPaths.add(p)
  let orig = countRecords(SmallBcf)
  doAssert total == orig, &"BCF 4-shard record count mismatch: {total} vs {orig}"
  doAssert recordsHash(bcfShardPaths) == recordsHash(@[SmallBcf]),
    "BCF 4-shard content hash mismatch: record corruption or reordering detected"
  removeDir(tmpDir)
  echo &"PASS R3.3 runShards BCF: 4 shards, bcftools view -Ob, {total} records, content hash matches"

# ===========================================================================
# R4 — CLI integration tests via the compiled binary
# ===========================================================================

const BinPath = "./vcfparty"

block buildBinary:
  if not fileExists(BinPath):
    let (outp, code) = execCmdEx("nimble build 2>&1")
    if code != 0:
      echo "nimble build failed:\n", outp
      quit(1)
  doAssert fileExists(BinPath), "binary not found: " & BinPath & " (run nimble build)"
  echo "PASS R4.0 binary available (run CLI tests)"

proc runBin(args: string): (string, int) =
  execCmdEx(BinPath & " run " & args & " 2>&1")

# ---------------------------------------------------------------------------
# R4.1 — testCliRunMissingSep: no --- exits 1 with '---' in error message
# ---------------------------------------------------------------------------
block testCliRunMissingSep:
  let (outp, code) = runBin(&"-n 1 -o /tmp/x {SmallVcf}")
  doAssert code != 0, "missing --- should exit non-zero"
  doAssert "---" in outp, &"error should mention '---', got:\n{outp}"
  echo "PASS R4.1 CLI run: missing --- → exits 1 with message"

# ---------------------------------------------------------------------------
# R4.2 — testCliRun1Shard: run -n 1 --- cat; output valid, record count matches
# ---------------------------------------------------------------------------
block testCliRun1Shard:
  let tmpDir = getTempDir() / "vcfparty_r6_1shard"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n 1 -o {outp_template} {SmallVcf} --- cat")
  doAssert code == 0, &"run -n 1 exited {code}:\n{outp}"
  let p = tmpDir / "shard_1.out.vcf.gz"
  doAssert fileExists(p), "output missing: " & p
  let (_, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
  doAssert bc == 0, "bcftools rejected 1-shard run output"
  let orig = countRecords(SmallVcf)
  doAssert countRecords(p) == orig, "1-shard CLI record count mismatch"
  removeDir(tmpDir)
  echo &"PASS R4.2 CLI run: 1 shard, cat stage, {orig} records"

# ---------------------------------------------------------------------------
# R4.3 — testCliRun4Shards: run -n 4 --- cat; 4 shards valid, record count matches
# ---------------------------------------------------------------------------
block testCliRun4Shards:
  let tmpDir = getTempDir() / "vcfparty_r7_4shards"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n 4 -o {outp_template} {SmallVcf} --- cat")
  doAssert code == 0, &"run -n 4 exited {code}:\n{outp}"
  var total = 0
  for i in 1..4:
    let p = tmpDir / ("shard_" & $i & ".out.vcf.gz")
    doAssert fileExists(p), &"shard {i} missing"
    let (_, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
    doAssert bc == 0, &"bcftools rejected shard {i}"
    total += countRecords(p)
  doAssert total == countRecords(SmallVcf), &"4-shard CLI record count mismatch: {total}"
  removeDir(tmpDir)
  echo &"PASS R4.3 CLI run: 4 shards, cat stage, {total} records"

# ---------------------------------------------------------------------------
# R4.4 — testCliRunMultiStage: run -n 2 --- cat --- cat; record count matches
# ---------------------------------------------------------------------------
block testCliRunMultiStage:
  let tmpDir = getTempDir() / "vcfparty_r8_multistage"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n 2 -o {outp_template} {SmallVcf} --- cat --- cat")
  doAssert code == 0, &"run multi-stage exited {code}:\n{outp}"
  var total = 0
  for i in 1..2:
    let p = tmpDir / ("shard_" & $i & ".out.vcf.gz")
    doAssert fileExists(p), &"multi-stage shard {i} missing"
    total += countRecords(p)
  doAssert total == countRecords(SmallVcf), "multi-stage record count mismatch"
  removeDir(tmpDir)
  echo "PASS R4.4 CLI run: multi-stage pipeline (cat | cat), records match"

# ---------------------------------------------------------------------------
# R4.5 — testCliRunJError: -j is unknown, exits non-zero
# ---------------------------------------------------------------------------
block testCliRunJError:
  let tmpDir = getTempDir() / "vcfparty_r19_jerror"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (_, code) = runBin(&"-n 4 -j 1 -o {outp_template} {SmallVcf} --- cat")
  doAssert code != 0, "-j should cause a non-zero exit (unknown option)"
  removeDir(tmpDir)
  echo "PASS R4.5 CLI run: -j exits non-zero (unknown option)"

# ---------------------------------------------------------------------------
# R4.6 — testCliRunAttachedN: -n4 (attached, no space) parsed correctly
# ---------------------------------------------------------------------------
block testCliRunAttachedN:
  # Nim's parseopt splits -n4 correctly; verify attached-value still works without -j.
  let tmpDir = getTempDir() / "vcfparty_r20_attached"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n4 -o {outp_template} {SmallVcf} --- cat")
  doAssert code == 0, &"run -n4 exited {code}:\n{outp}"
  var total = 0
  for i in 1..4:
    let p = tmpDir / ("shard_" & $i & ".out.vcf.gz")
    doAssert fileExists(p), &"-n4 shard {i} missing"
    total += countRecords(p)
  doAssert total == countRecords(SmallVcf), "-n4 record count mismatch"
  removeDir(tmpDir)
  echo "PASS R4.6 CLI run: -n4 attached-value flag works"

# ---------------------------------------------------------------------------
# R4.7 — testCliRunMaxJobsError: --max-jobs is unknown, exits non-zero
# ---------------------------------------------------------------------------
block testCliRunMaxJobsError:
  let tmpDir = getTempDir() / "vcfparty_r21_maxjobserror"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (_, code) = runBin(&"-n 2 --max-jobs 10 -o {outp_template} {SmallVcf} --- cat")
  doAssert code != 0, "--max-jobs should cause a non-zero exit (unknown option)"
  removeDir(tmpDir)
  echo "PASS R4.7 CLI run: --max-jobs exits non-zero (unknown option)"

# ---------------------------------------------------------------------------
# R4.8 — testCliRunNonZeroExit: non-zero stage exit → vcfparty exits 1, shard mentioned
# ---------------------------------------------------------------------------
block testCliRunNonZeroExit:
  let tmpDir = getTempDir() / "vcfparty_r2_fail"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n 1 -o {outp_template} {SmallVcf} --- false")
  doAssert code != 0, "non-zero stage should make vcfparty exit non-zero"
  doAssert "shard 1" in outp, &"stderr should mention 'shard 1', got:\n{outp}"
  removeDir(tmpDir)
  echo "PASS R4.8 CLI run: non-zero stage exit → vcfparty exits 1, shard mentioned"

# ---------------------------------------------------------------------------
# R4.9 — testCliRunColonColon: ::: separator works at CLI level
# ---------------------------------------------------------------------------
block testCliRunColonColon:
  let tmpDir = getTempDir() / "vcfparty_r3_coloncolon"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n 2 -o {outp_template} {SmallVcf} ::: cat")
  doAssert code == 0, &"::: separator exited {code}:\n{outp}"
  for i in 1..2:
    let p = tmpDir / ("shard_" & $i & ".out.vcf.gz")
    doAssert fileExists(p), &"::: shard {i} missing"
  removeDir(tmpDir)
  echo "PASS R4.9 CLI run: ::: separator works like ---"

# ---------------------------------------------------------------------------
# R4.10 — testCliRunDashDashPassthrough: -- inside stage passed through to tool
# ---------------------------------------------------------------------------
block testCliRunDashDashPassthrough:
  # bcftools view -Oz -- - reads stdin, writes bgzipped VCF to stdout.
  let tmpDir = getTempDir() / "vcfparty_r4_dashdash"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n 1 -o {outp_template} {SmallVcf} --- bcftools view -Oz -- -")
  doAssert code == 0, &"-- passthrough exited {code}:\n{outp}"
  let p = tmpDir / "shard_1.out.vcf.gz"
  doAssert fileExists(p), "-- passthrough output missing"
  doAssert countRecords(p) == countRecords(SmallVcf), "-- passthrough record count mismatch"
  removeDir(tmpDir)
  echo "PASS R4.10 CLI run: -- inside stage passed through to bcftools"

# ---------------------------------------------------------------------------
# R4.11 — testCliRunNoKill: --no-kill flag accepted; run completes normally
# ---------------------------------------------------------------------------
block testCliRunNoKill:
  # Smoke test: verify --no-kill parses and the run succeeds when all shards
  # succeed.  Behavioural difference (siblings not killed on failure) is hard to
  # assert deterministically in a unit test.
  let tmpDir = getTempDir() / "vcfparty_r5_nokill"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n 2 --no-kill -o {outp_template} {SmallVcf} --- cat")
  doAssert code == 0, &"run --no-kill exited {code}:\n{outp}"
  for i in 1..2:
    doAssert fileExists(tmpDir / ("shard_" & $i & ".out.vcf.gz")), &"--no-kill shard {i} missing"
  removeDir(tmpDir)
  echo "PASS R4.11 CLI run: --no-kill flag accepted, all shards complete normally"

# ---------------------------------------------------------------------------
# R4.12 — testCliRunConcat: +concat+ gathers N shards into a single output file
# ---------------------------------------------------------------------------
block testCliRunConcat:
  let tmpDir = getTempDir() / "vcfparty_r26_concat"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n 4 -o {outFile} {SmallVcf} ::: cat +concat+")
  doAssert code == 0, &"+concat+ exited {code}:\n{outp}"
  doAssert fileExists(outFile), "+concat+ output file missing"
  let orig = countRecords(SmallVcf)
  doAssert countRecords(outFile) == orig, "+concat+ record count mismatch"
  removeDir(tmpDir)
  echo &"PASS R4.12 CLI run: +concat+ gathers 4 shards into single file, {orig} records"

# ---------------------------------------------------------------------------
# R4.13 — testCliRunConcatStdout: +concat+ without -o writes all records to stdout
# ---------------------------------------------------------------------------
block testCliRunConcatStdout:
  let (outp, code) = execCmdEx(
    BinPath & " run -n 2 " & SmallVcf & " ::: cat +concat+ 2>/dev/null | bcftools view -H | wc -l")
  doAssert code == 0, &"+concat+ stdout exited {code}"
  let nRecs = outp.strip().parseInt
  let orig  = countRecords(SmallVcf)
  doAssert nRecs == orig, &"+concat+ stdout record count {nRecs} != {orig}"
  echo &"PASS R4.13 CLI run: +concat+ stdout, {nRecs} records"

# ===========================================================================
# R5 — inferRunMode: non-error cases
# (The error case — no output, no {} — calls quit(1) and is covered at the
#  CLI level by testCliRunMissingSep.)
# ===========================================================================

block testInferNormal:
  let m = inferRunMode(true, false)
  doAssert m == rmNormal, "expected rmNormal, got " & $m
  echo "PASS R5.1 inferRunMode: o / no-{} → rmNormal"

block testInferToolManaged:
  let m = inferRunMode(false, true)
  doAssert m == rmToolManaged, "expected rmToolManaged, got " & $m
  echo "PASS R5.2 inferRunMode: no-o / {} → rmToolManaged"

block testInferToolManagedWithO:
  # -o present but {} in cmd: -o ignored, still tool-managed (warning emitted)
  let m = inferRunMode(true, true)
  doAssert m == rmToolManaged, "expected rmToolManaged, got " & $m
  echo "PASS R5.3 inferRunMode: o / {} → rmToolManaged (warning: -o ignored)"

# ===========================================================================
# R6 — hasBracePlaceholder, substituteToken, buildShellCmdForShard
# ===========================================================================

block testHasBraceTrue:
  doAssert hasBracePlaceholder(@[@["bcftools", "view", "-o", "out.{}.vcf.gz"]]),
    "expected true for token containing {}"
  echo "PASS R6.1 hasBracePlaceholder: detects {} in token"

block testHasBraceFalse:
  doAssert not hasBracePlaceholder(@[@["bcftools", "view", "-Oz"]]),
    "expected false when no {} present"
  echo "PASS R6.2 hasBracePlaceholder: false when no {} present"

block testHasBraceEscaped:
  # \{} is escaped — should NOT count as a placeholder
  doAssert not hasBracePlaceholder(@[@["tool", "\\{}"  ]]),
    "expected false for escaped \\{}"
  echo "PASS R6.3 hasBracePlaceholder: escaped \\{} does not count"

block testSubstituteBasic:
  let r = substituteToken("out.{}.vcf.gz", "03")
  doAssert r == "out.03.vcf.gz", "basic sub: got " & r
  echo "PASS R6.4 substituteToken: basic {} substitution"

block testSubstituteMultiple:
  let r = substituteToken("{}-{}", "05")
  doAssert r == "05-05", "multiple: got " & r
  echo "PASS R6.5 substituteToken: multiple {} replaced"

block testSubstituteEscaped:
  # \{} should become literal {} in the output (backslash consumed)
  let r = substituteToken("\\{}", "07")
  doAssert r == "{}", "escaped: got " & r
  echo "PASS R6.6 substituteToken: \\{} → literal {}"

block testSubstituteNoBrace:
  let r = substituteToken("bcftools", "02")
  doAssert r == "bcftools", "no-brace: got " & r
  echo "PASS R6.7 substituteToken: no {} → unchanged"

block testSubstituteEscapedAndUnescaped:
  # \{} followed by {} → literal {} then shardNum
  let r = substituteToken("\\{}{}", "04")
  doAssert r == "{}04", "mixed escape: got " & r
  echo "PASS R6.8 substituteToken: \\{} then {} → literal {} then shardNum"

block testBuildShellCmdForShardPadding:
  # nShards = 10 → width 2; shardIdx = 0 → "01"
  let cmd = buildShellCmdForShard(@[@["bcftools", "view", "-o", "out.{}.vcf.gz"]], 0, 10)
  doAssert "out.01.vcf.gz" in cmd, "padding w2: got " & cmd
  echo "PASS R6.9 buildShellCmdForShard: zero-pads to width 2 for nShards=10"

block testBuildShellCmdForShardNoBrace:
  # No {} → cmd unchanged (aside from quoting)
  let cmd = buildShellCmdForShard(@[@["cat"]], 0, 4)
  doAssert cmd == "cat", "no-brace: got " & cmd
  echo "PASS R6.10 buildShellCmdForShard: no {} → cmd unchanged"

block testBuildShellCmdForShardWidthOne:
  # nShards = 4 → width 1; shardIdx = 2 → "3"
  let cmd = buildShellCmdForShard(@[@["tool", "{}"]], 2, 4)
  doAssert "3" in cmd, "width-1: got " & cmd
  doAssert "03" notin cmd, "width-1: should not be zero-padded: got " & cmd
  echo "PASS R6.11 buildShellCmdForShard: width 1 for nShards=4"

# ===========================================================================
# R7 — tool-managed mode integration tests
# ===========================================================================

# ---------------------------------------------------------------------------
# R7.1: tool-managed mode via API (runShards toolManaged=true)
# ---------------------------------------------------------------------------
block testToolManagedApiDirect:
  doAssert fileExists(SmallVcf), "fixture missing"
  let tmpDir = getTempDir() / "vcfparty_t3_1_tool_api"
  createDir(tmpDir)
  let outTemplate = tmpDir / "out.{}.vcf.gz"
  # Tool writes its own output using {} substitution.
  # We use bcftools view -Oz -o <path> rather than stdout.
  let stages = @[@["bcftools", "view", "-Oz", "-o", outTemplate]]
  runShards(SmallVcf, 2, "", 1, false, stages, toolManaged = true)
  var total = 0
  for i in 0..1:
    let p = shardOutputPath(outTemplate, i, 2)
    doAssert fileExists(p), &"tool-managed shard {i+1} missing: {p}"
    let (_, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
    doAssert bc == 0, &"bcftools rejected tool-managed shard {i+1}"
    total += countRecords(p)
  doAssert total == countRecords(SmallVcf),
    &"tool-managed record count mismatch: {total}"
  removeDir(tmpDir)
  echo &"PASS R7.1 tool-managed API: 2 shards, tool writes own output, {total} records"

# ---------------------------------------------------------------------------
# R7.2: tool-managed CLI — {} in tool cmd, no -o → tool writes files
# ---------------------------------------------------------------------------
block testToolManagedCli:
  doAssert fileExists(SmallVcf), "fixture missing"
  let tmpDir = getTempDir() / "vcfparty_t3_2_tool_cli"
  createDir(tmpDir)
  let outTemplate = tmpDir / "out.{}.vcf.gz"
  let (outp, code) = execCmdEx(
    BinPath & " run -n 2 " & SmallVcf &
    " ::: bcftools view -Oz -o " & outTemplate & " 2>&1")
  doAssert code == 0, &"tool-managed CLI exited {code}:\n{outp}"
  var total = 0
  for i in 0..1:
    let p = shardOutputPath(outTemplate, i, 2)
    doAssert fileExists(p), &"tool-managed CLI shard {i+1} missing: {p}"
    total += countRecords(p)
  doAssert total == countRecords(SmallVcf),
    &"tool-managed CLI record count mismatch: {total}"
  removeDir(tmpDir)
  echo &"PASS R7.2 tool-managed CLI: {{}} in tool cmd, no -o, {total} records"

# ---------------------------------------------------------------------------
# R7.3: tool-managed CLI — {} in tool cmd, -o present → warning + tool-managed
# ---------------------------------------------------------------------------
block testToolManagedCliWithO:
  doAssert fileExists(SmallVcf), "fixture missing"
  let tmpDir = getTempDir() / "vcfparty_t3_3_tool_cli_o"
  createDir(tmpDir)
  let outTemplate = tmpDir / "out.{}.vcf.gz"
  let (outp, code) = execCmdEx(
    BinPath & " run -n 2 -o " & tmpDir / "ignored.vcf.gz" &
    " " & SmallVcf &
    " ::: bcftools view -Oz -o " & outTemplate & " 2>&1")
  doAssert code == 0, &"tool-managed -o CLI exited {code}:\n{outp}"
  doAssert "warning" in outp.toLowerAscii, &"expected warning about -o ignored:\n{outp}"
  for i in 0..1:
    let p = shardOutputPath(outTemplate, i, 2)
    doAssert fileExists(p), &"tool-managed -o shard {i+1} missing"
  doAssert not fileExists(tmpDir / "ignored.vcf.gz"), "-o file should not be created"
  removeDir(tmpDir)
  echo "PASS R7.3 tool-managed CLI: -o present with {} → warning, -o ignored"

