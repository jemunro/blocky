## Tests for run.nim — argv parsing (R1–R9), pipe execution and worker pool (R10–R14),
## and CLI integration (R15–R25).
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
  let (pArgs, stages) = parseRunArgv(argv)
  doAssert pArgs == @["--shards", "4", "-o", "out", "input.vcf.gz"],
    "single stage: wrong vcfparty args"
  doAssert stages.len == 1, "single stage: expected 1 stage, got " & $stages.len
  doAssert stages[0] == @["bcftools", "view", "-Oz"],
    "single stage: wrong stage tokens"
  echo "PASS parseRunArgv: single stage"

# ---------------------------------------------------------------------------
# R2 — testMultiStage: multi-stage pipeline; inner -- is a plain token
# ---------------------------------------------------------------------------
block testMultiStage:
  let argv = @["-n", "10", "---",
               "bcftools", "+split-vep", "-Ou", "--", "-f", "%SYMBOL",
               "---",
               "bcftools", "view", "-s", "Sample", "-Oz"]
  let (pArgs, stages) = parseRunArgv(argv)
  doAssert pArgs == @["-n", "10"], "multi-stage: wrong vcfparty args"
  doAssert stages.len == 2, "multi-stage: expected 2 stages, got " & $stages.len
  doAssert stages[0] == @["bcftools", "+split-vep", "-Ou", "--", "-f", "%SYMBOL"],
    "multi-stage: wrong stage 0 tokens"
  doAssert stages[1] == @["bcftools", "view", "-s", "Sample", "-Oz"],
    "multi-stage: wrong stage 1 tokens"
  echo "PASS parseRunArgv: multi-stage pipeline"

# ---------------------------------------------------------------------------
# R3 — testSepFirst: --- at argv[0]; empty paravar args, one stage
# ---------------------------------------------------------------------------
block testSepFirst:
  let argv = @["---", "cat"]
  let (pArgs, stages) = parseRunArgv(argv)
  doAssert pArgs.len == 0, "sep-first: vcfparty args should be empty"
  doAssert stages.len == 1, "sep-first: expected 1 stage"
  doAssert stages[0] == @["cat"], "sep-first: wrong stage tokens"
  echo "PASS parseRunArgv: --- first, no paravar args"

# ---------------------------------------------------------------------------
# R4 — testDashDashPassthrough: -- inside stage is a plain token, not a separator
# ---------------------------------------------------------------------------
block testDashDashPassthrough:
  let argv = @["---", "bcftools", "+fill-tags", "-Ou", "--", "-t", "AF"]
  let (_, stages) = parseRunArgv(argv)
  doAssert stages.len == 1, "dash-dash: expected 1 stage"
  doAssert stages[0] == @["bcftools", "+fill-tags", "-Ou", "--", "-t", "AF"],
    "dash-dash: -- should be a plain token"
  echo "PASS parseRunArgv: -- inside stage is a plain token"

# ---------------------------------------------------------------------------
# R5 — testColonColonSep: ::: is an alias for ---; behaviour identical
# ---------------------------------------------------------------------------
block testColonColonSep:
  let argv = @["-n", "4", ":::", "bcftools", "view", "-Oz"]
  let (pArgs, stages) = parseRunArgv(argv)
  doAssert pArgs == @["-n", "4"], "::: sep: wrong vcfparty args"
  doAssert stages.len == 1, "::: sep: expected 1 stage"
  doAssert stages[0] == @["bcftools", "view", "-Oz"], "::: sep: wrong stage tokens"
  echo "PASS parseRunArgv: ::: is alias for ---"

# ---------------------------------------------------------------------------
# R6 — testMixedSeparators: --- and ::: may be mixed freely in one invocation
# ---------------------------------------------------------------------------
block testMixedSeparators:
  let argv = @["---", "cmd1", "a", ":::", "cmd2", "b", "---", "cmd3", "c"]
  let (_, stages) = parseRunArgv(argv)
  doAssert stages.len == 3, "mixed seps: expected 3 stages, got " & $stages.len
  doAssert stages[0] == @["cmd1", "a"], "mixed: wrong stage 0"
  doAssert stages[1] == @["cmd2", "b"], "mixed: wrong stage 1"
  doAssert stages[2] == @["cmd3", "c"], "mixed: wrong stage 2"
  echo "PASS parseRunArgv: ::: and --- may be mixed"

# ===========================================================================
# R7–R9 — buildShellCmd: stage list to shell command string
# ===========================================================================

# ---------------------------------------------------------------------------
# R7 — testBuildSingleStage: single stage produces no pipes
# ---------------------------------------------------------------------------
block testBuildSingleStage:
  let cmd = buildShellCmd(@[@["bcftools", "view", "-Oz"]])
  doAssert cmd == "bcftools view -Oz", "build single: got " & cmd
  echo "PASS buildShellCmd: single stage"

# ---------------------------------------------------------------------------
# R8 — testBuildMultiStage: two stages joined with |
# ---------------------------------------------------------------------------
block testBuildMultiStage:
  let cmd = buildShellCmd(@[
    @["bcftools", "+split-vep", "-Ou", "--", "-f", "%SYMBOL"],
    @["bcftools", "view", "-s", "Sample", "-Oz"]
  ])
  doAssert cmd == "bcftools +split-vep -Ou -- -f %SYMBOL | bcftools view -s Sample -Oz",
    "build multi: got " & cmd
  echo "PASS buildShellCmd: multi-stage pipeline"

# ---------------------------------------------------------------------------
# R9 — testBuildThreeStages: three stages produce two pipes
# ---------------------------------------------------------------------------
block testBuildThreeStages:
  let cmd = buildShellCmd(@[@["cmd1", "a"], @["cmd2", "b"], @["cmd3", "c"]])
  doAssert cmd == "cmd1 a | cmd2 b | cmd3 c", "build three: got " & cmd
  echo "PASS buildShellCmd: three stages"

# ===========================================================================
# R10–R14 — runShards unit tests (direct API calls, no binary)
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
# R10 — testRunSingle1Shard: 1 shard, cat pipeline; output valid, record count matches
# ---------------------------------------------------------------------------
block testRunSingle1Shard:
  doAssert fileExists(SmallVcf), "fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "vcfparty_run_r10_1shard"
  createDir(tmpDir)
  let tmpl = tmpDir / "out.{}.vcf.gz"
  runShards(SmallVcf, 1, tmpl, 1, false, 1, @[@["cat"]])
  let outPath = shardOutputPath(tmpl, 0, 1)
  doAssert fileExists(outPath), "output missing: " & outPath
  let (_, bcCode) = execCmdEx("bcftools view -HG " & outPath & " > /dev/null 2>&1")
  doAssert bcCode == 0, "bcftools rejected 1-shard run output"
  let orig = countRecords(SmallVcf)
  let got  = countRecords(outPath)
  doAssert got == orig, &"1-shard record count mismatch: got {got}, expected {orig}"
  removeDir(tmpDir)
  echo &"PASS runShards: 1 shard, cat stage, {orig} records"

# ---------------------------------------------------------------------------
# R11 — testRun4Shards: 4 shards, -j 4; all valid, record count + content hash match
# ---------------------------------------------------------------------------
block testRun4Shards:
  doAssert fileExists(SmallVcf), "fixture missing"
  let tmpDir = getTempDir() / "vcfparty_run_r11_4shards"
  createDir(tmpDir)
  let tmpl = tmpDir / "out.{}.vcf.gz"
  runShards(SmallVcf, 4, tmpl, 1, false, 4, @[@["cat"]])
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
  echo &"PASS runShards: 4 shards, --jobs 4, {total} records, content hash matches"

# ---------------------------------------------------------------------------
# R12 — testRunJobs1Serial: 4 shards, -j 1; serial execution, record count matches
# ---------------------------------------------------------------------------
block testRunJobs1Serial:
  doAssert fileExists(SmallVcf), "fixture missing"
  let tmpDir = getTempDir() / "vcfparty_run_r12_jobs1"
  createDir(tmpDir)
  let tmpl = tmpDir / "out.{}.vcf.gz"
  runShards(SmallVcf, 4, tmpl, 1, false, 1, @[@["cat"]])  # maxJobs = 1
  var total = 0
  for i in 0..3:
    let p = shardOutputPath(tmpl, i, 4)
    doAssert fileExists(p), &"serial shard {i+1} missing"
    let (_, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
    doAssert bc == 0, &"bcftools rejected serial shard {i+1}"
    total += countRecords(p)
  let orig = countRecords(SmallVcf)
  doAssert total == orig, &"serial record count mismatch: {total} vs {orig}"
  removeDir(tmpDir)
  echo &"PASS runShards: 4 shards, --jobs 1 (serial), {total} records"

# ---------------------------------------------------------------------------
# R13 — testRunJobsMoreThanShards: maxJobs > nShards; completes without hang
# ---------------------------------------------------------------------------
block testRunJobsMoreThanShards:
  doAssert fileExists(SmallVcf), "fixture missing"
  let tmpDir = getTempDir() / "vcfparty_run_r13_jobsover"
  createDir(tmpDir)
  let tmpl = tmpDir / "out.{}.vcf.gz"
  runShards(SmallVcf, 2, tmpl, 1, false, 10, @[@["cat"]])  # 10 jobs for 2 shards
  for i in 0..1:
    doAssert fileExists(shardOutputPath(tmpl, i, 2)), &"over-jobs shard {i+1} missing"
  removeDir(tmpDir)
  echo "PASS runShards: maxJobs > nShards, no hang"

# ---------------------------------------------------------------------------
# R14 — testBcfRun4Shards: BCF runShards 4 shards, bcftools view -Ob; hash matches
# ---------------------------------------------------------------------------
block testBcfRun4Shards:
  doAssert fileExists(SmallBcf), "BCF fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "vcfparty_run_r14_bcf4"
  createDir(tmpDir)
  let tmpl = tmpDir / "out.{}.bcf"
  runShards(SmallBcf, 4, tmpl, 1, false, 4, @[@["bcftools", "view", "-Ob"]])
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
  echo &"PASS runShards BCF: 4 shards, bcftools view -Ob, {total} records, content hash matches"

# ===========================================================================
# R15–R25 — CLI integration tests via the compiled binary
# ===========================================================================

const BinPath = "./vcfparty"

block buildBinary:
  if not fileExists(BinPath):
    let (outp, code) = execCmdEx("nimble build 2>&1")
    if code != 0:
      echo "nimble build failed:\n", outp
      quit(1)
  doAssert fileExists(BinPath), "binary not found: " & BinPath & " (run nimble build)"
  echo "PASS binary available (run CLI tests)"

proc runBin(args: string): (string, int) =
  execCmdEx(BinPath & " run " & args & " 2>&1")

# ---------------------------------------------------------------------------
# R15 — testCliRunMissingSep: no --- exits 1 with '---' in error message
# ---------------------------------------------------------------------------
block testCliRunMissingSep:
  let (outp, code) = runBin(&"-n 1 -o /tmp/x {SmallVcf}")
  doAssert code != 0, "missing --- should exit non-zero"
  doAssert "---" in outp, &"error should mention '---', got:\n{outp}"
  echo "PASS CLI run: missing --- → exits 1 with message"

# ---------------------------------------------------------------------------
# R16 — testCliRun1Shard: run -n 1 --- cat; output valid, record count matches
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
  echo &"PASS CLI run: 1 shard, cat stage, {orig} records"

# ---------------------------------------------------------------------------
# R17 — testCliRun4Shards: run -n 4 --- cat; 4 shards valid, record count matches
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
  echo &"PASS CLI run: 4 shards, cat stage, {total} records"

# ---------------------------------------------------------------------------
# R18 — testCliRunMultiStage: run -n 2 --- cat --- cat; record count matches
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
  echo "PASS CLI run: multi-stage pipeline (cat | cat), records match"

# ---------------------------------------------------------------------------
# R19 — testCliRunJobs1: run -j 1 (serial); 4 shards, record count matches
# ---------------------------------------------------------------------------
block testCliRunJobs1:
  let tmpDir = getTempDir() / "vcfparty_r9_jobs1"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n 4 -j 1 -o {outp_template} {SmallVcf} --- cat")
  doAssert code == 0, &"run -j 1 exited {code}:\n{outp}"
  var total = 0
  for i in 1..4:
    let p = tmpDir / ("shard_" & $i & ".out.vcf.gz")
    doAssert fileExists(p), &"-j 1 shard {i} missing"
    total += countRecords(p)
  doAssert total == countRecords(SmallVcf), "-j 1 record count mismatch"
  removeDir(tmpDir)
  echo "PASS CLI run: -j 1 (serial), records match"

# ---------------------------------------------------------------------------
# R20 — testCliRunAttachedFlags: -n4 -j2 (no space) parsed correctly
# ---------------------------------------------------------------------------
block testCliRunAttachedFlags:
  # Nim's parseopt splits -j2 into two short options; nextVal must recover the
  # digit value when the next token is all-digit.
  let tmpDir = getTempDir() / "vcfparty_r0_attached"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n4 -j2 -o {outp_template} {SmallVcf} --- cat")
  doAssert code == 0, &"run -n4 -j2 exited {code}:\n{outp}"
  var total = 0
  for i in 1..4:
    let p = tmpDir / ("shard_" & $i & ".out.vcf.gz")
    doAssert fileExists(p), &"-n4 -j2 shard {i} missing"
    total += countRecords(p)
  doAssert total == countRecords(SmallVcf), "-n4 -j2 record count mismatch"
  removeDir(tmpDir)
  echo "PASS CLI run: -n4 -j2 attached-value flags work"

# ---------------------------------------------------------------------------
# R21 — testCliRunJobsOver: -j > nShards; completes without hang
# ---------------------------------------------------------------------------
block testCliRunJobsOver:
  let tmpDir = getTempDir() / "vcfparty_r1_jobsover"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n 2 -j 10 -o {outp_template} {SmallVcf} --- cat")
  doAssert code == 0, &"run -j 10 -n 2 exited {code}:\n{outp}"
  for i in 1..2:
    doAssert fileExists(tmpDir / ("shard_" & $i & ".out.vcf.gz")), &"over-jobs shard {i} missing"
  removeDir(tmpDir)
  echo "PASS CLI run: -j > nShards, no hang"

# ---------------------------------------------------------------------------
# R22 — testCliRunNonZeroExit: non-zero stage exit → vcfparty exits 1, shard mentioned
# ---------------------------------------------------------------------------
block testCliRunNonZeroExit:
  let tmpDir = getTempDir() / "vcfparty_r2_fail"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = runBin(&"-n 1 -o {outp_template} {SmallVcf} --- false")
  doAssert code != 0, "non-zero stage should make vcfparty exit non-zero"
  doAssert "shard 1" in outp, &"stderr should mention 'shard 1', got:\n{outp}"
  removeDir(tmpDir)
  echo "PASS CLI run: non-zero stage exit → vcfparty exits 1, shard mentioned"

# ---------------------------------------------------------------------------
# R23 — testCliRunColonColon: ::: separator works at CLI level
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
  echo "PASS CLI run: ::: separator works like ---"

# ---------------------------------------------------------------------------
# R24 — testCliRunDashDashPassthrough: -- inside stage passed through to tool
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
  echo "PASS CLI run: -- inside stage passed through to bcftools"

# ---------------------------------------------------------------------------
# R25 — testCliRunNoKill: --no-kill flag accepted; run completes normally
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
  echo "PASS CLI run: --no-kill flag accepted, all shards complete normally"

# ===========================================================================
# T1 — inferRunMode: 7 non-error cases from the inference table
# (The error case — no output, no {}, no --gather — calls quit(1) so it is
#  covered by R15/testCliRunMissingSep at the CLI level rather than here.)
# ===========================================================================

block testInferGatherStdout:
  let m = inferRunMode(false, false, true)
  doAssert m == rmGatherStdout, "expected rmGatherStdout, got " & $m
  echo "PASS inferRunMode: no-o / no-{} / gather → rmGatherStdout"

block testInferGatherFile:
  let m = inferRunMode(true, false, true)
  doAssert m == rmGatherFile, "expected rmGatherFile, got " & $m
  echo "PASS inferRunMode: o / no-{} / gather → rmGatherFile"

block testInferNormal:
  let m = inferRunMode(true, false, false)
  doAssert m == rmNormal, "expected rmNormal, got " & $m
  echo "PASS inferRunMode: o / no-{} / no-gather → rmNormal"

block testInferToolManaged:
  let m = inferRunMode(false, true, false)
  doAssert m == rmToolManaged, "expected rmToolManaged, got " & $m
  echo "PASS inferRunMode: no-o / {} / no-gather → rmToolManaged"

block testInferToolManagedWithO:
  # -o present but {} in cmd: -o ignored, still tool-managed (warning emitted)
  let m = inferRunMode(true, true, false)
  doAssert m == rmToolManaged, "expected rmToolManaged, got " & $m
  echo "PASS inferRunMode: o / {} / no-gather → rmToolManaged (warning: -o ignored)"

block testInferGatherToolNoO:
  let m = inferRunMode(false, true, true)
  doAssert m == rmGatherTool, "expected rmGatherTool, got " & $m
  echo "PASS inferRunMode: no-o / {} / gather → rmGatherTool"

block testInferGatherToolWithO:
  let m = inferRunMode(true, true, true)
  doAssert m == rmGatherTool, "expected rmGatherTool, got " & $m
  echo "PASS inferRunMode: o / {} / gather → rmGatherTool"

# ===========================================================================
# T2 — hasBracePlaceholder and substituteToken / buildShellCmdForShard
# ===========================================================================

block testHasBraceTrue:
  doAssert hasBracePlaceholder(@[@["bcftools", "view", "-o", "out.{}.vcf.gz"]]),
    "expected true for token containing {}"
  echo "PASS hasBracePlaceholder: detects {} in token"

block testHasBraceFalse:
  doAssert not hasBracePlaceholder(@[@["bcftools", "view", "-Oz"]]),
    "expected false when no {} present"
  echo "PASS hasBracePlaceholder: false when no {} present"

block testHasBraceEscaped:
  # \{} is escaped — should NOT count as a placeholder
  doAssert not hasBracePlaceholder(@[@["tool", "\\{}"  ]]),
    "expected false for escaped \\{}"
  echo "PASS hasBracePlaceholder: escaped \\{} does not count"

block testSubstituteBasic:
  let r = substituteToken("out.{}.vcf.gz", "03")
  doAssert r == "out.03.vcf.gz", "basic sub: got " & r
  echo "PASS substituteToken: basic {} substitution"

block testSubstituteMultiple:
  let r = substituteToken("{}-{}", "05")
  doAssert r == "05-05", "multiple: got " & r
  echo "PASS substituteToken: multiple {} replaced"

block testSubstituteEscaped:
  # \{} should become literal {} in the output (backslash consumed)
  let r = substituteToken("\\{}", "07")
  doAssert r == "{}", "escaped: got " & r
  echo "PASS substituteToken: \\{} → literal {}"

block testSubstituteNoBrace:
  let r = substituteToken("bcftools", "02")
  doAssert r == "bcftools", "no-brace: got " & r
  echo "PASS substituteToken: no {} → unchanged"

block testSubstituteEscapedAndUnescaped:
  # \{} followed by {} → literal {} then shardNum
  let r = substituteToken("\\{}{}", "04")
  doAssert r == "{}04", "mixed escape: got " & r
  echo "PASS substituteToken: \\{} then {} → literal {} then shardNum"

block testBuildShellCmdForShardPadding:
  # nShards = 10 → width 2; shardIdx = 0 → "01"
  let cmd = buildShellCmdForShard(@[@["bcftools", "view", "-o", "out.{}.vcf.gz"]], 0, 10)
  doAssert "out.01.vcf.gz" in cmd, "padding w2: got " & cmd
  echo "PASS buildShellCmdForShard: zero-pads to width 2 for nShards=10"

block testBuildShellCmdForShardNoBrace:
  # No {} → cmd unchanged (aside from quoting)
  let cmd = buildShellCmdForShard(@[@["cat"]], 0, 4)
  doAssert cmd == "cat", "no-brace: got " & cmd
  echo "PASS buildShellCmdForShard: no {} → cmd unchanged"

block testBuildShellCmdForShardWidthOne:
  # nShards = 4 → width 1; shardIdx = 2 → "3"
  let cmd = buildShellCmdForShard(@[@["tool", "{}"]], 2, 4)
  doAssert "3" in cmd, "width-1: got " & cmd
  doAssert "03" notin cmd, "width-1: should not be zero-padded: got " & cmd
  echo "PASS buildShellCmdForShard: width 1 for nShards=4"

# ===========================================================================
# T3 — tool-managed mode integration tests
# ===========================================================================

# ---------------------------------------------------------------------------
# T3-1: tool-managed mode via API (runShards toolManaged=true)
# ---------------------------------------------------------------------------
block testToolManagedApiDirect:
  doAssert fileExists(SmallVcf), "fixture missing"
  let tmpDir = getTempDir() / "vcfparty_t3_1_tool_api"
  createDir(tmpDir)
  let outTemplate = tmpDir / "out.{}.vcf.gz"
  # Tool writes its own output using {} substitution.
  # We use bcftools view -Oz -o <path> rather than stdout.
  let stages = @[@["bcftools", "view", "-Oz", "-o", outTemplate]]
  runShards(SmallVcf, 2, "", 1, false, 2, stages, toolManaged = true)
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
  echo &"PASS tool-managed API: 2 shards, tool writes own output, {total} records"

# ---------------------------------------------------------------------------
# T3-2: tool-managed CLI — {} in tool cmd, no -o → tool writes files
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
  echo &"PASS tool-managed CLI: {{}} in tool cmd, no -o, {total} records"

# ---------------------------------------------------------------------------
# T3-3: tool-managed CLI — {} in tool cmd, -o present → warning + tool-managed
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
  echo "PASS tool-managed CLI: -o present with {} → warning, -o ignored"

echo ""
echo "All run tests passed."
