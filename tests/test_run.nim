## Tests for run.nim — argv parsing (R2), pipe execution and worker pool (R3/R4).
## Error-path tests that call quit(1) are deferred to R5 (tested via binary).
## Run from project root: nim c -r tests/test_run.nim
## Requires: tests/data/small.vcf.gz (run generate_fixtures.sh once)

import std/[os, osproc, strformat, strutils]
import "../src/paravar/run"

const DataDir  = "tests/data"
const SmallVcf = DataDir / "small.vcf.gz"

# ---------------------------------------------------------------------------
# R2 — parseRunArgv happy paths
# ---------------------------------------------------------------------------

block testSingleStage:
  let argv = @["--shards", "4", "-o", "out", "input.vcf.gz",
               "---", "bcftools", "view", "-Oz"]
  let (pArgs, stages) = parseRunArgv(argv)
  doAssert pArgs == @["--shards", "4", "-o", "out", "input.vcf.gz"],
    "single stage: wrong paravar args"
  doAssert stages.len == 1, "single stage: expected 1 stage, got " & $stages.len
  doAssert stages[0] == @["bcftools", "view", "-Oz"],
    "single stage: wrong stage tokens"
  echo "PASS parseRunArgv: single stage"

block testMultiStage:
  let argv = @["-n", "10", "---",
               "bcftools", "+split-vep", "-Ou", "--", "-f", "%SYMBOL",
               "---",
               "bcftools", "view", "-s", "Sample", "-Oz"]
  let (pArgs, stages) = parseRunArgv(argv)
  doAssert pArgs == @["-n", "10"], "multi-stage: wrong paravar args"
  doAssert stages.len == 2, "multi-stage: expected 2 stages, got " & $stages.len
  doAssert stages[0] == @["bcftools", "+split-vep", "-Ou", "--", "-f", "%SYMBOL"],
    "multi-stage: wrong stage 0 tokens"
  doAssert stages[1] == @["bcftools", "view", "-s", "Sample", "-Oz"],
    "multi-stage: wrong stage 1 tokens"
  echo "PASS parseRunArgv: multi-stage pipeline"

block testSepFirst:
  let argv = @["---", "cat"]
  let (pArgs, stages) = parseRunArgv(argv)
  doAssert pArgs.len == 0, "sep-first: paravar args should be empty"
  doAssert stages.len == 1, "sep-first: expected 1 stage"
  doAssert stages[0] == @["cat"], "sep-first: wrong stage tokens"
  echo "PASS parseRunArgv: --- first, no paravar args"

block testDashDashPassthrough:
  let argv = @["---", "bcftools", "+fill-tags", "-Ou", "--", "-t", "AF"]
  let (_, stages) = parseRunArgv(argv)
  doAssert stages.len == 1, "dash-dash: expected 1 stage"
  doAssert stages[0] == @["bcftools", "+fill-tags", "-Ou", "--", "-t", "AF"],
    "dash-dash: -- should be a plain token"
  echo "PASS parseRunArgv: -- inside stage is a plain token"

# ---------------------------------------------------------------------------
# R2 — buildShellCmd
# ---------------------------------------------------------------------------

block testBuildSingleStage:
  let cmd = buildShellCmd(@[@["bcftools", "view", "-Oz"]])
  doAssert cmd == "bcftools view -Oz", "build single: got " & cmd
  echo "PASS buildShellCmd: single stage"

block testBuildMultiStage:
  let cmd = buildShellCmd(@[
    @["bcftools", "+split-vep", "-Ou", "--", "-f", "%SYMBOL"],
    @["bcftools", "view", "-s", "Sample", "-Oz"]
  ])
  doAssert cmd == "bcftools +split-vep -Ou -- -f %SYMBOL | bcftools view -s Sample -Oz",
    "build multi: got " & cmd
  echo "PASS buildShellCmd: multi-stage pipeline"

block testBuildThreeStages:
  let cmd = buildShellCmd(@[@["cmd1", "a"], @["cmd2", "b"], @["cmd3", "c"]])
  doAssert cmd == "cmd1 a | cmd2 b | cmd3 c", "build three: got " & cmd
  echo "PASS buildShellCmd: three stages"

# ---------------------------------------------------------------------------
# R3 — single-shard pipe execution
# ---------------------------------------------------------------------------

proc countRecords(path: string): int =
  let (o, _) = execCmdEx("bcftools view -HG " & path & " 2>/dev/null | wc -l")
  o.strip.parseInt

block testRunSingle1Shard:
  doAssert fileExists(SmallVcf), "fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "paravar_run_r3_1shard"
  createDir(tmpDir)
  let prefix = tmpDir / "out"
  runShards(SmallVcf, 1, prefix, 1, false, 1, "cat")
  let outPath = prefix & ".1.vcf.gz"
  doAssert fileExists(outPath), "output missing: " & outPath
  let (_, bcCode) = execCmdEx("bcftools view -HG " & outPath & " > /dev/null 2>&1")
  doAssert bcCode == 0, "bcftools rejected 1-shard run output"
  let orig = countRecords(SmallVcf)
  let got  = countRecords(outPath)
  doAssert got == orig, &"1-shard record count mismatch: got {got}, expected {orig}"
  removeDir(tmpDir)
  echo &"PASS runShards: 1 shard, cat stage, {orig} records"

# ---------------------------------------------------------------------------
# R4 — worker pool (4 shards, --jobs 4 and --jobs 1)
# ---------------------------------------------------------------------------

block testRun4Shards:
  doAssert fileExists(SmallVcf), "fixture missing"
  let tmpDir = getTempDir() / "paravar_run_r4_4shards"
  createDir(tmpDir)
  let prefix = tmpDir / "out"
  runShards(SmallVcf, 4, prefix, 1, false, 4, "cat")
  var total = 0
  for i in 1..4:
    let p = prefix & "." & $i & ".vcf.gz"
    doAssert fileExists(p), &"shard {i} missing"
    let (_, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
    doAssert bc == 0, &"bcftools rejected shard {i}"
    total += countRecords(p)
  let orig = countRecords(SmallVcf)
  doAssert total == orig, &"4-shard record count mismatch: {total} vs {orig}"
  removeDir(tmpDir)
  echo &"PASS runShards: 4 shards, --jobs 4, {total} records"

block testRunJobs1Serial:
  doAssert fileExists(SmallVcf), "fixture missing"
  let tmpDir = getTempDir() / "paravar_run_r4_jobs1"
  createDir(tmpDir)
  let prefix = tmpDir / "out"
  runShards(SmallVcf, 4, prefix, 1, false, 1, "cat")  # maxJobs = 1
  var total = 0
  for i in 1..4:
    let p = prefix & "." & $i & ".vcf.gz"
    doAssert fileExists(p), &"serial shard {i} missing"
    let (_, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
    doAssert bc == 0, &"bcftools rejected serial shard {i}"
    total += countRecords(p)
  let orig = countRecords(SmallVcf)
  doAssert total == orig, &"serial record count mismatch: {total} vs {orig}"
  removeDir(tmpDir)
  echo &"PASS runShards: 4 shards, --jobs 1 (serial), {total} records"

block testRunJobsMoreThanShards:
  # maxJobs > nShards — should complete without hang or error.
  doAssert fileExists(SmallVcf), "fixture missing"
  let tmpDir = getTempDir() / "paravar_run_r4_jobsover"
  createDir(tmpDir)
  let prefix = tmpDir / "out"
  runShards(SmallVcf, 2, prefix, 1, false, 10, "cat")  # 10 jobs for 2 shards
  for i in 1..2:
    doAssert fileExists(prefix & "." & $i & ".vcf.gz"), &"over-jobs shard {i} missing"
  removeDir(tmpDir)
  echo "PASS runShards: maxJobs > nShards, no hang"

# ---------------------------------------------------------------------------
# R5 — CLI tests via the compiled binary
# ---------------------------------------------------------------------------

const BinPath = "./paravar"

block buildBinary:
  let (outp, code) = execCmdEx("nimble build 2>&1")
  if code != 0:
    echo "nimble build failed:\n", outp
    quit(1)
  doAssert fileExists(BinPath), "binary not found after nimble build"
  echo "PASS nimble build (run R5)"

proc runBin(args: string): (string, int) =
  execCmdEx(BinPath & " run " & args & " 2>&1")

block testCliRun1Shard:
  let tmpDir = getTempDir() / "paravar_r5_1shard"
  createDir(tmpDir)
  let prefix = tmpDir / "out"
  let (outp, code) = runBin(&"-n 1 -o {prefix} {SmallVcf} --- cat")
  doAssert code == 0, &"run -n 1 exited {code}:\n{outp}"
  let p = prefix & ".1.vcf.gz"
  doAssert fileExists(p), "output missing: " & p
  let (_, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
  doAssert bc == 0, "bcftools rejected 1-shard run output"
  let orig = countRecords(SmallVcf)
  doAssert countRecords(p) == orig, "1-shard CLI record count mismatch"
  removeDir(tmpDir)
  echo &"PASS CLI run: 1 shard, cat stage, {orig} records"

block testCliRun4Shards:
  let tmpDir = getTempDir() / "paravar_r5_4shards"
  createDir(tmpDir)
  let prefix = tmpDir / "out"
  let (outp, code) = runBin(&"-n 4 -o {prefix} {SmallVcf} --- cat")
  doAssert code == 0, &"run -n 4 exited {code}:\n{outp}"
  var total = 0
  for i in 1..4:
    let p = prefix & "." & $i & ".vcf.gz"
    doAssert fileExists(p), &"shard {i} missing"
    let (_, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
    doAssert bc == 0, &"bcftools rejected shard {i}"
    total += countRecords(p)
  doAssert total == countRecords(SmallVcf), &"4-shard CLI record count mismatch: {total}"
  removeDir(tmpDir)
  echo &"PASS CLI run: 4 shards, cat stage, {total} records"

block testCliRunMultiStage:
  # Two --- stages: cat | cat — output identical bytes to a single cat.
  let tmpDir = getTempDir() / "paravar_r5_multistage"
  createDir(tmpDir)
  let prefix = tmpDir / "out"
  let (outp, code) = runBin(&"-n 2 -o {prefix} {SmallVcf} --- cat --- cat")
  doAssert code == 0, &"run multi-stage exited {code}:\n{outp}"
  var total = 0
  for i in 1..2:
    let p = prefix & "." & $i & ".vcf.gz"
    doAssert fileExists(p), &"multi-stage shard {i} missing"
    total += countRecords(p)
  doAssert total == countRecords(SmallVcf), "multi-stage record count mismatch"
  removeDir(tmpDir)
  echo "PASS CLI run: multi-stage pipeline (cat | cat), records match"

block testCliRunJobs1:
  let tmpDir = getTempDir() / "paravar_r5_jobs1"
  createDir(tmpDir)
  let prefix = tmpDir / "out"
  let (outp, code) = runBin(&"-n 4 -j 1 -o {prefix} {SmallVcf} --- cat")
  doAssert code == 0, &"run -j 1 exited {code}:\n{outp}"
  var total = 0
  for i in 1..4:
    let p = prefix & "." & $i & ".vcf.gz"
    doAssert fileExists(p), &"-j 1 shard {i} missing"
    total += countRecords(p)
  doAssert total == countRecords(SmallVcf), "-j 1 record count mismatch"
  removeDir(tmpDir)
  echo "PASS CLI run: -j 1 (serial), records match"

block testCliRunJobsOver:
  # --jobs greater than nShards: should complete without hang.
  let tmpDir = getTempDir() / "paravar_r5_jobsover"
  createDir(tmpDir)
  let prefix = tmpDir / "out"
  let (outp, code) = runBin(&"-n 2 -j 10 -o {prefix} {SmallVcf} --- cat")
  doAssert code == 0, &"run -j 10 -n 2 exited {code}:\n{outp}"
  for i in 1..2:
    doAssert fileExists(prefix & "." & $i & ".vcf.gz"), &"over-jobs shard {i} missing"
  removeDir(tmpDir)
  echo "PASS CLI run: -j > nShards, no hang"

block testCliRunNonZeroExit:
  # Stage exits non-zero: paravar must exit 1 and mention the shard index.
  let tmpDir = getTempDir() / "paravar_r5_fail"
  createDir(tmpDir)
  let prefix = tmpDir / "out"
  let (outp, code) = runBin(&"-n 1 -o {prefix} {SmallVcf} --- false")
  doAssert code != 0, "non-zero stage should make paravar exit non-zero"
  doAssert "shard 1" in outp, &"stderr should mention 'shard 1', got:\n{outp}"
  removeDir(tmpDir)
  echo "PASS CLI run: non-zero stage exit → paravar exits 1, shard mentioned"

block testCliRunMissingSep:
  # No --- at all: paravar must exit 1 with appropriate message.
  let (outp, code) = runBin(&"-n 1 -o /tmp/x {SmallVcf}")
  doAssert code != 0, "missing --- should exit non-zero"
  doAssert "---" in outp, &"error should mention '---', got:\n{outp}"
  echo "PASS CLI run: missing --- → exits 1 with message"

block testCliRunDashDashPassthrough:
  # -- inside a stage should be passed to the tool, not consumed by paravar.
  # bcftools view -Oz -- - reads stdin, writes bgzipped VCF to stdout.
  let tmpDir = getTempDir() / "paravar_r5_dashdash"
  createDir(tmpDir)
  let prefix = tmpDir / "out"
  let (outp, code) = runBin(&"-n 1 -o {prefix} {SmallVcf} --- bcftools view -Oz -- -")
  doAssert code == 0, &"-- passthrough exited {code}:\n{outp}"
  let p = prefix & ".1.vcf.gz"
  doAssert fileExists(p), "-- passthrough output missing"
  doAssert countRecords(p) == countRecords(SmallVcf), "-- passthrough record count mismatch"
  removeDir(tmpDir)
  echo "PASS CLI run: -- inside stage passed through to bcftools"

echo ""
echo "All run R2/R3/R4/R5 tests passed."
