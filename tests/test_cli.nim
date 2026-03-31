## Tests for CLI argument handling.
## Run from project root: nim c -r tests/test_cli.nim
## Requires the paravar binary to be built first (nimble build).

import std/[os, osproc, strformat, strutils]

const BinPath  = "./paravar"
const DataDir  = "tests/data"
const SmallVcf = DataDir / "small.vcf.gz"       # TBI indexed
const CsiVcf   = DataDir / "small_csi.vcf.gz"   # CSI indexed only
const KgVcf    = DataDir / "chr22_1kg.vcf.gz"
const SmallBcf = DataDir / "small.bcf"          # CSI indexed BCF

proc run(args: string): (string, int) =
  ## Run paravar with shell args; combine stdout+stderr; return (outp, code).
  execCmdEx(BinPath & " " & args & " 2>&1")

proc recordsHash(paths: seq[string]): string =
  ## Concatenate records from paths in order (bcftools view -H, full genotypes),
  ## write to temp file, return sha256sum hex digest.
  let tmp = getTempDir() / "paravar_hash_" & $getCurrentProcessId() & ".txt"
  var f = open(tmp, fmWrite)
  for p in paths:
    let (o, _) = execCmdEx("bcftools view -H " & p & " 2>/dev/null")
    f.write(o)
  f.close()
  let (h, _) = execCmdEx("sha256sum " & tmp)
  removeFile(tmp)
  h.split(" ")[0]

# ---------------------------------------------------------------------------
# Build the binary before testing
# ---------------------------------------------------------------------------

block buildBinary:
  let (outp, code) = execCmdEx("nimble build 2>&1")
  if code != 0:
    echo "nimble build failed:\n", outp
    quit(1)
  doAssert fileExists(BinPath), "binary not found after nimble build: " & BinPath
  echo "PASS nimble build"

# ---------------------------------------------------------------------------
# Missing -n
# ---------------------------------------------------------------------------

block testMissingN:
  let (outp, code) = run(&"scatter -o /tmp/paravar_cli_test {SmallVcf}")
  doAssert code != 0, "missing -n should exit non-zero"
  doAssert "n" in outp.toLowerAscii,
    &"missing -n error should mention 'n', got: {outp}"
  echo "PASS missing -n exits non-zero"

# ---------------------------------------------------------------------------
# -n 0 (invalid shard count)
# ---------------------------------------------------------------------------

block testInvalidN0:
  let (_, code) = run(&"scatter -n 0 -o /tmp/paravar_cli_test {SmallVcf}")
  doAssert code != 0, "-n 0 should exit non-zero"
  echo "PASS -n 0 exits non-zero"

# ---------------------------------------------------------------------------
# Missing -o
# ---------------------------------------------------------------------------

block testMissingO:
  let (outp, code) = run(&"scatter -n 2 {SmallVcf}")
  doAssert code != 0, "missing -o should exit non-zero"
  doAssert "o" in outp.toLowerAscii,
    &"missing -o error should mention 'o', got: {outp}"
  echo "PASS missing -o exits non-zero"

# ---------------------------------------------------------------------------
# Missing index file
# ---------------------------------------------------------------------------

block testMissingIndex:
  # With no index, scatter should warn and fall back to BGZF scan automatically.
  let tmpDir = getTempDir() / "paravar_noindex_test"
  createDir(tmpDir)
  let tmpVcf = tmpDir / "noindex.vcf.gz"
  copyFile(SmallVcf, tmpVcf)
  let prefix = tmpDir / "shard"
  let (outp, code) = run(&"scatter -n 2 -o {prefix} {tmpVcf}")
  doAssert code == 0, &"no-index scatter should succeed (auto-scan), got exit {code}:\n{outp}"
  doAssert "warning" in outp.toLowerAscii,
    &"no-index scatter should print a warning, got: {outp}"
  doAssert fileExists(prefix & ".1.vcf.gz"), "shard 1 missing after no-index scatter"
  doAssert fileExists(prefix & ".2.vcf.gz"), "shard 2 missing after no-index scatter"
  removeDir(tmpDir)
  echo "PASS no-index scatter: auto-scan with warning, shards produced"

# ---------------------------------------------------------------------------
# Step 7 — end-to-end: run the binary, validate shards with bcftools
# ---------------------------------------------------------------------------

block testForceScanFlag:
  # --force-scan: index exists but is ignored.
  let tmpDir = getTempDir() / "paravar_forcescan_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"
  let (runOutp, runCode) = run(&"scatter -n 4 -o {prefix} --force-scan {SmallVcf}")
  doAssert runCode == 0, &"--force-scan exited non-zero:\n{runOutp}"
  proc countFS(path: string): int =
    let (o, _) = execCmdEx("bcftools view -HG " & path & " 2>/dev/null | wc -l")
    o.strip.parseInt
  var total = 0
  for i in 1..4:
    let p = prefix & "." & $i & ".vcf.gz"
    doAssert fileExists(p), &"--force-scan shard {i} missing"
    let (bo, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
    doAssert bc == 0, &"bcftools rejected --force-scan shard {i}: {bo}"
    total += countFS(p)
  doAssert total == countFS(SmallVcf),
    &"--force-scan record count mismatch: shards={total} orig={countFS(SmallVcf)}"
  echo &"PASS --force-scan: 4 shards, {total} records"
  removeDir(tmpDir)

block testEndToEnd:
  let tmpDir = getTempDir() / "paravar_e2e_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"

  # Run the CLI binary.
  let (runOutp, runCode) = run(&"scatter -n 4 -o {prefix} {SmallVcf}")
  doAssert runCode == 0, &"paravar scatter exited non-zero:\n{runOutp}"
  echo "PASS e2e: paravar scatter -n 4 exited 0"

  # Each shard must be a VCF readable by bcftools.
  for i in 1..4:
    let shardPath = prefix & "." & $i & ".vcf.gz"
    doAssert fileExists(shardPath), &"shard {i} not found: {shardPath}"
    let (bcfOutp, bcfCode) = execCmdEx(
      "bcftools view -HG " & shardPath & " > /dev/null 2>&1")
    doAssert bcfCode == 0,
      &"bcftools rejected shard {i}: {bcfOutp}"
  echo "PASS e2e: all 4 shards are valid VCFs (bcftools view)"

  # Total record count across shards must equal original.
  proc countRecords(path: string): int =
    let (outp2, _) = execCmdEx("bcftools view -HG " & path & " 2>/dev/null | wc -l")
    outp2.strip.parseInt

  var shardTotal = 0
  var shardPaths: seq[string]
  for i in 1..4:
    let p = prefix & "." & $i & ".vcf.gz"
    shardTotal += countRecords(p)
    shardPaths.add(p)
  let origTotal = countRecords(SmallVcf)
  doAssert shardTotal == origTotal,
    &"e2e record count mismatch: shards={shardTotal} orig={origTotal}"
  echo &"PASS e2e: record count matches original ({origTotal} records across 4 shards)"

  doAssert recordsHash(shardPaths) == recordsHash(@[SmallVcf]),
    "e2e content hash mismatch: record corruption or reordering detected"
  echo "PASS e2e: record content hash matches original (no corruption)"

  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# CSI index: scatter a file that has only a .csi (no .tbi)
# ---------------------------------------------------------------------------

block testCsiIndex:
  doAssert fileExists(CsiVcf), "CSI fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "paravar_csi_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"

  let (runOutp, runCode) = run(&"scatter -n 4 -o {prefix} {CsiVcf}")
  doAssert runCode == 0, &"paravar scatter (CSI) exited non-zero:\n{runOutp}"

  proc countRec(path: string): int =
    let (o, _) = execCmdEx("bcftools view -HG " & path & " 2>/dev/null | wc -l")
    o.strip.parseInt

  var shardTotal = 0
  for i in 1..4:
    let path = prefix & "." & $i & ".vcf.gz"
    doAssert fileExists(path), &"CSI shard {i} not found: {path}"
    let (bcfO, bcfC) = execCmdEx("bcftools view -HG " & path & " > /dev/null 2>&1")
    doAssert bcfC == 0, &"bcftools rejected CSI shard {i}: {bcfO}"
    shardTotal += countRec(path)
  let origTotal = countRec(CsiVcf)
  doAssert shardTotal == origTotal,
    &"CSI record count mismatch: shards={shardTotal} orig={origTotal}"
  echo &"PASS CSI: scatter -n 4, all shards valid, {origTotal} records"

  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# Optional: real 1000 Genomes chr22 integration test
# Skipped if tests/data/chr22_1kg.vcf.gz has not been downloaded.
# Run generate_fixtures.sh first to fetch it.
# ---------------------------------------------------------------------------

block testKg1000Genomes:
  if not fileExists(KgVcf):
    echo "SKIP 1KG chr22: file not present (run tests/generate_fixtures.sh)"
  else:
    let tmpDir = getTempDir() / "paravar_kg_test"
    createDir(tmpDir)
    let prefix = tmpDir / "shard"

    # Scatter into 10 shards via the CLI binary.
    let (runOutp, runCode) = run(&"scatter -n 10 -o {prefix} {KgVcf}")
    doAssert runCode == 0, &"paravar scatter (1KG) exited non-zero:\n{runOutp}"
    echo "PASS 1KG: paravar scatter -n 10 exited 0"

    # Each shard must be parseable by bcftools.
    # With -n 10, nDigits=2 so names are shard.01.vcf.gz … shard.10.vcf.gz
    for i in 1..10:
      let path = prefix & "." & align($i, 2, '0') & ".vcf.gz"
      doAssert fileExists(path), &"1KG shard {i} not found: {path}"
      let (bcfOutp, bcfCode) = execCmdEx(
        "bcftools view -HG " & path & " > /dev/null 2>&1")
      doAssert bcfCode == 0, &"bcftools rejected 1KG shard {i}: {bcfOutp}"
    echo "PASS 1KG: all 10 shards valid VCFs (bcftools view)"

    # Total record count must match.
    proc countRecs(path: string): int =
      let (o, _) = execCmdEx("bcftools view -HG " & path & " 2>/dev/null | wc -l")
      o.strip.parseInt

    var shardTotal = 0
    for i in 1..10:
      shardTotal += countRecs(prefix & "." & align($i, 2, '0') & ".vcf.gz")
    let origTotal = countRecs(KgVcf)
    doAssert shardTotal == origTotal,
      &"1KG record count mismatch: shards={shardTotal} orig={origTotal}"
    echo &"PASS 1KG: record count matches original ({origTotal} records across 10 shards)"

    removeDir(tmpDir)

# ---------------------------------------------------------------------------
# BCF: .bcf extension → BCF mode (no error, shards have .bcf extension)
# ---------------------------------------------------------------------------

block testBcfExtension:
  doAssert fileExists(SmallBcf), "BCF fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "paravar_bcf_ext_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"
  let (outp, code) = run(&"scatter -n 4 -o {prefix} {SmallBcf}")
  doAssert code == 0, &"BCF scatter exited non-zero:\n{outp}"
  var bcfShardPaths: seq[string]
  for i in 1..4:
    let p = prefix & "." & $i & ".bcf"
    doAssert fileExists(p), &"BCF shard {i} missing (.bcf extension): {p}"
    let (bo, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
    doAssert bc == 0, &"bcftools rejected BCF shard {i}: {bo}"
    bcfShardPaths.add(p)
  doAssert recordsHash(bcfShardPaths) == recordsHash(@[SmallBcf]),
    "BCF content hash mismatch: record corruption or reordering detected"
  removeDir(tmpDir)
  echo "PASS BCF .bcf extension → BCF mode, shards have .bcf extension, content hash matches"

# ---------------------------------------------------------------------------
# BCF: unknown extension → exits 1 with extension in message
# ---------------------------------------------------------------------------

block testUnknownExtension:
  let tmpDir = getTempDir() / "paravar_unknown_ext_test"
  createDir(tmpDir)
  let tmpFile = tmpDir / "input.xyz"
  writeFile(tmpFile, "dummy")
  let (outp, code) = run(&"scatter -n 2 -o {tmpDir}/shard {tmpFile}")
  doAssert code != 0, "unknown extension should exit non-zero"
  doAssert ".xyz" in outp, &"error message should contain '.xyz', got: {outp}"
  removeDir(tmpDir)
  echo "PASS unknown extension exits 1 with extension in message"

# ---------------------------------------------------------------------------
# BCF: no index → exits 1 (BCF requires CSI; no auto-scan fallback)
# ---------------------------------------------------------------------------

block testBcfNoIndex:
  doAssert fileExists(SmallBcf), "BCF fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "paravar_bcf_noindex_test"
  createDir(tmpDir)
  let tmpBcf = tmpDir / "noindex.bcf"
  copyFile(SmallBcf, tmpBcf)
  # Intentionally omit .csi so BCF has no index.
  let (outp, code) = run(&"scatter -n 2 -o {tmpDir}/shard {tmpBcf}")
  doAssert code != 0, &"BCF with no index should exit non-zero, got {code}:\n{outp}"
  removeDir(tmpDir)
  echo "PASS BCF no index exits 1"

# ---------------------------------------------------------------------------
# BCF: --force-scan via run subcommand → exits 1
# ---------------------------------------------------------------------------

block testBcfRunForceScan:
  doAssert fileExists(SmallBcf), "BCF fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "paravar_bcf_run_forcescan_test"
  createDir(tmpDir)
  let (outp, code) = run(&"run -n 2 -o {tmpDir}/shard --force-scan {SmallBcf} --- cat")
  doAssert code != 0, "--force-scan with BCF via run should exit non-zero"
  doAssert "force-scan" in outp.toLowerAscii,
    &"error should mention force-scan, got: {outp}"
  removeDir(tmpDir)
  echo "PASS BCF run --force-scan exits 1"

# ---------------------------------------------------------------------------
# BCF: --force-scan via scatter subcommand → exits 1 (not supported for BCF)
# ---------------------------------------------------------------------------

block testBcfForceScan:
  doAssert fileExists(SmallBcf), "BCF fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "paravar_bcf_forcescan_test"
  createDir(tmpDir)
  let (outp, code) = run(&"scatter -n 2 -o {tmpDir}/shard --force-scan {SmallBcf}")
  doAssert code != 0, "--force-scan with BCF should exit non-zero"
  doAssert "force-scan" in outp.toLowerAscii,
    &"error should mention force-scan, got: {outp}"
  removeDir(tmpDir)
  echo "PASS BCF --force-scan exits 1"

echo ""
echo "All CLI tests passed."
