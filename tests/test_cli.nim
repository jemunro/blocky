## Tests for CLI argument handling.
## Run from project root: nim c -r tests/test_cli.nim
## Requires the vcfparty binary to be built first (nimble build).

import std/[os, osproc, sequtils, strformat, strutils]

const BinPath  = "./vcfparty"
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
# Build binary (setup)
# ---------------------------------------------------------------------------

block buildBinary:
  if not fileExists(BinPath):
    let (outp, code) = execCmdEx("nimble build 2>&1")
    if code != 0:
      echo "nimble build failed:\n", outp
      quit(1)
  doAssert fileExists(BinPath), "binary not found: " & BinPath & " (run nimble build)"
  echo "PASS binary available"

# ===========================================================================
# C1–C7 — Error cases (missing flags, invalid args, unsupported modes)
# ===========================================================================

# ---------------------------------------------------------------------------
# C1 — testMissingN: missing -n exits non-zero with 'n' in error message
# ---------------------------------------------------------------------------

block testMissingN:
  let (outp, code) = run(&"scatter -o /tmp/vcfparty_cli_test {SmallVcf}")
  doAssert code != 0, "missing -n should exit non-zero"
  doAssert "n" in outp.toLowerAscii,
    &"missing -n error should mention 'n', got: {outp}"
  echo "PASS missing -n exits non-zero"

# ---------------------------------------------------------------------------
# C2 — testInvalidN0: -n 0 exits non-zero
# ---------------------------------------------------------------------------

block testInvalidN0:
  let (_, code) = run(&"scatter -n 0 -o /tmp/vcfparty_cli_test {SmallVcf}")
  doAssert code != 0, "-n 0 should exit non-zero"
  echo "PASS -n 0 exits non-zero"

# ---------------------------------------------------------------------------
# C3 — testMissingO: missing -o exits non-zero with 'o' in error message
# ---------------------------------------------------------------------------

block testMissingO:
  let (outp, code) = run(&"scatter -n 2 {SmallVcf}")
  doAssert code != 0, "missing -o should exit non-zero"
  doAssert "o" in outp.toLowerAscii,
    &"missing -o error should mention 'o', got: {outp}"
  echo "PASS missing -o exits non-zero"

# ---------------------------------------------------------------------------
# C4 — testUnknownExtension: unknown extension (.xyz) exits 1 with extension in message
# ---------------------------------------------------------------------------

block testUnknownExtension:
  let tmpDir = getTempDir() / "vcfparty_unknown_ext_test"
  createDir(tmpDir)
  let tmpFile = tmpDir / "input.xyz"
  writeFile(tmpFile, "dummy")
  let (outp, code) = run(&"scatter -n 2 -o {tmpDir}/shard {tmpFile}")
  doAssert code != 0, "unknown extension should exit non-zero"
  doAssert ".xyz" in outp, &"error message should contain '.xyz', got: {outp}"
  removeDir(tmpDir)
  echo "PASS unknown extension exits 1 with extension in message"

# ---------------------------------------------------------------------------
# C5 — testBcfNoIndex: BCF without index exits 1 (no auto-scan fallback)
# ---------------------------------------------------------------------------

block testBcfNoIndex:
  doAssert fileExists(SmallBcf), "BCF fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "vcfparty_bcf_noindex_test"
  createDir(tmpDir)
  let tmpBcf = tmpDir / "noindex.bcf"
  copyFile(SmallBcf, tmpBcf)
  # Intentionally omit .csi so BCF has no index.
  let (outp, code) = run(&"scatter -n 2 -o {tmpDir}/shard {tmpBcf}")
  doAssert code != 0, &"BCF with no index should exit non-zero, got {code}:\n{outp}"
  removeDir(tmpDir)
  echo "PASS BCF no index exits 1"

# ---------------------------------------------------------------------------
# C6 — testBcfRunForceScan: --force-scan with BCF via run exits 1
# ---------------------------------------------------------------------------

block testBcfRunForceScan:
  doAssert fileExists(SmallBcf), "BCF fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "vcfparty_bcf_run_forcescan_test"
  createDir(tmpDir)
  let (outp, code) = run(&"run -n 2 -o {tmpDir}/out.vcf.gz --force-scan {SmallBcf} --- cat")
  doAssert code != 0, "--force-scan with BCF via run should exit non-zero"
  doAssert "force-scan" in outp.toLowerAscii,
    &"error should mention force-scan, got: {outp}"
  removeDir(tmpDir)
  echo "PASS BCF run --force-scan exits 1"

# ---------------------------------------------------------------------------
# C7 — testBcfForceScan: --force-scan with BCF via scatter exits 1
# ---------------------------------------------------------------------------

block testBcfForceScan:
  doAssert fileExists(SmallBcf), "BCF fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "vcfparty_bcf_forcescan_test"
  createDir(tmpDir)
  let (outp, code) = run(&"scatter -n 2 -o {tmpDir}/shard --force-scan {SmallBcf}")
  doAssert code != 0, "--force-scan with BCF should exit non-zero"
  doAssert "force-scan" in outp.toLowerAscii,
    &"error should mention force-scan, got: {outp}"
  removeDir(tmpDir)
  echo "PASS BCF --force-scan exits 1"

# ===========================================================================
# C8–C13 — Integration: scatter correctness for VCF and BCF
# ===========================================================================

# ---------------------------------------------------------------------------
# C8 — testMissingIndex: no index file → auto-scan fallback with warning; shards produced
# ---------------------------------------------------------------------------

block testMissingIndex:
  # With no index, scatter should warn and fall back to BGZF scan automatically.
  let tmpDir = getTempDir() / "vcfparty_noindex_test"
  createDir(tmpDir)
  let tmpVcf = tmpDir / "noindex.vcf.gz"
  copyFile(SmallVcf, tmpVcf)
  let outp_template = tmpDir / "out.vcf.gz"
  let (outp, code) = run(&"scatter -n 2 -o {outp_template} {tmpVcf}")
  doAssert code == 0, &"no-index scatter should succeed (auto-scan), got exit {code}:\n{outp}"
  doAssert "warning" in outp.toLowerAscii,
    &"no-index scatter should print a warning, got: {outp}"
  doAssert fileExists(tmpDir / "shard_1.out.vcf.gz"), "shard 1 missing after no-index scatter"
  doAssert fileExists(tmpDir / "shard_2.out.vcf.gz"), "shard 2 missing after no-index scatter"
  removeDir(tmpDir)
  echo "PASS no-index scatter: auto-scan with warning, shards produced"

# ---------------------------------------------------------------------------
# C9 — testForceScanFlag: --force-scan ignores existing index; shards valid, record count matches
# ---------------------------------------------------------------------------

block testForceScanFlag:
  let tmpDir = getTempDir() / "vcfparty_forcescan_test"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"
  let (runOutp, runCode) = run(&"scatter -n 4 -o {outp_template} --force-scan {SmallVcf}")
  doAssert runCode == 0, &"--force-scan exited non-zero:\n{runOutp}"
  proc countAndCheckFS(path: string): int =
    ## Count records; also validates the file (bcftools exits 0 on success).
    let (o, code) = execCmdEx("bcftools view -HG " & path & " 2>/dev/null")
    doAssert code == 0, &"bcftools rejected --force-scan shard: {path}"
    o.splitLines.countIt(it.len > 0)
  var total = 0
  for i in 1..4:
    let p = tmpDir / ("shard_" & $i & ".out.vcf.gz")
    doAssert fileExists(p), &"--force-scan shard {i} missing"
    total += countAndCheckFS(p)
  doAssert total == countAndCheckFS(SmallVcf),
    &"--force-scan record count mismatch: shards={total}"
  echo &"PASS --force-scan: 4 shards, {total} records"
  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# C10 — testEndToEnd: scatter -n 4 VCF; all shards valid, content hash matches
# ---------------------------------------------------------------------------

block testEndToEnd:
  let tmpDir = getTempDir() / "vcfparty_e2e_test"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"

  let (runOutp, runCode) = run(&"scatter -n 4 -o {outp_template} {SmallVcf}")
  doAssert runCode == 0, &"vcfparty scatter exited non-zero:\n{runOutp}"
  echo "PASS e2e: vcfparty scatter -n 4 exited 0"

  for i in 1..4:
    let shardPath = tmpDir / ("shard_" & $i & ".out.vcf.gz")
    doAssert fileExists(shardPath), &"shard {i} not found: {shardPath}"
    let (bcfOutp, bcfCode) = execCmdEx(
      "bcftools view -HG " & shardPath & " > /dev/null 2>&1")
    doAssert bcfCode == 0,
      &"bcftools rejected shard {i}: {bcfOutp}"
  echo "PASS e2e: all 4 shards are valid VCFs (bcftools view)"

  var shardPaths: seq[string]
  for i in 1..4:
    shardPaths.add(tmpDir / ("shard_" & $i & ".out.vcf.gz"))

  doAssert recordsHash(shardPaths) == recordsHash(@[SmallVcf]),
    "e2e content hash mismatch: record corruption or reordering detected"
  echo "PASS e2e: record content hash matches original (no corruption, correct count)"

  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# C11 — testCsiIndex: CSI-indexed VCF scatter; shards valid, record count matches
# ---------------------------------------------------------------------------

block testCsiIndex:
  doAssert fileExists(CsiVcf), "CSI fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "vcfparty_csi_test"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.vcf.gz"

  let (runOutp, runCode) = run(&"scatter -n 4 -o {outp_template} {CsiVcf}")
  doAssert runCode == 0, &"vcfparty scatter (CSI) exited non-zero:\n{runOutp}"

  proc countRec(path: string): int =
    let (o, _) = execCmdEx("bcftools view -HG " & path & " 2>/dev/null | wc -l")
    o.strip.parseInt

  var shardTotal = 0
  for i in 1..4:
    let path = tmpDir / ("shard_" & $i & ".out.vcf.gz")
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
# C12 — testBcfExtension: BCF scatter produces .bcf shards with matching content hash
# ---------------------------------------------------------------------------

block testBcfExtension:
  doAssert fileExists(SmallBcf), "BCF fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "vcfparty_bcf_ext_test"
  createDir(tmpDir)
  let outp_template = tmpDir / "out.bcf"
  let (outp, code) = run(&"scatter -n 4 -o {outp_template} {SmallBcf}")
  doAssert code == 0, &"BCF scatter exited non-zero:\n{outp}"
  var bcfShardPaths: seq[string]
  for i in 1..4:
    let p = tmpDir / ("shard_" & $i & ".out.bcf")
    doAssert fileExists(p), &"BCF shard {i} missing (.bcf extension): {p}"
    let (bo, bc) = execCmdEx("bcftools view -HG " & p & " > /dev/null 2>&1")
    doAssert bc == 0, &"bcftools rejected BCF shard {i}: {bo}"
    bcfShardPaths.add(p)
  doAssert recordsHash(bcfShardPaths) == recordsHash(@[SmallBcf]),
    "BCF content hash mismatch: record corruption or reordering detected"
  removeDir(tmpDir)
  echo "PASS BCF .bcf extension → BCF mode, shards have .bcf extension, content hash matches"

# ---------------------------------------------------------------------------
# C13 — testKg1000Genomes: large 1KG VCF scatter -n 10 (skipped if fixture absent)
# ---------------------------------------------------------------------------

block testKg1000Genomes:
  if not fileExists(KgVcf):
    echo "SKIP 1KG chr22: file not present (run tests/generate_fixtures.sh)"
  else:
    let tmpDir = getTempDir() / "vcfparty_kg_test"
    createDir(tmpDir)
    let outp_template = tmpDir / "out.vcf.gz"

    let (runOutp, runCode) = run(&"scatter -n 10 -o {outp_template} {KgVcf}")
    doAssert runCode == 0, &"vcfparty scatter (1KG) exited non-zero:\n{runOutp}"
    echo "PASS 1KG: vcfparty scatter -n 10 exited 0"

    # With -n 10, nDigits=2 so names are shard_01.out.vcf.gz … shard_10.out.vcf.gz
    for i in 1..10:
      let path = tmpDir / ("shard_" & align($i, 2, '0') & ".out.vcf.gz")
      doAssert fileExists(path), &"1KG shard {i} not found: {path}"
      let (bcfOutp, bcfCode) = execCmdEx(
        "bcftools view -HG " & path & " > /dev/null 2>&1")
      doAssert bcfCode == 0, &"bcftools rejected 1KG shard {i}: {bcfOutp}"
    echo "PASS 1KG: all 10 shards valid VCFs (bcftools view)"

    proc countRecs(path: string): int =
      let (o, _) = execCmdEx("bcftools view -HG " & path & " 2>/dev/null | wc -l")
      o.strip.parseInt

    var shardTotal = 0
    for i in 1..10:
      shardTotal += countRecs(tmpDir / ("shard_" & align($i, 2, '0') & ".out.vcf.gz"))
    let origTotal = countRecs(KgVcf)
    doAssert shardTotal == origTotal,
      &"1KG record count mismatch: shards={shardTotal} orig={origTotal}"
    echo &"PASS 1KG: record count matches original ({origTotal} records across 10 shards)"

    removeDir(tmpDir)

echo ""
echo "All CLI tests passed."
