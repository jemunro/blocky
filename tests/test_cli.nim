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
  ## Run partyvcf with shell args; combine stdout+stderr; return (outp, code).
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

# ===========================================================================
# C14–C17 — -i/--interleave and -s/--sequential flags
# ===========================================================================

# ---------------------------------------------------------------------------
# C14 — scatter -s: accepted as no-op, correct output
# ---------------------------------------------------------------------------

block testScatterSequentialFlag:
  let tmpDir = getTempDir() / "vcfparty_cli_sequential_scatter"
  createDir(tmpDir)
  let (outp, code) = run(&"scatter -n 2 -s -o {tmpDir}/shard.vcf.gz {SmallVcf}")
  doAssert code == 0, &"C14 scatter -s exited {code}:\n{outp}"
  doAssert fileExists(tmpDir / "shard_1.shard.vcf.gz"), "C14: shard 1 missing"
  doAssert fileExists(tmpDir / "shard_2.shard.vcf.gz"), "C14: shard 2 missing"
  removeDir(tmpDir)
  echo "PASS C14 scatter -s: accepted, shards produced"

# ---------------------------------------------------------------------------
# C15 — scatter -i: warning emitted, sequential fallback, correct output
# ---------------------------------------------------------------------------

block testScatterInterleaveFlag:
  let tmpDir = getTempDir() / "vcfparty_cli_interleave_scatter"
  createDir(tmpDir)
  let (outp, code) = run(&"scatter -n 2 -i -o {tmpDir}/shard.vcf.gz {SmallVcf}")
  doAssert code == 0, &"C15 scatter -i exited {code}:\n{outp}"
  doAssert "not yet implemented" in outp.toLowerAscii or "interleave" in outp.toLowerAscii,
    &"C15: expected interleave warning, got: {outp}"
  doAssert fileExists(tmpDir / "shard_1.shard.vcf.gz"), "C15: shard 1 missing"
  doAssert fileExists(tmpDir / "shard_2.shard.vcf.gz"), "C15: shard 2 missing"
  removeDir(tmpDir)
  echo "PASS C15 scatter -i: warning emitted, sequential fallback, shards produced"

# ---------------------------------------------------------------------------
# C16 — run -s: accepted as no-op, correct output
# ---------------------------------------------------------------------------

block testRunSequentialFlag:
  let tmpDir = getTempDir() / "vcfparty_cli_sequential_run"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf.gz"
  let (outp, code) = run(
    &"run -n 2 -s -o {outFile} {SmallVcf} ::: bcftools view -Oz +concat+")
  doAssert code == 0, &"C16 run -s exited {code}:\n{outp}"
  doAssert fileExists(outFile), "C16: output missing"
  let (cntOut, _) = execCmdEx("bcftools view -HG " & outFile & " 2>/dev/null | wc -l")
  doAssert cntOut.strip.parseInt > 0, "C16: output has no records"
  removeDir(tmpDir)
  echo "PASS C16 run -s: accepted, output produced"

# ---------------------------------------------------------------------------
# C17 — run -i: warning emitted, sequential fallback, correct output
# ---------------------------------------------------------------------------

block testRunInterleaveFlag:
  let tmpDir = getTempDir() / "vcfparty_cli_interleave_run"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf.gz"
  let (outp, code) = run(
    &"run -n 2 -i -o {outFile} {SmallVcf} ::: bcftools view -Oz +concat+")
  doAssert code == 0, &"C17 run -i exited {code}:\n{outp}"
  doAssert "not yet implemented" in outp.toLowerAscii or "interleave" in outp.toLowerAscii,
    &"C17: expected interleave warning, got: {outp}"
  doAssert fileExists(outFile), "C17: output missing"
  let (cntOut, _) = execCmdEx("bcftools view -HG " & outFile & " 2>/dev/null | wc -l")
  doAssert cntOut.strip.parseInt > 0, "C17: output has no records"
  removeDir(tmpDir)
  echo "PASS C17 run -i: warning emitted, sequential fallback, output produced"

echo ""
echo "All C14-C17 -i/-s flag tests passed."

# ===========================================================================
# C18–C21 — -O output format flag
# ===========================================================================

proc isBgzf(path: string): bool =
  let f = open(path, fmRead)
  var b: array[2, byte]
  discard f.readBytes(b, 0, 2)
  f.close()
  b[0] == 0x1f'u8 and b[1] == 0x8b'u8

# ---------------------------------------------------------------------------
# C18 — run -Oz: BGZF output, records intact
# ---------------------------------------------------------------------------

block testRunOutputFmtBgzf:
  let tmpDir = getTempDir() / "vcfparty_cli_O_bgzf"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf.gz"
  let (outp, code) = run(&"run -n 2 -Oz -o {outFile} {SmallVcf} ::: cat +concat+")
  doAssert code == 0, &"C18 run -Oz exited {code}:\n{outp}"
  doAssert fileExists(outFile), "C18: output file missing"
  let (cnt, _) = execCmdEx("bcftools view -HG " & outFile & " 2>/dev/null | wc -l")
  doAssert cnt.strip.parseInt > 0, "C18: output has no records"
  removeDir(tmpDir)
  echo "PASS C18 run -Oz: VCF BGZF output, records intact"

# ---------------------------------------------------------------------------
# C19 — run -Ov: VCF uncompressed output
# ---------------------------------------------------------------------------

block testRunOutputFmtVcfUncomp:
  let tmpDir = getTempDir() / "vcfparty_cli_O_vcf"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf"
  let (outp, code) = run(&"run -n 2 -Ov -o {outFile} {SmallVcf} ::: cat +concat+")
  doAssert code == 0, &"C19 run -Ov exited {code}:\n{outp}"
  doAssert fileExists(outFile), "C19: output file missing"
  doAssert not isBgzf(outFile), "C19: output should not be BGZF-compressed"
  let (cnt, _) = execCmdEx("grep -c '^[^#]' " & outFile & " 2>/dev/null || true")
  doAssert cnt.strip.parseInt > 0, "C19: output has no records"
  removeDir(tmpDir)
  echo "PASS C19 run -Ov: VCF uncompressed output"

# ---------------------------------------------------------------------------
# C20 — run -Ob with VCF input: exits non-zero (sniff mismatch: BCF flag, VCF input)
# ---------------------------------------------------------------------------

block testRunOutputFmtBcfMismatch:
  let tmpDir = getTempDir() / "vcfparty_cli_c20_ob"
  createDir(tmpDir)
  let outFile = tmpDir / "out.bcf"
  # Pipeline outputs VCF but -Ob expects BCF → runtime sniff mismatch in interceptor.
  let (outp, code) = run(&"run -n 2 -Ob -o {outFile} {SmallVcf} ::: cat +concat+")
  doAssert code != 0, &"C20: -Ob with VCF input should exit non-zero, got {code}"
  doAssert "pipeline" in outp or "format conversion" in outp or "VCF" in outp or "BCF" in outp,
    &"C20: expected format mismatch error, got:\n{outp}"
  removeDir(tmpDir)
  echo "PASS C20 run -Ob VCF input: exits non-zero (format sniff mismatch)"

# ---------------------------------------------------------------------------
# C21 — run -Ov with VCF input: exits 0 and produces uncompressed VCF output
# ---------------------------------------------------------------------------

block testRunOutputFmtVcfUncompressed:
  let tmpDir = getTempDir() / "vcfparty_cli_c21_ov"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf"
  let (outp, code) = run(&"run -n 2 -Ov -o {outFile} {SmallVcf} ::: cat +concat+")
  doAssert code == 0, &"C21 run -Ov exited {code}:\n{outp}"
  doAssert fileExists(outFile), "C21: output file missing"
  doAssert not isBgzf(outFile), "C21: output should not be BGZF"
  let (cnt, _) = execCmdEx("grep -c '^[^#]' " & outFile & " 2>/dev/null || true")
  doAssert cnt.strip.parseInt > 0, "C21: output has no records"
  removeDir(tmpDir)
  echo "PASS C21 run -Ov: VCF uncompressed output, records intact"

echo ""
echo "All C18-C21 -O output format tests passed."

# ===========================================================================
# C26 — scatter -Oz/-Ou and gather -Oz/-Ou
# ===========================================================================

# ---------------------------------------------------------------------------
# C26.1 — scatter -Oz: shard files are BGZF
# ---------------------------------------------------------------------------

block testScatterOz:
  let tmpDir = getTempDir() / "vcfparty_cli_scatter_oz"
  createDir(tmpDir)
  let (outp, code) = run(&"scatter -n 2 -Oz -o {tmpDir}/shard.vcf.gz {SmallVcf}")
  doAssert code == 0, &"C26.1 scatter -Oz exited {code}:\n{outp}"
  doAssert isBgzf(tmpDir / "shard_1.shard.vcf.gz"), "C26.1: shard 1 not BGZF"
  doAssert isBgzf(tmpDir / "shard_2.shard.vcf.gz"), "C26.1: shard 2 not BGZF"
  removeDir(tmpDir)
  echo "PASS C26.1 scatter -Oz: BGZF shard files produced"

# ---------------------------------------------------------------------------
# C26.2 — scatter -Ov: shard files are uncompressed VCF
# ---------------------------------------------------------------------------

block testScatterOv:
  let tmpDir = getTempDir() / "vcfparty_cli_scatter_ov"
  createDir(tmpDir)
  let (outp, code) = run(&"scatter -n 2 -Ov -o {tmpDir}/shard.vcf {SmallVcf}")
  doAssert code == 0, &"C26.2 scatter -Ov exited {code}:\n{outp}"
  doAssert not isBgzf(tmpDir / "shard_1.shard.vcf"), "C26.2: shard 1 should not be BGZF"
  doAssert not isBgzf(tmpDir / "shard_2.shard.vcf"), "C26.2: shard 2 should not be BGZF"
  removeDir(tmpDir)
  echo "PASS C26.2 scatter -Ov: uncompressed VCF shard files produced"

# ---------------------------------------------------------------------------
# C26.3 — scatter -Oz -Oz (duplicate): exits non-zero
# ---------------------------------------------------------------------------

block testScatterOzDuplicate:
  let (outp, code) = run(&"scatter -n 2 -Oz -Oz -o /tmp/ignored_c26_3 {SmallVcf}")
  doAssert code != 0, &"C26.3: duplicate -Oz should exit non-zero, got {code}"
  doAssert "only one" in outp or "allowed" in outp,
    &"C26.3: expected 'only one' in error, got:\n{outp}"
  echo "PASS C26.3 scatter duplicate -O: exits non-zero"

# ---------------------------------------------------------------------------
# C26.4 — scatter extension inference: .vcf.gz → BGZF, .vcf → uncompressed
# ---------------------------------------------------------------------------

block testScatterExtInfer:
  let d1 = getTempDir() / "vcfparty_cli_scatter_ext_bgzf"
  createDir(d1)
  let (o1, c1) = run(&"scatter -n 2 -o {d1}/shard.vcf.gz {SmallVcf}")
  doAssert c1 == 0, &"C26.4a scatter .vcf.gz exited {c1}:\n{o1}"
  doAssert isBgzf(d1 / "shard_1.shard.vcf.gz"), "C26.4a: shard 1 not BGZF"
  removeDir(d1)
  let d2 = getTempDir() / "vcfparty_cli_scatter_ext_plain"
  createDir(d2)
  let (o2, c2) = run(&"scatter -n 2 -o {d2}/shard.vcf {SmallVcf}")
  doAssert c2 == 0, &"C26.4b scatter .vcf exited {c2}:\n{o2}"
  doAssert not isBgzf(d2 / "shard_1.shard.vcf"), "C26.4b: shard 1 should not be BGZF"
  removeDir(d2)
  echo "PASS C26.4 scatter extension inference: .vcf.gz→BGZF, .vcf→uncompressed"

# ---------------------------------------------------------------------------
# C26.5 — run -Oz -Oz (duplicate): exits non-zero
# ---------------------------------------------------------------------------

block testRunOzDuplicate:
  let (outp, code) = run(&"run -n 2 -Oz -Oz -o /tmp/ignored_c26_5 {SmallVcf} ::: cat +concat+")
  doAssert code != 0, &"C26.5: duplicate -Oz should exit non-zero, got {code}"
  doAssert "only one" in outp or "allowed" in outp,
    &"C26.5: expected 'only one' in error, got:\n{outp}"
  echo "PASS C26.5 run duplicate -O: exits non-zero"

# ---------------------------------------------------------------------------
# C26.6 — gather -Oz: output is BGZF
# ---------------------------------------------------------------------------

block testGatherOz:
  # First scatter, then gather with -Oz
  let scatterDir = getTempDir() / "vcfparty_cli_gather_oz_scatter"
  createDir(scatterDir)
  let (so, sc) = run(&"scatter -n 2 -o {scatterDir}/shard.vcf.gz {SmallVcf}")
  doAssert sc == 0, &"C26.6 scatter exited {sc}:\n{so}"
  let outFile = scatterDir / "gathered.vcf.gz"
  let shards = scatterDir / "shard_1.shard.vcf.gz " & scatterDir / "shard_2.shard.vcf.gz"
  let (go, gc) = run(&"gather -Oz -o {outFile} {shards}")
  doAssert gc == 0, &"C26.6 gather -Oz exited {gc}:\n{go}"
  doAssert isBgzf(outFile), "C26.6: output should be BGZF"
  let (cnt, _) = execCmdEx("bcftools view -HG " & outFile & " 2>/dev/null | wc -l")
  doAssert cnt.strip.parseInt > 0, "C26.6: output has no records"
  removeDir(scatterDir)
  echo "PASS C26.6 gather -Oz: BGZF output, records intact"

# ---------------------------------------------------------------------------
# C26.7 — gather -Ov: output is uncompressed VCF
# ---------------------------------------------------------------------------

block testGatherOv:
  let scatterDir = getTempDir() / "vcfparty_cli_gather_ov_scatter"
  createDir(scatterDir)
  let (so, sc) = run(&"scatter -n 2 -o {scatterDir}/shard.vcf.gz {SmallVcf}")
  doAssert sc == 0, &"C26.7 scatter exited {sc}:\n{so}"
  let outFile = scatterDir / "gathered.vcf"
  let shards = scatterDir / "shard_1.shard.vcf.gz " & scatterDir / "shard_2.shard.vcf.gz"
  let (go, gc) = run(&"gather -Ov -o {outFile} {shards}")
  doAssert gc == 0, &"C26.7 gather -Ov exited {gc}:\n{go}"
  doAssert not isBgzf(outFile), "C26.7: output should not be BGZF"
  removeDir(scatterDir)
  echo "PASS C26.7 gather -Ov: uncompressed VCF output"

# ---------------------------------------------------------------------------
# C26.8 — gather -Oz -Oz (duplicate): exits non-zero
# ---------------------------------------------------------------------------

block testGatherOzDuplicate:
  let (outp, code) = run("gather -Oz -Oz -o /tmp/ignored_c26_8 /dev/null")
  doAssert code != 0, &"C26.8: duplicate -Oz should exit non-zero, got {code}"
  doAssert "only one" in outp or "allowed" in outp,
    &"C26.8: expected 'only one' in error, got:\n{outp}"
  echo "PASS C26.8 gather duplicate -O: exits non-zero"

echo ""
echo "All C26 scatter/gather -Oz/-Ou tests passed."

# ===========================================================================
# C22–C25 — -d/--decompress retired; -Iu added to run
# ===========================================================================

# ---------------------------------------------------------------------------
# C22 — scatter -d exits non-zero with helpful error message
# ---------------------------------------------------------------------------

block testScatterDecompressRetired:
  let (outp, code) = run(&"scatter -n 2 -d -o /tmp/ignored {SmallVcf}")
  doAssert code != 0, &"C22: scatter -d should exit non-zero but got {code}"
  doAssert "retired" in outp or "-Ou" in outp,
    &"C22: expected helpful error mentioning -Ou, got:\n{outp}"
  echo "PASS C22 scatter -d: exits non-zero with helpful error"

# ---------------------------------------------------------------------------
# C23 — scatter --decompress exits non-zero with helpful error message
# ---------------------------------------------------------------------------

block testScatterDecompressLongRetired:
  let (outp, code) = run(&"scatter -n 2 --decompress -o /tmp/ignored {SmallVcf}")
  doAssert code != 0, &"C23: scatter --decompress should exit non-zero but got {code}"
  doAssert "retired" in outp or "-Ou" in outp,
    &"C23: expected helpful error mentioning -Ou, got:\n{outp}"
  echo "PASS C23 scatter --decompress: exits non-zero with helpful error"

# ---------------------------------------------------------------------------
# C24 — run -Pv ... +collect+: all records present in output (VCF uncompressed piped)
# ---------------------------------------------------------------------------

block testRunPipeTypeVcfCollect:
  let tmpDir = getTempDir() / "vcfparty_cli_pv_collect"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf.gz"
  let (outp, code) = run(
    &"run -n 2 -Pv -o {outFile} {SmallVcf} ::: bcftools view -Oz +collect+")
  doAssert code == 0, &"C24 run -Pv +collect+ exited {code}:\n{outp}"
  doAssert fileExists(outFile), "C24: output missing"
  let (origCnt, _) = execCmdEx("bcftools view -HG " & SmallVcf & " 2>/dev/null | wc -l")
  let (outCnt, _)  = execCmdEx("bcftools view -HG " & outFile  & " 2>/dev/null | wc -l")
  doAssert origCnt.strip == outCnt.strip,
    &"C24: record count mismatch: orig={origCnt.strip} out={outCnt.strip}"
  removeDir(tmpDir)
  echo "PASS C24 run -Pv +collect+: all records present"

# ---------------------------------------------------------------------------
# C25 — run -d exits non-zero with helpful error message
# ---------------------------------------------------------------------------

block testRunDecompressRetired:
  let (outp, code) = run(
    &"run -n 2 -d -o /tmp/ignored {SmallVcf} ::: bcftools view -Oz +collect+")
  doAssert code != 0, &"C25: run -d should exit non-zero but got {code}"
  doAssert "retired" in outp or "-Pv" in outp or "-Pu" in outp or "-P" in outp,
    &"C25: expected helpful error mentioning -Pv/-Pu, got:\n{outp}"
  echo "PASS C25 run -d: exits non-zero with helpful error"

echo ""
echo "All C22-C25 -d/--decompress retired / -Iu tests passed."

# ===========================================================================
# C27 — +merge+ basic integration
# ===========================================================================

# ---------------------------------------------------------------------------
# C27 — +merge+: exits zero, output contains all records
# ---------------------------------------------------------------------------

block testMergeBasic:
  let tmpDir  = getTempDir() / "vcfparty_cli_merge_op"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf"
  let (outp, code) = run(
    &"run -n 2 -o {outFile} {SmallVcf} ::: bcftools view -Ov +merge+")
  doAssert code == 0, &"C27: +merge+ exited {code}:\n{outp}"
  doAssert fileExists(outFile), "C27: output file missing"
  let origCnt = execCmdEx("bcftools view -H " & SmallVcf & " 2>/dev/null | wc -l")[0].strip.parseInt
  var outCnt = 0
  for line in lines(outFile):
    if not line.startsWith("#"): inc outCnt
  doAssert outCnt == origCnt,
    &"C27: record count mismatch: orig={origCnt} out={outCnt}"
  removeDir(tmpDir)
  echo &"PASS C27 +merge+: exits zero, {outCnt} records present"

echo ""
echo "All C27 +merge+ tests passed."

# ===========================================================================
# C28 — FD inheritance regression: +concat+ with slow subprocess
# ===========================================================================
#
# Regression test for the fd-inheritance deadlock fixed by FD_CLOEXEC.
#
# Without FD_CLOEXEC on stdinPipe[1]: when child i+1 is forked it inherits
# stdinPipe_i[1] from the writer thread. After the writer closes its copy,
# child i's subprocess (cat reading stdin) never sees EOF because child i+1
# still holds the fd. Child i hangs → interceptor i hangs → +concat+ deadlocks.
#
# The sleep before cat ensures the writer for shard i is still alive when
# shard i+1 is forked, maximising the chance of the race being triggered.

block testConcatFdInheritance:
  let tmpDir  = getTempDir() / "vcfparty_cli_c28_fdinherit"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf.gz"
  # 4 shards, slow subprocess: sleep then cat (passes data through unchanged).
  let (outp, code) = run(
    &"run -n 4 -o {outFile} {SmallVcf} ::: sh -c 'sleep 0.05 && cat' +concat+")
  doAssert code == 0,
    &"C28: +concat+ with slow subprocess exited {code}:\n{outp}"
  doAssert fileExists(outFile), "C28: output file missing"
  let origCnt = execCmdEx(
    "bcftools view -H " & SmallVcf & " 2>/dev/null | wc -l")[0].strip.parseInt
  let outCnt  = execCmdEx(
    "bcftools view -H " & outFile  & " 2>/dev/null | wc -l")[0].strip.parseInt
  doAssert outCnt == origCnt,
    &"C28: record count mismatch: orig={origCnt} out={outCnt}"
  removeDir(tmpDir)
  echo &"PASS C28 +concat+ fd-inheritance: 4 shards, slow subprocess, {outCnt} records"

echo ""
echo "All C28 FD inheritance tests passed."

# ===========================================================================
# C29 — -P/--pipe-type and -O/--output-type full letter table (C4)
# ===========================================================================

# ---------------------------------------------------------------------------
# C29.1 — run -Pv with VCF input: exits 0, records intact
# ---------------------------------------------------------------------------

block testPipeTypeVcfUncompress:
  let tmpDir  = getTempDir() / "vcfparty_cli_c29_1"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf.gz"
  let (outp, code) = run(
    &"run -n 2 -Pv -o {outFile} {SmallVcf} ::: bcftools view -Oz +collect+")
  doAssert code == 0, &"C29.1 run -Pv exited {code}:\n{outp}"
  let (origCnt, _) = execCmdEx("bcftools view -HG " & SmallVcf & " 2>/dev/null | wc -l")
  let (outCnt, _)  = execCmdEx("bcftools view -HG " & outFile  & " 2>/dev/null | wc -l")
  doAssert origCnt.strip == outCnt.strip, &"C29.1: record count mismatch"
  removeDir(tmpDir)
  echo "PASS C29.1 run -Pv VCF input: exits 0, records intact"

# ---------------------------------------------------------------------------
# C29.2 — run -Pv with BCF input: exits non-zero (sniff mismatch)
# ---------------------------------------------------------------------------

block testPipeTypeVcfWithBcfInput:
  let tmpDir = getTempDir() / "vcfparty_cli_c29_2"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf.gz"
  let (outp, code) = run(
    &"run -n 2 -Pv -o {outFile} {SmallBcf} ::: bcftools view -Oz +collect+")
  doAssert code != 0, &"C29.2: -Pv with BCF input should exit non-zero, got {code}"
  doAssert "BCF" in outp or "format conversion" in outp or "pipeline" in outp,
    &"C29.2: expected format mismatch error, got:\n{outp}"
  removeDir(tmpDir)
  echo "PASS C29.2 run -Pv BCF input: exits non-zero (sniff mismatch)"

# ---------------------------------------------------------------------------
# C29.3 — run -Pb with BCF input: exits 0 (raw BGZF piped)
# ---------------------------------------------------------------------------

block testPipeTypeBcfBgzf:
  let tmpDir  = getTempDir() / "vcfparty_cli_c29_3"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf.gz"
  let (outp, code) = run(
    &"run -n 2 -Pb -o {outFile} {SmallBcf} ::: bcftools view -Oz +collect+")
  doAssert code == 0, &"C29.3 run -Pb BCF input exited {code}:\n{outp}"
  let (origCnt, _) = execCmdEx("bcftools view -HG " & SmallBcf & " 2>/dev/null | wc -l")
  let (outCnt, _)  = execCmdEx("bcftools view -HG " & outFile  & " 2>/dev/null | wc -l")
  doAssert origCnt.strip == outCnt.strip, &"C29.3: record count mismatch"
  removeDir(tmpDir)
  echo "PASS C29.3 run -Pb BCF input: exits 0 (raw BGZF piped), records intact"

# ---------------------------------------------------------------------------
# C29.4 — run -Pz with VCF input: exits 0 (raw BGZF piped)
# ---------------------------------------------------------------------------

block testPipeTypeVcfBgzf:
  let tmpDir  = getTempDir() / "vcfparty_cli_c29_4"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf.gz"
  let (outp, code) = run(
    &"run -n 2 -Pz -o {outFile} {SmallVcf} ::: bcftools view -Oz +collect+")
  doAssert code == 0, &"C29.4 run -Pz VCF input exited {code}:\n{outp}"
  let (origCnt, _) = execCmdEx("bcftools view -HG " & SmallVcf & " 2>/dev/null | wc -l")
  let (outCnt, _)  = execCmdEx("bcftools view -HG " & outFile  & " 2>/dev/null | wc -l")
  doAssert origCnt.strip == outCnt.strip, &"C29.4: record count mismatch"
  removeDir(tmpDir)
  echo "PASS C29.4 run -Pz VCF input: exits 0 (raw BGZF piped), records intact"

# ---------------------------------------------------------------------------
# C29.5 — run -Pu with BCF input: exits 0 (BCF uncompressed piped)
# ---------------------------------------------------------------------------

block testPipeTypeBcfUncompress:
  let tmpDir  = getTempDir() / "vcfparty_cli_c29_5"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf.gz"
  let (outp, code) = run(
    &"run -n 2 -Pu -o {outFile} {SmallBcf} ::: bcftools view -Oz +collect+")
  doAssert code == 0, &"C29.5 run -Pu BCF input exited {code}:\n{outp}"
  let (origCnt, _) = execCmdEx("bcftools view -HG " & SmallBcf & " 2>/dev/null | wc -l")
  let (outCnt, _)  = execCmdEx("bcftools view -HG " & outFile  & " 2>/dev/null | wc -l")
  doAssert origCnt.strip == outCnt.strip, &"C29.5: record count mismatch"
  removeDir(tmpDir)
  echo "PASS C29.5 run -Pu BCF input: exits 0 (BCF uncompressed piped), records intact"

# ---------------------------------------------------------------------------
# C29.6 — run -Px unknown letter: exits non-zero
# ---------------------------------------------------------------------------

block testPipeTypeUnknownLetter:
  let (outp, code) = run(
    &"run -n 2 -Px -o /tmp/ignored_c29_6 {SmallVcf} ::: cat +collect+")
  doAssert code != 0, &"C29.6: -Px should exit non-zero, got {code}"
  doAssert "accepted" in outp or "letter" in outp or "z" in outp,
    &"C29.6: expected letter error, got:\n{outp}"
  echo "PASS C29.6 run -Px: exits non-zero (unknown letter)"

# ---------------------------------------------------------------------------
# C29.7 — run -Iu exits non-zero with retired hint
# ---------------------------------------------------------------------------

block testInputUncompressRetiredHint:
  let (outp, code) = run(
    &"run -n 2 -Iu -o /tmp/ignored_c29_7 {SmallVcf} ::: cat +collect+")
  doAssert code != 0, &"C29.7: -Iu should exit non-zero, got {code}"
  doAssert "retired" in outp or "-P" in outp,
    &"C29.7: expected retired hint, got:\n{outp}"
  echo "PASS C29.7 run -Iu: exits non-zero with retired hint"

# ---------------------------------------------------------------------------
# C29.8 — scatter -Ob with VCF input: exits non-zero (sniff mismatch)
# ---------------------------------------------------------------------------

block testScatterObVcfInput:
  let (outp, code) = run(&"scatter -n 2 -Ob -o /tmp/ignored_c29_8 {SmallVcf}")
  doAssert code != 0, &"C29.8: scatter -Ob with VCF input should exit non-zero, got {code}"
  doAssert "VCF" in outp or "format conversion" in outp or "pipeline" in outp,
    &"C29.8: expected format mismatch error, got:\n{outp}"
  echo "PASS C29.8 scatter -Ob VCF input: exits non-zero (sniff mismatch)"

# ---------------------------------------------------------------------------
# C29.9 — scatter -Ov with BCF input: exits non-zero (sniff mismatch)
# ---------------------------------------------------------------------------

block testScatterOvBcfInput:
  let (outp, code) = run(&"scatter -n 2 -Ov -o /tmp/ignored_c29_9 {SmallBcf}")
  doAssert code != 0, &"C29.9: scatter -Ov with BCF input should exit non-zero, got {code}"
  doAssert "BCF" in outp or "format conversion" in outp or "pipeline" in outp,
    &"C29.9: expected format mismatch error, got:\n{outp}"
  echo "PASS C29.9 scatter -Ov BCF input: exits non-zero (sniff mismatch)"

# ---------------------------------------------------------------------------
# C29.10 — -O z (space-separated) and --output-type z (long form): exits 0
# ---------------------------------------------------------------------------

block testOutputTypeLongAndSpaceForms:
  let tmpDir  = getTempDir() / "vcfparty_cli_c29_10"
  createDir(tmpDir)
  let outFile1 = tmpDir / "out1.vcf.gz"
  let outFile2 = tmpDir / "out2.vcf.gz"
  # Space-separated: -O z
  let (o1, c1) = run(&"run -n 2 -O z -o {outFile1} {SmallVcf} ::: cat +concat+")
  doAssert c1 == 0, &"C29.10a run -O z exited {c1}:\n{o1}"
  doAssert fileExists(outFile1), "C29.10a: output missing"
  # Long form: --output-type z
  let (o2, c2) = run(&"run -n 2 --output-type z -o {outFile2} {SmallVcf} ::: cat +concat+")
  doAssert c2 == 0, &"C29.10b run --output-type z exited {c2}:\n{o2}"
  doAssert fileExists(outFile2), "C29.10b: output missing"
  removeDir(tmpDir)
  echo "PASS C29.10 -O z and --output-type z: exits 0"

# ---------------------------------------------------------------------------
# C29.11 — --pipe-type v (long form): exits 0, records intact
# ---------------------------------------------------------------------------

block testPipeTypeLongForm:
  let tmpDir  = getTempDir() / "vcfparty_cli_c29_11"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf.gz"
  let (outp, code) = run(
    &"run -n 2 --pipe-type v -o {outFile} {SmallVcf} ::: bcftools view -Oz +collect+")
  doAssert code == 0, &"C29.11 --pipe-type v exited {code}:\n{outp}"
  doAssert fileExists(outFile), "C29.11: output missing"
  let (origCnt, _) = execCmdEx("bcftools view -HG " & SmallVcf & " 2>/dev/null | wc -l")
  let (outCnt, _)  = execCmdEx("bcftools view -HG " & outFile  & " 2>/dev/null | wc -l")
  doAssert origCnt.strip == outCnt.strip, &"C29.11: record count mismatch"
  removeDir(tmpDir)
  echo "PASS C29.11 --pipe-type v: exits 0, records intact"

echo ""
echo "All C29 -P/--pipe-type and -O/--output-type full letter tests passed."
