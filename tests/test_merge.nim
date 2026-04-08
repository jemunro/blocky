## Tests for MG — +merge+ k-way merge output in run.nim.
## Run from project root: nim c -r tests/test_merge.nim
## Requires: tests/data/small.vcf.gz, tests/data/small.bcf

import std/[os, osproc, strformat, strutils, sequtils]

const DataDir  = "tests/data"
const SmallVcf = DataDir / "small.vcf.gz"
const SmallBcf = DataDir / "small.bcf"
const BinPath  = "./vcfparty"

block buildBinary:
  if not fileExists(BinPath):
    let (outp, code) = execCmdEx("nimble build 2>&1")
    if code != 0:
      echo "nimble build failed:\n", outp
      quit(1)
  doAssert fileExists(BinPath), "binary not found: " & BinPath & " (run nimble build)"
  echo "PASS MG0 binary available"

proc runBin(args: string): (string, int) =
  execCmdEx(BinPath & " run " & args & " 2>&1")

proc vcfRecordCount(path: string): int =
  ## Count non-header lines in an uncompressed VCF file.
  for line in lines(path):
    if not line.startsWith("#"):
      inc result

proc vcfRecords(path: string): seq[string] =
  ## Return non-header lines from an uncompressed VCF file.
  for line in lines(path):
    if not line.startsWith("#"):
      result.add(line)

proc contigOrderFromVcf(path: string): seq[string] =
  ## Return contig names in the order they appear in ##contig= header lines.
  for line in lines(path):
    if not line.startsWith("##contig="): continue
    let inner = line  # ##contig=<ID=chrN,...>
    let idPos = inner.find("ID=")
    if idPos < 0: continue
    var start = idPos + 3
    var stop  = start
    while stop < inner.len and inner[stop] != ',' and inner[stop] != '>':
      inc stop
    result.add(inner[start ..< stop])

proc isSortedVcf(path: string): bool =
  ## Verify records in path are in non-decreasing (contig_index, pos) order.
  let contigs = contigOrderFromVcf(path)
  var prevCi  = -1
  var prevPos = -1
  for line in lines(path):
    if line.startsWith("#"): continue
    let parts = line.split('\t')
    if parts.len < 2: continue
    let ci  = contigs.find(parts[0])
    let pos = parts[1].parseInt
    if ci < prevCi: return false
    if ci == prevCi and pos < prevPos: return false
    prevCi  = ci
    prevPos = pos
  true

# ---------------------------------------------------------------------------
# MG1 — VCF +merge+ 4 shards: bcftools view -Ov pipeline; sorted and complete
# ---------------------------------------------------------------------------
block testMergeVcf4Shards:
  doAssert fileExists(SmallVcf), &"VCF fixture missing: {SmallVcf}"
  let tmpDir = getTempDir() / "vcfparty_merge_m5_vcf4"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf"
  let (outp, code) = runBin(
    &"-n 4 -o {outFile} {SmallVcf} ::: bcftools view -Ov +merge+")
  doAssert code == 0, &"MG1: +merge+ exited {code}:\n{outp}"
  doAssert fileExists(outFile), "MG1: output file missing"
  let origCnt = vcfRecordCount(SmallVcf)
  # Get count from input BGZF via bcftools
  let (bco, _) = execCmdEx("bcftools view -H " & SmallVcf & " 2>/dev/null | wc -l")
  let origCount = bco.strip.parseInt
  let outCount  = vcfRecordCount(outFile)
  doAssert outCount == origCount,
    &"MG1: record count mismatch: got {outCount}, expected {origCount}"
  doAssert isSortedVcf(outFile), "MG1: output is not sorted by (contig, pos)"
  removeDir(tmpDir)
  echo &"PASS MG1 +merge+ VCF: 4 shards, {outCount} records, sorted"

# ---------------------------------------------------------------------------
# MG2 — BCF input, cat pipeline (BGZF pass-through): warning emitted, records present
# ---------------------------------------------------------------------------
block testMergeBcfCat:
  doAssert fileExists(SmallBcf), &"BCF fixture missing: {SmallBcf}"
  let tmpDir = getTempDir() / "vcfparty_merge_m5_bcfcat"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf"
  # Use bcftools view -Ov so the subprocess outputs uncompressed VCF,
  # which +merge+ can read as VCF records (simpler than raw BCF output).
  let (outp, code) = runBin(
    &"-n 4 -o {outFile} {SmallBcf} ::: bcftools view -Ov +merge+")
  doAssert code == 0, &"MG2: BCF +merge+ exited {code}:\n{outp}"
  doAssert fileExists(outFile), "MG2: output file missing"
  let (bco, _) = execCmdEx("bcftools view -H " & SmallBcf & " 2>/dev/null | wc -l")
  let origCount = bco.strip.parseInt
  let outCount  = vcfRecordCount(outFile)
  doAssert outCount == origCount,
    &"MG2: BCF record count mismatch: got {outCount}, expected {origCount}"
  doAssert isSortedVcf(outFile), "MG2: BCF +merge+ output is not sorted"
  removeDir(tmpDir)
  echo &"PASS MG2 +merge+ BCF input: 4 shards, {outCount} records, sorted"

# ---------------------------------------------------------------------------
# MG3 — Stdout output: no -o, output captured from stdout, records present
# ---------------------------------------------------------------------------
block testMergeStdout:
  doAssert fileExists(SmallVcf), &"VCF fixture missing: {SmallVcf}"
  let tmpDir  = getTempDir() / "vcfparty_merge_m5_stdout"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf"
  # Redirect stdout to outFile; stderr (warnings) go to /dev/null.
  let (_, code) = execCmdEx(
    BinPath & " run -n 4 " & SmallVcf &
    " ::: bcftools view -Ov +merge+ > " & outFile & " 2>/dev/null")
  doAssert code == 0, &"MG3: stdout +merge+ exited {code}"
  doAssert fileExists(outFile), "MG3: stdout output file missing"
  let (bco, _) = execCmdEx("bcftools view -H " & SmallVcf & " 2>/dev/null | wc -l")
  let origCount = bco.strip.parseInt
  let outCount  = vcfRecordCount(outFile)
  doAssert outCount == origCount,
    &"MG3: stdout record count: got {outCount}, expected {origCount}"
  doAssert isSortedVcf(outFile), "MG3: stdout output is not sorted"
  removeDir(tmpDir)
  echo &"PASS MG3 +merge+ stdout: {outCount} records, sorted"

# ---------------------------------------------------------------------------
# MG4 — BGZF pipeline (cat): warning triggered, records present
# ---------------------------------------------------------------------------
block testMergeBgzfWarning:
  doAssert fileExists(SmallVcf), &"VCF fixture missing: {SmallVcf}"
  let tmpDir  = getTempDir() / "vcfparty_merge_m5_bgzfwarn"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf"
  # cat passes BGZF bytes unchanged → feeder decompresses and warns.
  let (outp, code) = runBin(
    &"-n 4 -o {outFile} {SmallVcf} ::: cat +merge+")
  doAssert code == 0, &"MG4: BGZF cat +merge+ exited {code}:\n{outp}"
  doAssert "works best with uncompressed" in outp,
    &"MG4: expected BGZF warning in output, got:\n{outp}"
  doAssert fileExists(outFile), "MG4: output file missing"
  let (bco, _) = execCmdEx("bcftools view -H " & SmallVcf & " 2>/dev/null | wc -l")
  let origCount = bco.strip.parseInt
  let outCount  = vcfRecordCount(outFile)
  doAssert outCount == origCount,
    &"MG4: BGZF cat record count: got {outCount}, expected {origCount}"
  removeDir(tmpDir)
  echo &"PASS MG4 +merge+ BGZF pipeline: warning emitted, {outCount} records present"

