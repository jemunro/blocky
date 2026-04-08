## test_collect.nim — integration tests for +collect+ streaming output.
## Run from project root: nim c -r tests/test_collect.nim
## Requires the vcfparty binary (nimble build) and tests/data fixtures.

import std/[os, osproc, strformat, strutils]

const BinPath  = "./vcfparty"
const DataDir  = "tests/data"
const SmallVcf = DataDir / "small.vcf.gz"
const SmallBcf = DataDir / "small.bcf"

proc runBin(args: string): (string, int) =
  execCmdEx(BinPath & " run " & args & " 2>&1")

proc countRecords(path: string): int =
  let (o, _) = execCmdEx("bcftools view -H " & path & " 2>/dev/null | wc -l")
  o.strip.parseInt

# ---------------------------------------------------------------------------
# Build binary
# ---------------------------------------------------------------------------
block buildBinary:
  if not fileExists(BinPath):
    let (outp, code) = execCmdEx("nimble build 2>&1")
    if code != 0:
      echo "nimble build failed:\n", outp
      quit(1)
  doAssert fileExists(BinPath), "binary not found: " & BinPath
  echo "PASS CO0 binary available"

let origVcf = countRecords(SmallVcf)
let origBcf = countRecords(SmallBcf)

# ---------------------------------------------------------------------------
# CO1 — single shard VCF collect → file
# ---------------------------------------------------------------------------
block testCollect1Shard:
  let tmpDir  = getTempDir() / "vcfparty_c1_1shard"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf"
  let (outp, code) = runBin(&"-n 1 -o {outFile} {SmallVcf} ::: cat +collect+")
  doAssert code == 0, &"C1 exited {code}:\n{outp}"
  doAssert fileExists(outFile), "C1: output file missing"
  let (_, vc) = execCmdEx("bcftools view -HG " & outFile & " > /dev/null 2>&1")
  doAssert vc == 0, "C1: bcftools rejected collect output"
  doAssert countRecords(outFile) == origVcf, "C1: record count mismatch"
  removeDir(tmpDir)
  echo &"PASS CO1 collect: 1 shard VCF, {origVcf} records"

# ---------------------------------------------------------------------------
# CO2 — 4 shards VCF collect → file (order-insensitive)
# ---------------------------------------------------------------------------
block testCollect4Shards:
  let tmpDir  = getTempDir() / "vcfparty_c2_4shards"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf"
  let (outp, code) = runBin(&"-n 4 -o {outFile} {SmallVcf} ::: cat +collect+")
  doAssert code == 0, &"C2 exited {code}:\n{outp}"
  doAssert fileExists(outFile), "C2: output file missing"
  let (_, vc) = execCmdEx("bcftools view -HG " & outFile & " > /dev/null 2>&1")
  doAssert vc == 0, "C2: bcftools rejected collect output"
  doAssert countRecords(outFile) == origVcf, "C2: record count mismatch"
  removeDir(tmpDir)
  echo &"PASS CO2 collect: 4 shards VCF, {origVcf} records (order-insensitive)"

# ---------------------------------------------------------------------------
# CO3 — BCF collect → file
# ---------------------------------------------------------------------------
block testCollectBcf:
  let tmpDir  = getTempDir() / "vcfparty_c3_bcf"
  createDir(tmpDir)
  let outFile = tmpDir / "out.bcf"
  let (outp, code) = runBin(
    &"-n 4 -o {outFile} {SmallBcf} ::: bcftools view -Ou +collect+")
  doAssert code == 0, &"C3 exited {code}:\n{outp}"
  doAssert fileExists(outFile), "C3: output file missing"
  let (_, vc) = execCmdEx("bcftools view -HG " & outFile & " > /dev/null 2>&1")
  doAssert vc == 0, "C3: bcftools rejected BCF collect output"
  doAssert countRecords(outFile) == origBcf, "C3: BCF record count mismatch"
  removeDir(tmpDir)
  echo &"PASS CO3 collect: 4 shards BCF, {origBcf} records"

# ---------------------------------------------------------------------------
# CO4 — VCF collect → stdout
# ---------------------------------------------------------------------------
block testCollectStdout:
  let (outp, code) = execCmdEx(
    BinPath & " run -n 2 " & SmallVcf &
    " ::: cat +collect+ 2>/dev/null | bcftools view -H | wc -l")
  doAssert code == 0, &"C4 exited {code}"
  let n = outp.strip.parseInt
  doAssert n == origVcf, &"C4: stdout got {n} records, expected {origVcf}"
  echo &"PASS CO4 collect: stdout, {n} records"

# ---------------------------------------------------------------------------
# CO5 — no partial records: uncompressed VCF pipeline, bcftools validates output
# ---------------------------------------------------------------------------
block testCollectNoPartialRecords:
  let tmpDir  = getTempDir() / "vcfparty_c5_nopartial"
  createDir(tmpDir)
  let outFile = tmpDir / "out.vcf"
  let (outp, code) = runBin(
    &"-n 4 -o {outFile} {SmallVcf} ::: bcftools view -Ov +collect+")
  doAssert code == 0, &"C5 exited {code}:\n{outp}"
  doAssert fileExists(outFile), "C5: output file missing"
  # bcftools view -H will fail or produce wrong counts if records are partial.
  let (_, vc) = execCmdEx("bcftools view -H " & outFile & " > /dev/null 2>&1")
  doAssert vc == 0, "C5: bcftools failed — likely partial record in output"
  doAssert countRecords(outFile) == origVcf, "C5: record count mismatch"
  removeDir(tmpDir)
  echo &"PASS CO5 collect: no partial records (bcftools validates uncompressed VCF)"

