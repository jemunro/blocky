## Unit tests for gather.sniffFileFormat.
## Run from project root: nim c --hints:off -r tests/test_sniff.nim

import std/[os]
import gather

# ---------------------------------------------------------------------------
# S1 — sniff VCF BGZF file
# ---------------------------------------------------------------------------

block sniffVcfBgzf:
  doAssert fileExists("tests/data/small.vcf.gz"), "fixture missing: tests/data/small.vcf.gz"
  let (fmt, compressed) = sniffFileFormat("tests/data/small.vcf.gz")
  doAssert fmt == ffVcf,  "S1: expected ffVcf, got " & $fmt
  doAssert compressed,    "S1: expected compressed=true"
  echo "PASS S1 sniff VCF BGZF: format=VCF compressed=true"

# ---------------------------------------------------------------------------
# S2 — sniff BCF BGZF file
# ---------------------------------------------------------------------------

block sniffBcfBgzf:
  doAssert fileExists("tests/data/small.bcf"), "fixture missing: tests/data/small.bcf"
  let (fmt, compressed) = sniffFileFormat("tests/data/small.bcf")
  doAssert fmt == ffBcf,  "S2: expected ffBcf, got " & $fmt
  doAssert compressed,    "S2: expected compressed=true"
  echo "PASS S2 sniff BCF BGZF: format=BCF compressed=true"

# ---------------------------------------------------------------------------
# S3 — sniff uncompressed VCF text
# ---------------------------------------------------------------------------

block sniffUncompressedVcf:
  let tmp = getTempDir() / "vcfparty_sniff_test_s3.vcf"
  writeFile(tmp, "##fileformat=VCFv4.1\n#CHROM\tPOS\tID\tREF\tALT\n")
  let (fmt, compressed) = sniffFileFormat(tmp)
  doAssert fmt == ffVcf,   "S3: expected ffVcf, got " & $fmt
  doAssert not compressed, "S3: expected compressed=false"
  removeFile(tmp)
  echo "PASS S3 sniff uncompressed VCF: format=VCF compressed=false"

# ---------------------------------------------------------------------------
# S4 — sniff uncompressed BCF (magic bytes only)
# ---------------------------------------------------------------------------

block sniffUncompressedBcf:
  let tmp = getTempDir() / "vcfparty_sniff_test_s4.bcf_raw"
  # BCF magic: B C F 0x02 0x02
  let magic: seq[byte] = @[byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8]
  var f = open(tmp, fmWrite)
  discard f.writeBytes(magic, 0, magic.len)
  f.close()
  let (fmt, compressed) = sniffFileFormat(tmp)
  doAssert fmt == ffBcf,   "S4: expected ffBcf, got " & $fmt
  doAssert not compressed, "S4: expected compressed=false"
  removeFile(tmp)
  echo "PASS S4 sniff uncompressed BCF magic: format=BCF compressed=false"

echo ""
echo "All sniff unit tests passed."
