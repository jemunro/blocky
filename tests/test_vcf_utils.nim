## Tests for vcf_utils.nim — BGZF I/O (V1), format sniffing (V2), file format sniffing (V3).
## Run from project root: nim c -r tests/test_vcf_utils.nim

import std/[os, strformat]
import "../src/vcfparty/vcf_utils"

# libdeflate CRC32 — already linked via -ldeflate in vcf_utils
proc libdeflateCrc32(crc: cuint; buf: pointer; len: csize_t): cuint
  {.importc: "libdeflate_crc32", header: "<libdeflate.h>".}

proc dataCrc32(data: seq[byte]): uint32 =
  ## Compute CRC32 of data using libdeflate.
  if data.len == 0:
    return libdeflateCrc32(0'u32, nil, 0).uint32
  libdeflateCrc32(0'u32, data[0].unsafeAddr, data.len.csize_t).uint32

const DataDir  = "tests/data"
const SmallVcf = DataDir / "small.vcf.gz"
const TinyVcf  = DataDir / "tiny.vcf.gz"
const SmallBcf = DataDir / "small.bcf"

# ---------------------------------------------------------------------------
# Helper: read raw bytes from a file slice
# ---------------------------------------------------------------------------
proc leU32At(data: seq[byte]; pos: int): uint32 =
  data[pos].uint32 or (data[pos+1].uint32 shl 8) or
  (data[pos+2].uint32 shl 16) or (data[pos+3].uint32 shl 24)

proc readFileSlice(path: string; start: int64; length: int): seq[byte] =
  let f = open(path, fmRead)
  defer: f.close()
  f.setFilePos(start)
  result = newSeq[byte](length)
  discard readBytes(f, result, 0, length)

# ===========================================================================
# V1 — BGZF I/O: block scanning, raw copy, compress/decompress, split, CRC
# ===========================================================================

# ---------------------------------------------------------------------------
# V1.1 — scanBgzfBlockStarts: offsets valid, first=0, last block is 28-byte EOF
# ---------------------------------------------------------------------------

block testScanBlockStarts:
  let starts = scanBgzfBlockStarts(SmallVcf)
  doAssert starts.len >= 2,
    &"expected >= 2 blocks (data + EOF), got {starts.len}"
  doAssert starts[0] == 0,
    &"first block must start at offset 0, got {starts[0]}"
  for off in starts:
    let hdr = readFileSlice(SmallVcf, off, 3)
    doAssert hdr[0] == 0x1f and hdr[1] == 0x8b and hdr[2] == 0x08,
      &"bad BGZF magic at offset {off}"
  let lastHdr = readFileSlice(SmallVcf, starts[^1], 18)
  let blkSize = bgzfBlockSize(lastHdr)
  doAssert blkSize == 28,
    &"expected EOF block of 28 bytes, got {blkSize}"
  echo "PASS V1.1 scanBgzfBlockStarts: offsets valid, first=0, last is EOF"

# ---------------------------------------------------------------------------
# V1.2 — scanBgzfBlockStarts range: range-limited scan truncates correctly
# ---------------------------------------------------------------------------

block testScanRange:
  let allStarts = scanBgzfBlockStarts(SmallVcf)
  doAssert allStarts.len >= 2
  let first = scanBgzfBlockStarts(SmallVcf, 0, allStarts[1])
  doAssert first == @[allStarts[0]],
    &"range scan: expected [{allStarts[0]}], got {first}"
  echo "PASS V1.2 scanBgzfBlockStarts range: truncates at upper bound"

# ---------------------------------------------------------------------------
# V1.3 — rawCopyBytes: copied bytes match source slice exactly
# ---------------------------------------------------------------------------

block testRawCopyBytes:
  let starts = scanBgzfBlockStarts(SmallVcf)
  let hdr = readFileSlice(SmallVcf, starts[0], 18)
  let blkSize = bgzfBlockSize(hdr)
  doAssert blkSize > 0

  let expected = readFileSlice(SmallVcf, starts[0], blkSize)
  let tmpPath = getTempDir() / "vcfparty_test_rawcopy.bin"
  let dst = open(tmpPath, fmWrite)
  rawCopyBytes(SmallVcf, dst, starts[0], blkSize.int64)
  dst.close()
  let got = readFile(tmpPath)
  removeFile(tmpPath)
  doAssert got.len == blkSize,
    &"rawCopyBytes: expected {blkSize} bytes, got {got.len}"
  for i in 0 ..< blkSize:
    doAssert got[i].byte == expected[i],
      &"rawCopyBytes: mismatch at byte {i}"
  echo "PASS V1.3 rawCopyBytes: copied bytes match source"

# ---------------------------------------------------------------------------
# V1.4 — compressToBgzf/decompressBgzf round-trip
# ---------------------------------------------------------------------------

block testRoundTrip:
  let original = "Hello, BGZF world!\nSecond line.\n"
  let origBytes = cast[seq[byte]](original)
  let compressed = compressToBgzf(origBytes)
  doAssert compressed[0] == 0x1f and compressed[1] == 0x8b,
    "compressed output missing gzip magic"
  doAssert compressed[12] == 0x42 and compressed[13] == 0x43,
    "compressed output missing BC extra field"
  let decompressed = decompressBgzf(compressed)
  doAssert decompressed == origBytes,
    &"round-trip mismatch: {decompressed} != {origBytes}"
  echo "PASS V1.4 compressToBgzf/decompressBgzf: round-trip"

# ---------------------------------------------------------------------------
# V1.5 — round-trip empty: empty input compresses and decompresses correctly
# ---------------------------------------------------------------------------

block testRoundTripEmpty:
  let compressed = compressToBgzf(@[])
  doAssert bgzfBlockSize(compressed) > 0, "empty compress: invalid block header"
  let decompressed = decompressBgzf(compressed)
  doAssert decompressed.len == 0, "empty round-trip: expected empty result"
  echo "PASS V1.5 round-trip empty: empty input"

# ---------------------------------------------------------------------------
# V1.6 — decompressBgzf fixture: real fixture block starts with '#'
# ---------------------------------------------------------------------------

block testDecompressFixture:
  let starts = scanBgzfBlockStarts(SmallVcf)
  var dataStart = -1'i64
  var dataSize  = 0
  for i, off in starts:
    let hdr = readFileSlice(SmallVcf, off, 18)
    let sz = bgzfBlockSize(hdr)
    if sz != 28:
      dataStart = off
      dataSize  = sz
      break
  doAssert dataStart >= 0, "no non-EOF block found in small.vcf.gz"
  let raw = readFileSlice(SmallVcf, dataStart, dataSize)
  let decompressed = decompressBgzf(raw)
  doAssert decompressed.len > 0, "decompressed data block is empty"
  doAssert decompressed[0] == byte('#') or decompressed[0] == byte('c'),
    &"unexpected first byte: {decompressed[0]}"
  echo "PASS V1.6 decompressBgzf fixture: real block decompresses"

# V1.7-V1.11 (splitChunk, bcfFirstDataOffset, splitBcfBoundaryBlock) removed:
# these procs no longer exist after Milestone V — scatter splits exclusively
# at index virtual offsets via splitBgzfBlockAtUOffset, eliminating the
# midpoint-line/record search and the bcfFirstDataOffset helper. End-to-end
# scatter correctness for both formats is covered by CL10-CL13 in test_cli.nim.

# ---------------------------------------------------------------------------
# V1.12 — BGZF CRC32 validation: stored CRC matches computed value
# ---------------------------------------------------------------------------

block testBgzfCrc32Validation:
  doAssert fileExists(SmallVcf), "fixture missing"
  let starts = scanBgzfBlockStarts(SmallVcf)
  var testOff = -1'i64
  for off in starts:
    let hdr = readFileSlice(SmallVcf, off, 18)
    let sz = bgzfBlockSize(hdr)
    if sz != 28:
      testOff = off
      break
  doAssert testOff >= 0, "no non-EOF block found in " & SmallVcf

  let hdr = readFileSlice(SmallVcf, testOff, 18)
  let blkSize = bgzfBlockSize(hdr)
  let blk = readFileSlice(SmallVcf, testOff, blkSize)

  let storedCrc = leU32At(blk, blkSize - 8)
  let decompressed = decompressBgzf(blk)
  let computedCrc = dataCrc32(decompressed)

  doAssert storedCrc != 0, "stored CRC32 should be non-zero for real data"
  doAssert storedCrc == computedCrc,
    &"BGZF CRC32 mismatch: stored={storedCrc:#x} computed={computedCrc:#x}"
  echo "PASS V1.12 BGZF CRC32: stored matches computed"

# ===========================================================================
# V2 — Format sniffing: isBgzfStream, sniffFormat, sniffStreamFormat
# ===========================================================================

# ---------------------------------------------------------------------------
# V2.1 — isBgzfStream: BGZF magic detected; plain gzip and random bytes rejected
# ---------------------------------------------------------------------------

block testIsBgzfStream:
  let bgzfHead = [0x1f'u8, 0x8b, 0x08, 0x04, 0x00]
  doAssert isBgzfStream(bgzfHead), "should detect BGZF magic"

  let gzHead = [0x1f'u8, 0x8b, 0x08, 0x00, 0x00]
  doAssert not isBgzfStream(gzHead), "plain gzip is not BGZF"

  let rnd = [0x42'u8, 0x43, 0x46, 0x02]
  doAssert not isBgzfStream(rnd), "random bytes are not BGZF"

  let short = [0x1f'u8, 0x8b]
  doAssert not isBgzfStream(short), "too-short buffer is not BGZF"

  echo "PASS V2.1 isBgzfStream: BGZF magic detected, non-BGZF rejected"

# ---------------------------------------------------------------------------
# V2.2 — sniffFormat: BCF/VCF/text detected from uncompressed bytes
# ---------------------------------------------------------------------------

block testSniffFormat:
  let bcfBytes = [byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8, 0x00'u8]
  doAssert sniffFormat(bcfBytes) == ffBcf, "BCF magic -> ffBcf"

  var vcfBytes: seq[byte]
  for c in "##fileformatVCFv4.2\n":
    vcfBytes.add(byte(c))
  doAssert sniffFormat(vcfBytes) == ffVcf, "##fileformat -> ffVcf"

  var txtBytes: seq[byte]
  for c in "CHROM\tPOS\tID\n":
    txtBytes.add(byte(c))
  doAssert sniffFormat(txtBytes) == ffText, "other bytes -> ffText"

  doAssert sniffFormat([0x00'u8, 0x01'u8]) == ffText, "short buffer -> ffText"

  let bcfExact = [byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8]
  doAssert sniffFormat(bcfExact) == ffBcf, "exact BCF magic length -> ffBcf"

  echo "PASS V2.2 sniffFormat: BCF/VCF/text detected correctly"

# ---------------------------------------------------------------------------
# V2.3 — sniffStreamFormat BCF: small.bcf detected as BCF/BGZF
# ---------------------------------------------------------------------------

block testSniffStreamFormatBcf:
  doAssert fileExists(SmallBcf), "BCF fixture missing"
  let f = open(SmallBcf, fmRead)
  var buf = newSeq[byte](65536)
  let n = readBytes(f, buf, 0, 65536)
  f.close()
  buf.setLen(n)
  let (fmt, isBgzf) = sniffStreamFormat(buf)
  doAssert fmt == ffBcf,  &"small.bcf: expected ffBcf, got {fmt}"
  doAssert isBgzf,         "small.bcf: expected BGZF stream"
  echo "PASS V2.3 sniffStreamFormat: BCF/BGZF"

# ---------------------------------------------------------------------------
# V2.4 — sniffStreamFormat VCF: small.vcf.gz detected as VCF/BGZF
# ---------------------------------------------------------------------------

block testSniffStreamFormatVcf:
  doAssert fileExists(SmallVcf), "VCF fixture missing"
  let f = open(SmallVcf, fmRead)
  var buf = newSeq[byte](65536)
  let n = readBytes(f, buf, 0, 65536)
  f.close()
  buf.setLen(n)
  let (fmt, isBgzf) = sniffStreamFormat(buf)
  doAssert fmt == ffVcf,  &"small.vcf.gz: expected ffVcf, got {fmt}"
  doAssert isBgzf,         "small.vcf.gz: expected BGZF stream"
  echo "PASS V2.4 sniffStreamFormat: VCF/BGZF"

# ---------------------------------------------------------------------------
# V2.5 — sniffStreamFormat text: plain text detected as text/uncompressed
# ---------------------------------------------------------------------------

block testSniffStreamFormatText:
  var raw: seq[byte]
  for c in "col1\tcol2\tcol3\nhello\tworld\t42\n":
    raw.add(byte(c))
  let (fmt, isBgzf) = sniffStreamFormat(raw)
  doAssert fmt == ffText, &"plain text: expected ffText, got {fmt}"
  doAssert not isBgzf,    "plain text: should not be BGZF"
  echo "PASS V2.5 sniffStreamFormat: text/uncompressed"

# ---------------------------------------------------------------------------
# V2.6 — sniffStreamFormat uncompressed VCF: VCF/uncompressed
# ---------------------------------------------------------------------------

block testSniffStreamFormatUncompressedVcf:
  var raw: seq[byte]
  for c in "##fileformatVCFv4.2\n##source=vcfparty\n#CHROM\tPOS\n":
    raw.add(byte(c))
  let (fmt, isBgzf) = sniffStreamFormat(raw)
  doAssert fmt == ffVcf,  &"uncompressed VCF: expected ffVcf, got {fmt}"
  doAssert not isBgzf,    "uncompressed VCF: should not be BGZF"
  echo "PASS V2.6 sniffStreamFormat: uncompressed VCF"

# ---------------------------------------------------------------------------
# V2.7 — sniffStreamFormat compressed text: BGZF text detected as text/BGZF
# ---------------------------------------------------------------------------

block testSniffStreamFormatCompressedText:
  var raw: seq[byte]
  for c in "hello world\n":
    raw.add(byte(c))
  let compressed = compressToBgzf(raw)
  let (fmt, isBgzf) = sniffStreamFormat(compressed)
  doAssert fmt == ffText, &"BGZF text: expected ffText, got {fmt}"
  doAssert isBgzf,         "BGZF text: expected BGZF stream"
  echo "PASS V2.7 sniffStreamFormat: BGZF-compressed text"

# ===========================================================================
# V3 — File format sniffing: sniffFileFormat
# ===========================================================================

# ---------------------------------------------------------------------------
# V3.1 — sniffFileFormat VCF BGZF: small.vcf.gz
# ---------------------------------------------------------------------------

block sniffVcfBgzf:
  doAssert fileExists(SmallVcf), "fixture missing: " & SmallVcf
  let (fmt, compressed) = sniffFileFormat(SmallVcf)
  doAssert fmt == ffVcf,  "V3.1: expected ffVcf, got " & $fmt
  doAssert compressed,    "V3.1: expected compressed=true"
  echo "PASS V3.1 sniffFileFormat: VCF BGZF"

# ---------------------------------------------------------------------------
# V3.2 — sniffFileFormat BCF BGZF: small.bcf
# ---------------------------------------------------------------------------

block sniffBcfBgzf:
  doAssert fileExists(SmallBcf), "fixture missing: " & SmallBcf
  let (fmt, compressed) = sniffFileFormat(SmallBcf)
  doAssert fmt == ffBcf,  "V3.2: expected ffBcf, got " & $fmt
  doAssert compressed,    "V3.2: expected compressed=true"
  echo "PASS V3.2 sniffFileFormat: BCF BGZF"

# ---------------------------------------------------------------------------
# V3.3 — sniffFileFormat uncompressed VCF
# ---------------------------------------------------------------------------

block sniffUncompressedVcf:
  let tmp = getTempDir() / "vcfparty_sniff_test_v3_3.vcf"
  writeFile(tmp, "##fileformat=VCFv4.1\n#CHROM\tPOS\tID\tREF\tALT\n")
  let (fmt, compressed) = sniffFileFormat(tmp)
  doAssert fmt == ffVcf,   "V3.3: expected ffVcf, got " & $fmt
  doAssert not compressed, "V3.3: expected compressed=false"
  removeFile(tmp)
  echo "PASS V3.3 sniffFileFormat: uncompressed VCF"

# ---------------------------------------------------------------------------
# V3.4 — sniffFileFormat uncompressed BCF (magic bytes only)
# ---------------------------------------------------------------------------

block sniffUncompressedBcf:
  let tmp = getTempDir() / "vcfparty_sniff_test_v3_4.bcf_raw"
  let magic: seq[byte] = @[byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8]
  var f = open(tmp, fmWrite)
  discard f.writeBytes(magic, 0, magic.len)
  f.close()
  let (fmt, compressed) = sniffFileFormat(tmp)
  doAssert fmt == ffBcf,   "V3.4: expected ffBcf, got " & $fmt
  doAssert not compressed, "V3.4: expected compressed=false"
  removeFile(tmp)
  echo "PASS V3.4 sniffFileFormat: uncompressed BCF magic"
