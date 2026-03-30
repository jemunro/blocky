## Tests for bgzf_utils.nim.
## Run from project root: nim c -r tests/test_bgzf_utils.nim

import std/[os, strformat]
import "../src/paravar/bgzf_utils"

const DataDir = "tests/data"
const SmallVcf = DataDir / "small.vcf.gz"
const TinyVcf  = DataDir / "tiny.vcf.gz"

# ---------------------------------------------------------------------------
# Helper: read raw bytes from a file slice
# ---------------------------------------------------------------------------
proc readFileSlice(path: string; start: int64; length: int): seq[byte] =
  let f = open(path, fmRead)
  defer: f.close()
  f.setFilePos(start)
  result = newSeq[byte](length)
  discard readBytes(f, result, 0, length)

# ---------------------------------------------------------------------------
# Test: scanBgzfBlockStarts
# ---------------------------------------------------------------------------

block testScanBlockStarts:
  let starts = scanBgzfBlockStarts(SmallVcf)
  doAssert starts.len >= 2,
    &"expected >= 2 blocks (data + EOF), got {starts.len}"
  doAssert starts[0] == 0,
    &"first block must start at offset 0, got {starts[0]}"
  # Every returned offset must have valid BGZF magic bytes
  for off in starts:
    let hdr = readFileSlice(SmallVcf, off, 3)
    doAssert hdr[0] == 0x1f and hdr[1] == 0x8b and hdr[2] == 0x08,
      &"bad BGZF magic at offset {off}"
  # Last block should be the EOF block (28 bytes, BSIZE-1 = 0x1b = 27)
  let lastHdr = readFileSlice(SmallVcf, starts[^1], 18)
  let blkSize = bgzfBlockSize(lastHdr)
  doAssert blkSize == 28,
    &"expected EOF block of 28 bytes, got {blkSize}"
  echo "PASS scanBgzfBlockStarts"

# ---------------------------------------------------------------------------
# Test: scanBgzfBlockStarts with startAt / endAt
# ---------------------------------------------------------------------------

block testScanRange:
  let allStarts = scanBgzfBlockStarts(SmallVcf)
  doAssert allStarts.len >= 2
  # Scan only up to the second block start — should return exactly 1 block
  let first = scanBgzfBlockStarts(SmallVcf, 0, allStarts[1])
  doAssert first == @[allStarts[0]],
    &"range scan: expected [{allStarts[0]}], got {first}"
  echo "PASS scanBgzfBlockStarts range"

# ---------------------------------------------------------------------------
# Test: rawCopyBytes
# ---------------------------------------------------------------------------

block testRawCopyBytes:
  let starts = scanBgzfBlockStarts(SmallVcf)
  let hdr = readFileSlice(SmallVcf, starts[0], 18)
  let blkSize = bgzfBlockSize(hdr)
  doAssert blkSize > 0

  let expected = readFileSlice(SmallVcf, starts[0], blkSize)
  let tmpPath = getTempDir() / "paravar_test_rawcopy.bin"
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
  echo "PASS rawCopyBytes"

# ---------------------------------------------------------------------------
# Test: compressToBgzf / decompressBgzf round-trip
# ---------------------------------------------------------------------------

block testRoundTrip:
  let original = "Hello, BGZF world!\nSecond line.\n"
  let origBytes = cast[seq[byte]](original)
  let compressed = compressToBgzf(origBytes)
  # Must start with BGZF magic
  doAssert compressed[0] == 0x1f and compressed[1] == 0x8b,
    "compressed output missing gzip magic"
  # Must contain BC subfield
  doAssert compressed[12] == 0x42 and compressed[13] == 0x43,
    "compressed output missing BC extra field"
  let decompressed = decompressBgzf(compressed)
  doAssert decompressed == origBytes,
    &"round-trip mismatch: {decompressed} != {origBytes}"
  echo "PASS compressToBgzf/decompressBgzf round-trip"

# ---------------------------------------------------------------------------
# Test: round-trip with empty input
# ---------------------------------------------------------------------------

block testRoundTripEmpty:
  let compressed = compressToBgzf(@[])
  doAssert bgzfBlockSize(compressed) > 0, "empty compress: invalid block header"
  let decompressed = decompressBgzf(compressed)
  doAssert decompressed.len == 0, "empty round-trip: expected empty result"
  echo "PASS round-trip empty"

# ---------------------------------------------------------------------------
# Test: decompressBgzf on an actual fixture block
# ---------------------------------------------------------------------------

block testDecompressFixture:
  let starts = scanBgzfBlockStarts(SmallVcf)
  # Find first non-EOF block (BSIZE != 28)
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
  # Must start with '#' (VCF header or data)
  doAssert decompressed[0] == byte('#') or decompressed[0] == byte('c'),
    &"unexpected first byte: {decompressed[0]}"
  echo "PASS decompressBgzf fixture"

# ---------------------------------------------------------------------------
# Test: splitChunk — halves decompress and concatenate back to original
# ---------------------------------------------------------------------------

block testSplitChunk:
  # Use tiny.vcf.gz: find the first data block (non-EOF)
  let starts = scanBgzfBlockStarts(TinyVcf)
  var dataStart = -1'i64
  var dataSize  = 0
  for off in starts:
    let hdr = readFileSlice(TinyVcf, off, 18)
    let sz = bgzfBlockSize(hdr)
    if sz != 28:
      dataStart = off
      dataSize  = sz
      break
  doAssert dataStart >= 0, "no data block found in tiny.vcf.gz"

  # Decompress the block directly to get the expected content
  let raw = readFileSlice(TinyVcf, dataStart, dataSize)
  let original = decompressBgzf(raw)

  let (head, tail) = splitChunk(TinyVcf, dataStart, dataSize.int64)
  doAssert bgzfBlockSize(head) > 0, "splitChunk head: invalid BGZF block"
  doAssert bgzfBlockSize(tail) > 0, "splitChunk tail: invalid BGZF block"

  let headData = decompressBgzf(head)
  let tailData = decompressBgzf(tail)
  let rejoined = headData & tailData
  doAssert rejoined == original,
    &"splitChunk: rejoined data != original ({rejoined.len} vs {original.len})"
  echo "PASS splitChunk"

echo ""
echo "All bgzf_utils tests passed."
