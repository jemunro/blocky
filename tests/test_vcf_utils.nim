## Tests for vcf_utils.nim.
## Run from project root: nim c -r tests/test_vcf_utils.nim

import std/[os, strformat]
import "../src/vcfparty/vcf_utils"

# libdeflate CRC32 — already linked via -ldeflate in bgzf_utils
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

# ---------------------------------------------------------------------------
# B1 — testScanBlockStarts: offsets valid, first=0, last block is 28-byte EOF
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
# B2 — testScanRange: range-limited scan truncates at upper bound correctly
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
# B3 — testRawCopyBytes: copied bytes match source slice exactly
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
  echo "PASS rawCopyBytes"

# ---------------------------------------------------------------------------
# B4 — testRoundTrip: compressToBgzf + decompressBgzf round-trips; BGZF magic + BC subfield present
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
# B5 — testRoundTripEmpty: empty input compresses and decompresses correctly
# ---------------------------------------------------------------------------

block testRoundTripEmpty:
  let compressed = compressToBgzf(@[])
  doAssert bgzfBlockSize(compressed) > 0, "empty compress: invalid block header"
  let decompressed = decompressBgzf(compressed)
  doAssert decompressed.len == 0, "empty round-trip: expected empty result"
  echo "PASS round-trip empty"

# ---------------------------------------------------------------------------
# B6 — testDecompressFixture: decompressBgzf on a real fixture block starts with '#'
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
# B7 — testSplitChunk: head ++ tail decompresses to original block contents
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

# ---------------------------------------------------------------------------
# B8 — testBcfFirstDataOffset: returned block straddles the l_text boundary
# ---------------------------------------------------------------------------

block testBcfFirstDataOffset:
  let dataOff = bcfFirstDataOffset(SmallBcf)
  doAssert dataOff >= 0,
    &"bcfFirstDataOffset: expected non-negative offset, got {dataOff}"

  # Must be a valid BGZF block start
  let starts = scanBgzfBlockStarts(SmallBcf)
  doAssert dataOff in starts,
    &"bcfFirstDataOffset: {dataOff} is not a BGZF block start"

  # Compute l_text from the raw decompressed stream to verify the threshold.
  let all = decompressBgzfFile(SmallBcf)
  doAssert all.len >= 9, "small.bcf decompressed too short to contain BCF header"
  doAssert all[0] == byte('B') and all[1] == byte('C') and
           all[2] == byte('F') and all[3] == 0x02'u8 and all[4] == 0x02'u8,
    "small.bcf does not have BCF magic"
  let lText = leU32At(all, 5).int64
  let firstRecOff = 5'i64 + 4'i64 + lText

  # Scan blocks and confirm dataOff is the first block that crosses firstRecOff.
  var cumUncomp = 0'i64
  var found = false
  for off in starts:
    let hdr = readFileSlice(SmallBcf, off, 18)
    let blkSize = bgzfBlockSize(hdr)
    if blkSize <= 0: break
    let blk = readFileSlice(SmallBcf, off, blkSize)
    let decoLen = decompressBgzf(blk).len.int64
    if off == dataOff:
      doAssert cumUncomp <= firstRecOff,
        &"cumulative before block {off} ({cumUncomp}) already >= firstRecOff ({firstRecOff})"
      doAssert cumUncomp + decoLen > firstRecOff,
        &"block at {off} does not cross firstRecOff {firstRecOff}"
      found = true
      break
    cumUncomp += decoLen
  doAssert found, &"dataOff {dataOff} was not reached while scanning blocks"
  echo "PASS bcfFirstDataOffset"

# ---------------------------------------------------------------------------
# B9 — testSplitBcfBoundaryBlockRoundTrip: decompressed head ++ tail equals original
# ---------------------------------------------------------------------------

block testSplitBcfBoundaryBlockRoundTrip:
  let dataOff = bcfFirstDataOffset(SmallBcf)
  let starts  = scanBgzfBlockStarts(SmallBcf)
  # Find size of the data block (distance to next block start)
  var dataSize = 0'i64
  for i, off in starts:
    if off == dataOff and i + 1 < starts.len:
      dataSize = starts[i + 1] - off
      break
  doAssert dataSize > 0, "could not determine data block size"

  let raw = readFileSlice(SmallBcf, dataOff, dataSize.int)
  let original = decompressBgzf(raw)

  let (valid, head, tail) = splitBcfBoundaryBlock(SmallBcf, dataOff, dataSize)
  doAssert valid, "splitBcfBoundaryBlock returned invalid for a real data block"
  doAssert bgzfBlockSize(head) > 0, "head is not a valid BGZF block"
  doAssert bgzfBlockSize(tail) > 0, "tail is not a valid BGZF block"

  let headData = decompressBgzf(head)
  let tailData = decompressBgzf(tail)
  doAssert headData & tailData == original,
    &"round-trip mismatch: {headData.len} + {tailData.len} != {original.len}"
  echo "PASS splitBcfBoundaryBlock round-trip"

# ---------------------------------------------------------------------------
# B10 — testSplitBcfBoundaryBlockMidpoint: head ends exactly on a record boundary
# ---------------------------------------------------------------------------

block testSplitBcfBoundaryBlockMidpoint:
  let dataOff = bcfFirstDataOffset(SmallBcf)
  let starts  = scanBgzfBlockStarts(SmallBcf)
  var dataSize = 0'i64
  for i, off in starts:
    if off == dataOff and i + 1 < starts.len:
      dataSize = starts[i + 1] - off
      break
  doAssert dataSize > 0

  let (valid, head, _) = splitBcfBoundaryBlock(SmallBcf, dataOff, dataSize)
  doAssert valid
  let headData = decompressBgzf(head)
  # Walk records in headData — must reach exactly headData.len with no partial record.
  var pos = 0
  while pos + 8 <= headData.len:
    let lShared = leU32At(headData, pos).int
    let lIndiv  = leU32At(headData, pos + 4).int
    let recLen  = 8 + lShared + lIndiv
    doAssert pos + recLen <= headData.len,
      &"head contains a partial BCF record at pos {pos} (recLen {recLen}, headData.len {headData.len})"
    pos += recLen
  doAssert pos == headData.len,
    &"head has {headData.len - pos} trailing bytes that are not a complete record"
  echo "PASS splitBcfBoundaryBlock midpoint"

# ---------------------------------------------------------------------------
# B11 — testSplitBcfBoundaryBlockZeroRecords: single-record block returns (false, @[], @[])
# ---------------------------------------------------------------------------

block testSplitBcfBoundaryBlockZeroRecords:
  # Construct a BGZF block with exactly ONE complete BCF record (l_shared=4, l_indiv=0).
  # A single record cannot be split, so the proc must return (false, @[], @[]).
  var recData = newSeq[byte](12)
  # l_shared = 4 (little-endian uint32)
  recData[0] = 4; recData[1] = 0; recData[2] = 0; recData[3] = 0
  # l_indiv = 0 (already zero)
  # shared data: 4 bytes of zeros (already zero)
  let blk = compressToBgzf(recData)

  let tmpPath = getTempDir() / "vcfparty_test_bcf_1rec.bin"
  let tf = open(tmpPath, fmWrite)
  discard tf.writeBytes(blk, 0, blk.len)
  tf.close()

  let (valid, h, t) = splitBcfBoundaryBlock(tmpPath, 0, blk.len.int64)
  removeFile(tmpPath)
  doAssert not valid,
    "splitBcfBoundaryBlock: single-record block should return valid=false"
  doAssert h.len == 0, "head should be empty for invalid block"
  doAssert t.len == 0, "tail should be empty for invalid block"
  echo "PASS splitBcfBoundaryBlock zero-record block"

# ---------------------------------------------------------------------------
# B12 — testBgzfCrc32Validation: CRC32 field in BGZF block matches computed value
# ---------------------------------------------------------------------------

block testBgzfCrc32Validation:
  doAssert fileExists(SmallVcf), "fixture missing"
  # Find first non-EOF block.
  let starts = scanBgzfBlockStarts(SmallVcf)
  var testOff = -1'i64
  for off in starts:
    let hdr = readFileSlice(SmallVcf, off, 18)
    let sz = bgzfBlockSize(hdr)
    if sz != 28:   # 28 = EOF block size
      testOff = off
      break
  doAssert testOff >= 0, "no non-EOF block found in " & SmallVcf

  let hdr = readFileSlice(SmallVcf, testOff, 18)
  let blkSize = bgzfBlockSize(hdr)
  let blk = readFileSlice(SmallVcf, testOff, blkSize)

  # CRC32 is stored at blk[blkSize-8 .. blkSize-5] (little-endian uint32).
  let storedCrc = leU32At(blk, blkSize - 8)
  let decompressed = decompressBgzf(blk)
  let computedCrc = dataCrc32(decompressed)

  doAssert storedCrc != 0, "stored CRC32 should be non-zero for real data"
  doAssert storedCrc == computedCrc,
    &"BGZF CRC32 mismatch: stored={storedCrc:#x} computed={computedCrc:#x}"
  echo &"PASS BGZF CRC32 validation: stored CRC32 matches decompressed data ({blkSize} byte block)"

echo ""
echo "All bgzf_utils tests passed."
