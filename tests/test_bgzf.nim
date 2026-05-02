## Tests for bgzf — BGZF I/O, format sniffing, file format sniffing, GZI, copyRange.
## Run from project root: nim c -r tests/test_vcf_utils.nim

echo "--------------- Test BGZF ---------------"

import std/[algorithm, atomics, os, posix, strformat]
import test_utils
import "../src/blocky/bgzf"

# libdeflate CRC32 — already linked via -ldeflate in bgzf
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
# B01–B06 — BGZF I/O: block scanning, compress/decompress, CRC
# ===========================================================================

# ---------------------------------------------------------------------------
# B01 — scanBgzfBlockStarts: offsets valid, first=0, last block is 28-byte EOF
# ---------------------------------------------------------------------------

timed("B01", "scanBgzfBlockStarts: offsets valid, first=0, last is EOF"):
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

# ---------------------------------------------------------------------------
# B02 — scanBgzfBlockStarts range: range-limited scan truncates correctly
# ---------------------------------------------------------------------------

timed("B02", "scanBgzfBlockStarts range: truncates at upper bound"):
  let allStarts = scanBgzfBlockStarts(SmallVcf)
  doAssert allStarts.len >= 2
  let first = scanBgzfBlockStarts(SmallVcf, 0, allStarts[1])
  doAssert first == @[allStarts[0]],
    &"range scan: expected [{allStarts[0]}], got {first}"

# ---------------------------------------------------------------------------
# B03 — compressToBgzf/decompressBgzf round-trip
# ---------------------------------------------------------------------------

timed("B03", "compressToBgzf/decompressBgzf: round-trip"):
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

# ---------------------------------------------------------------------------
# B04 — round-trip empty: empty input compresses and decompresses correctly
# ---------------------------------------------------------------------------

timed("B04", "round-trip empty: empty input"):
  let compressed = compressToBgzf(@[])
  doAssert bgzfBlockSize(compressed) > 0, "empty compress: invalid block header"
  let decompressed = decompressBgzf(compressed)
  doAssert decompressed.len == 0, "empty round-trip: expected empty result"

# ---------------------------------------------------------------------------
# B05 — decompressBgzf fixture: real fixture block starts with '#'
# ---------------------------------------------------------------------------

timed("B05", "decompressBgzf fixture: real block decompresses"):
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

# (splitChunk, bcfFirstDataOffset, splitBcfBoundaryBlock removed:
# these procs no longer exist after Milestone V — scatter splits exclusively
# at index virtual offsets via splitBgzfBlockAtUOffset, eliminating the
# midpoint-line/record search and the bcfFirstDataOffset helper. End-to-end
# scatter correctness for both formats is covered by CL10-CL13 in test_cli.nim.

# ---------------------------------------------------------------------------
# B06 — BGZF CRC32 validation: stored CRC matches computed value
# ---------------------------------------------------------------------------

timed("B06", "BGZF CRC32: stored matches computed"):
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

# ===========================================================================
# B07–B13 — Format sniffing: isBgzfStream, sniffFormat, sniffStreamFormat
# ===========================================================================

# ---------------------------------------------------------------------------
# B07 — isBgzfStream: BGZF magic detected; plain gzip and random bytes rejected
# ---------------------------------------------------------------------------

timed("B07", "isBgzfStream: BGZF magic detected, non-BGZF rejected"):
  let bgzfHead = [0x1f'u8, 0x8b, 0x08, 0x04, 0x00]
  doAssert isBgzfStream(bgzfHead), "should detect BGZF magic"

  let gzHead = [0x1f'u8, 0x8b, 0x08, 0x00, 0x00]
  doAssert not isBgzfStream(gzHead), "plain gzip is not BGZF"

  let rnd = [0x42'u8, 0x43, 0x46, 0x02]
  doAssert not isBgzfStream(rnd), "random bytes are not BGZF"

  let short = [0x1f'u8, 0x8b]
  doAssert not isBgzfStream(short), "too-short buffer is not BGZF"

# ---------------------------------------------------------------------------
# B08 — sniffFormat: BCF/VCF/text detected from uncompressed bytes
# ---------------------------------------------------------------------------

timed("B08", "sniffFormat: BCF/VCF/text detected correctly"):
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

# ---------------------------------------------------------------------------
# B09 — sniffStreamFormat BCF: small.bcf detected as BCF/BGZF
# ---------------------------------------------------------------------------

timed("B09", "sniffStreamFormat: BCF/BGZF"):
  doAssert fileExists(SmallBcf), "BCF fixture missing"
  let f = open(SmallBcf, fmRead)
  var buf = newSeq[byte](65536)
  let n = readBytes(f, buf, 0, 65536)
  f.close()
  buf.setLen(n)
  let (fmt, isBgzf) = sniffStreamFormat(buf)
  doAssert fmt == ffBcf,  &"small.bcf: expected ffBcf, got {fmt}"
  doAssert isBgzf,         "small.bcf: expected BGZF stream"

# ---------------------------------------------------------------------------
# B10 — sniffStreamFormat VCF: small.vcf.gz detected as VCF/BGZF
# ---------------------------------------------------------------------------

timed("B10", "sniffStreamFormat: VCF/BGZF"):
  doAssert fileExists(SmallVcf), "VCF fixture missing"
  let f = open(SmallVcf, fmRead)
  var buf = newSeq[byte](65536)
  let n = readBytes(f, buf, 0, 65536)
  f.close()
  buf.setLen(n)
  let (fmt, isBgzf) = sniffStreamFormat(buf)
  doAssert fmt == ffVcf,  &"small.vcf.gz: expected ffVcf, got {fmt}"
  doAssert isBgzf,         "small.vcf.gz: expected BGZF stream"

# ---------------------------------------------------------------------------
# B11 — sniffStreamFormat text: plain text detected as text/uncompressed
# ---------------------------------------------------------------------------

timed("B11", "sniffStreamFormat: text/uncompressed"):
  var raw: seq[byte]
  for c in "col1\tcol2\tcol3\nhello\tworld\t42\n":
    raw.add(byte(c))
  let (fmt, isBgzf) = sniffStreamFormat(raw)
  doAssert fmt == ffText, &"plain text: expected ffText, got {fmt}"
  doAssert not isBgzf,    "plain text: should not be BGZF"

# ---------------------------------------------------------------------------
# B12 — sniffStreamFormat uncompressed VCF: VCF/uncompressed
# ---------------------------------------------------------------------------

timed("B12", "sniffStreamFormat: uncompressed VCF"):
  var raw: seq[byte]
  for c in "##fileformatVCFv4.2\n##source=blocky\n#CHROM\tPOS\n":
    raw.add(byte(c))
  let (fmt, isBgzf) = sniffStreamFormat(raw)
  doAssert fmt == ffVcf,  &"uncompressed VCF: expected ffVcf, got {fmt}"
  doAssert not isBgzf,    "uncompressed VCF: should not be BGZF"

# ---------------------------------------------------------------------------
# B13 — sniffStreamFormat compressed text: BGZF text detected as text/BGZF
# ---------------------------------------------------------------------------

timed("B13", "sniffStreamFormat: BGZF-compressed text"):
  var raw: seq[byte]
  for c in "hello world\n":
    raw.add(byte(c))
  let compressed = compressToBgzf(raw)
  let (fmt, isBgzf) = sniffStreamFormat(compressed)
  doAssert fmt == ffText, &"BGZF text: expected ffText, got {fmt}"
  doAssert isBgzf,         "BGZF text: expected BGZF stream"

# ===========================================================================
# B14–B17 — File format sniffing: sniffFileFormat
# ===========================================================================

# ---------------------------------------------------------------------------
# B14 — sniffFileFormat VCF BGZF: small.vcf.gz
# ---------------------------------------------------------------------------

timed("B14", "sniffFileFormat: VCF BGZF"):
  doAssert fileExists(SmallVcf), "fixture missing: " & SmallVcf
  let (fmt, compressed) = sniffFileFormat(SmallVcf)
  doAssert fmt == ffVcf,  "B14: expected ffVcf, got " & $fmt
  doAssert compressed,    "B14: expected compressed=true"

# ---------------------------------------------------------------------------
# B15 — sniffFileFormat BCF BGZF: small.bcf
# ---------------------------------------------------------------------------

timed("B15", "sniffFileFormat: BCF BGZF"):
  doAssert fileExists(SmallBcf), "fixture missing: " & SmallBcf
  let (fmt, compressed) = sniffFileFormat(SmallBcf)
  doAssert fmt == ffBcf,  "B15: expected ffBcf, got " & $fmt
  doAssert compressed,    "B15: expected compressed=true"

# ---------------------------------------------------------------------------
# B16 — sniffFileFormat uncompressed VCF
# ---------------------------------------------------------------------------

timed("B16", "sniffFileFormat: uncompressed VCF"):
  let tmp = getTempDir() / "blocky_sniff_test_v3_3.vcf"
  writeFile(tmp, "##fileformat=VCFv4.1\n#CHROM\tPOS\tID\tREF\tALT\n")
  let (fmt, compressed) = sniffFileFormat(tmp)
  doAssert fmt == ffVcf,   "B16: expected ffVcf, got " & $fmt
  doAssert not compressed, "B16: expected compressed=false"
  removeFile(tmp)

# ---------------------------------------------------------------------------
# B17 — sniffFileFormat uncompressed BCF (magic bytes only)
# ---------------------------------------------------------------------------

timed("B17", "sniffFileFormat: uncompressed BCF magic"):
  let tmp = getTempDir() / "blocky_sniff_test_v3_4.bcf_raw"
  let magic: seq[byte] = @[byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8]
  var f = open(tmp, fmWrite)
  discard f.writeBytes(magic, 0, magic.len)
  f.close()
  let (fmt, compressed) = sniffFileFormat(tmp)
  doAssert fmt == ffBcf,   "B17: expected ffBcf, got " & $fmt
  doAssert not compressed, "B17: expected compressed=false"
  removeFile(tmp)

# ===========================================================================
# B18–B19 — GZI index parsing
# ===========================================================================

const GziVcf = DataDir / "small_gzi.vcf.gz"
const GziIdx = GziVcf & ".gzi"

# ---------------------------------------------------------------------------
# B18 — parseGziBlockStarts: offsets non-empty, sorted, first is 0
# ---------------------------------------------------------------------------

timed("B18", "parseGziBlockStarts: offsets non-empty, sorted, first=0"):
  let starts = parseGziBlockStarts(GziIdx)
  doAssert starts.len > 0, "GZI should have at least one entry"
  doAssert starts[0] == 0, &"first GZI offset should be 0, got {starts[0]}"
  for i in 1 ..< starts.len:
    doAssert starts[i] > starts[i - 1],
      &"GZI offsets not sorted at index {i}: {starts[i-1]} >= {starts[i]}"

# ---------------------------------------------------------------------------
# B19 — parseGziBlockStarts cross-validate: every GZI offset is a valid block
# ---------------------------------------------------------------------------

timed("B19", "parseGziBlockStarts: every offset is a valid BGZF block start"):
  let gziStarts = parseGziBlockStarts(GziIdx)
  let scanStarts = scanBgzfBlockStarts(GziVcf)
  # Every GZI offset must appear in scanBgzfBlockStarts results.
  var scanSet: seq[int64] = scanStarts
  scanSet.sort()
  for off in gziStarts:
    let idx = scanSet.binarySearch(off)
    doAssert idx >= 0,
      &"GZI offset {off} not found in scanBgzfBlockStarts results"

# ===========================================================================
# B20–B21 — Streaming BGZF compress / decompress
# ===========================================================================

# ---------------------------------------------------------------------------
# B20 — bgzfCompressStream/bgzfDecompressStream: round-trip identity
# ---------------------------------------------------------------------------

timed("B20", "bgzfCompressStream/bgzfDecompressStream: round-trip"):
  let original = decompressBgzfFile(SmallVcf)
  doAssert original.len > 0, "fixture decompressed to empty"

  let tmpComp = getTempDir() / "blocky_test_compress_stream.gz"
  let tmpDecomp = getTempDir() / "blocky_test_decompress_stream.raw"
  let tmpRaw = getTempDir() / "blocky_test_raw_input.bin"

  # Write raw bytes to a temp file, then compress via stream.
  var fRaw = open(tmpRaw, fmWrite)
  discard fRaw.writeBytes(original, 0, original.len)
  fRaw.close()

  var fIn = open(tmpRaw, fmRead)
  var fOut = open(tmpComp, fmWrite)
  bgzfCompressStream(fIn, fOut)
  fOut.close()
  fIn.close()

  # Verify output is valid BGZF (check magic at start).
  let compData = readFile(tmpComp)
  doAssert compData.len > 28, "compressed output too small"
  doAssert compData[0].byte == 0x1f and compData[1].byte == 0x8b,
    "compressed output missing gzip magic"

  # Decompress back via stream.
  fIn = open(tmpComp, fmRead)
  fOut = open(tmpDecomp, fmWrite)
  bgzfDecompressStream(fIn, fOut)
  fOut.close()
  fIn.close()

  let roundTrip = cast[seq[byte]](readFile(tmpDecomp))
  doAssert roundTrip.len == original.len,
    &"round-trip size mismatch: {roundTrip.len} vs {original.len}"
  doAssert roundTrip == original, "round-trip content mismatch"

  removeFile(tmpRaw)
  removeFile(tmpComp)
  removeFile(tmpDecomp)

# ---------------------------------------------------------------------------
# B21 — bgzfCompressStream: empty input produces only EOF block
# ---------------------------------------------------------------------------

timed("B21", "bgzfCompressStream: empty input -> EOF block only"):
  let tmpIn = getTempDir() / "blocky_test_empty_in.bin"
  let tmpOut = getTempDir() / "blocky_test_empty_out.gz"
  writeFile(tmpIn, "")
  var fIn = open(tmpIn, fmRead)
  var fOut = open(tmpOut, fmWrite)
  bgzfCompressStream(fIn, fOut)
  fOut.close()
  fIn.close()

  let data = readFile(tmpOut)
  doAssert data.len == 28, &"empty compress: expected 28-byte EOF, got {data.len}"

  removeFile(tmpIn)
  removeFile(tmpOut)

# ===========================================================================
# B22–B25 — copyRange / copyRangeFromFile: tiered copy with fallback
# ===========================================================================

# Helper: write known data, copy a range, verify identity.
proc testCopyRangeTier(tierName: string) =
  let tmpSrc = getTempDir() / "blocky_test_copysrc.bin"
  let tmpDst = getTempDir() / "blocky_test_copydst.bin"
  # Write 1 MB of known data: repeating pattern 0x00..0xFF.
  const DataSize = 1024 * 1024
  var srcData = newSeqUninit[byte](DataSize)
  for i in 0 ..< DataSize: srcData[i] = byte(i and 0xFF)
  block:
    let f = open(tmpSrc, fmWrite)
    discard f.writeBytes(srcData, 0, DataSize)
    f.close()
  # Copy a sub-range: skip first 1024 bytes, copy 500 KiB.
  const CopyStart = 1024
  const CopyLen = 500 * 1024
  let srcFd = posix.open(tmpSrc.cstring, O_RDONLY)
  let dstFd = posix.open(tmpDst.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o666.Mode)
  doAssert srcFd >= 0, tierName & ": could not open src"
  doAssert dstFd >= 0, tierName & ": could not open dst"
  copyRange(dstFd, srcFd, CopyLen.Off, CopyStart.Off)
  discard posix.close(srcFd)
  discard posix.close(dstFd)
  # Verify.
  let got = cast[seq[byte]](readFile(tmpDst))
  doAssert got.len == CopyLen,
    &"{tierName}: expected {CopyLen} bytes, got {got.len}"
  for i in 0 ..< CopyLen:
    doAssert got[i] == srcData[CopyStart + i],
      &"{tierName}: mismatch at byte {i}"
  removeFile(tmpSrc)
  removeFile(tmpDst)

# ---------------------------------------------------------------------------
# B22 — copyRange via copy_file_range (default, both flags clear)
# ---------------------------------------------------------------------------

timed("B22", "copyRange: copy_file_range tier"):
  gTierCopyFileRangeFailed.store(false, moRelaxed)
  gTierSendfileFailed.store(false, moRelaxed)
  testCopyRangeTier("B22 copy_file_range")

# ---------------------------------------------------------------------------
# B23 — copyRange via sendfile (copy_file_range disabled)
# ---------------------------------------------------------------------------

timed("B23", "copyRange: sendfile tier"):
  gTierCopyFileRangeFailed.store(true, moRelaxed)
  gTierSendfileFailed.store(false, moRelaxed)
  testCopyRangeTier("B23 sendfile")

# ---------------------------------------------------------------------------
# B24 — copyRange via pread/pwrite (both tiers disabled)
# ---------------------------------------------------------------------------

timed("B24", "copyRange: pread/pwrite tier"):
  gTierCopyFileRangeFailed.store(true, moRelaxed)
  gTierSendfileFailed.store(true, moRelaxed)
  testCopyRangeTier("B24 pread/pwrite")

# ---------------------------------------------------------------------------
# B25 — copyRangeFromFile round-trip
# ---------------------------------------------------------------------------

timed("B25", "copyRangeFromFile: round-trip identity"):
  gTierCopyFileRangeFailed.store(false, moRelaxed)
  gTierSendfileFailed.store(false, moRelaxed)
  let tmpSrc = getTempDir() / "blocky_test_crff_src.bin"
  let tmpDst = getTempDir() / "blocky_test_crff_dst.bin"
  const DataSize = 256 * 1024
  var srcData = newSeqUninit[byte](DataSize)
  for i in 0 ..< DataSize: srcData[i] = byte(i and 0xFF)
  block:
    let f = open(tmpSrc, fmWrite)
    discard f.writeBytes(srcData, 0, DataSize)
    f.close()
  let dstFd = posix.open(tmpDst.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o666.Mode)
  doAssert dstFd >= 0, "B25: could not open dst"
  copyRangeFromFile(tmpSrc, dstFd, 0, DataSize)
  discard posix.close(dstFd)
  let got = cast[seq[byte]](readFile(tmpDst))
  doAssert got == srcData, "B25: round-trip mismatch"
  removeFile(tmpSrc)
  removeFile(tmpDst)

# ---------------------------------------------------------------------------
# B26 — copyRange pwrite tier: verify fd position advances after copy
# ---------------------------------------------------------------------------

timed("B26", "copyRange: pwrite tier advances fd position"):
  gTierCopyFileRangeFailed.store(true, moRelaxed)
  gTierSendfileFailed.store(true, moRelaxed)
  let tmpSrc = getTempDir() / "blocky_test_pos_src.bin"
  let tmpDst = getTempDir() / "blocky_test_pos_dst.bin"
  const SrcSize = 256 * 1024
  const CopyStart = 1024
  const CopyLen = 100 * 1024
  var srcData = newSeqUninit[byte](SrcSize)
  for i in 0 ..< SrcSize: srcData[i] = byte(i and 0xFF)
  block:
    let f = open(tmpSrc, fmWrite)
    discard f.writeBytes(srcData, 0, SrcSize)
    f.close()
  # Write prefix, copyRange, write suffix — all to the same fd.
  let prefix = [byte 0xAA, 0xBB, 0xCC, 0xDD]
  let suffix = [byte 0x11, 0x22, 0x33, 0x44]
  let srcFd = posix.open(tmpSrc.cstring, O_RDONLY)
  let dstFd = posix.open(tmpDst.cstring, O_WRONLY or O_CREAT or O_TRUNC, 0o666.Mode)
  doAssert srcFd >= 0 and dstFd >= 0
  discard posix.write(dstFd, unsafeAddr prefix[0], prefix.len)
  copyRange(dstFd, srcFd, CopyLen.Off, CopyStart.Off)
  discard posix.write(dstFd, unsafeAddr suffix[0], suffix.len)
  discard posix.close(srcFd)
  discard posix.close(dstFd)
  let got = cast[seq[byte]](readFile(tmpDst))
  doAssert got.len == prefix.len + CopyLen + suffix.len,
    &"B26: expected {prefix.len + CopyLen + suffix.len}, got {got.len}"
  doAssert got[0..3] == @prefix, "B26: prefix mismatch"
  for i in 0 ..< CopyLen:
    doAssert got[prefix.len + i] == srcData[CopyStart + i],
      &"B26: copy mismatch at byte {i}"
  doAssert got[prefix.len + CopyLen .. prefix.len + CopyLen + 3] == @suffix,
    "B26: suffix mismatch — fd position not advanced after pwrite copyRange"
  removeFile(tmpSrc)
  removeFile(tmpDst)

# Reset flags to not pollute subsequent test runs.
gTierCopyFileRangeFailed.store(false, moRelaxed)
gTierSendfileFailed.store(false, moRelaxed)
