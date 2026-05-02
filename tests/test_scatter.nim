## Tests for scatter.nim — index parsing (Step 3), boundary optimisation (Step 4),
## and shard writing (Step 5).
## Run from project root: nim c -r tests/test_scatter.nim

echo "--------------- Test Scatter ---------------"

import std/[algorithm, atomics, math, os, posix, strformat, strutils, tempfiles]
import test_utils
import "../src/blocky/bgzf"
import "../src/blocky/scatter"

const DataDir      = "tests/data"
const SmallVcf     = DataDir / "small.vcf.gz"          # TBI indexed
const CsiVcf       = DataDir / "small_csi.vcf.gz"      # CSI indexed only (no .tbi)
const GziVcf       = DataDir / "small_gzi.vcf.gz"      # GZI indexed only (no .tbi/.csi)
const UnalignedVcf = DataDir / "small_unaligned.vcf.gz" # no index, blocks don't align to records
const SmallBcf     = DataDir / "small.bcf"
const KgBcf        = DataDir / "chr22_1kg.bcf"

proc readMagic(path: string; offset: int64): array[3, byte] =
  let f = open(path, fmRead)
  defer: f.close()
  f.setFilePos(offset)
  discard readBytes(f, result, 0, 3)

# ===========================================================================
# S01–S04 — Index parsing: TBI and CSI virtual offsets
# ===========================================================================

# ---------------------------------------------------------------------------
# S01 — parseTbiVirtualOffsets: voffs non-empty, sorted, block_off has BGZF magic
# ---------------------------------------------------------------------------
timed("S01", "parseTbiVirtualOffsets: virtual offsets valid"):
  let voffs = parseTbiVirtualOffsets(SmallVcf & ".tbi")
  doAssert voffs.len > 0, "parseTbiVirtualOffsets: no entries"
  for i in 1 ..< voffs.len:
    doAssert (voffs[i][0], voffs[i][1]) >= (voffs[i-1][0], voffs[i-1][1]),
      "parseTbiVirtualOffsets: not sorted"
  for v in voffs:
    let magic = readMagic(SmallVcf, v[0])
    doAssert magic[0] == 0x1f and magic[1] == 0x8b,
      &"bad BGZF magic at offset {v[0]}"

# ---------------------------------------------------------------------------
# S02 — readIndexVirtualOffsets falls back to TBI when no .csi present
# ---------------------------------------------------------------------------
timed("S02", "readIndexVirtualOffsets via TBI"):
  let voffs = readIndexVirtualOffsets(SmallVcf)
  doAssert voffs.len > 0, "readIndexVirtualOffsets (TBI): no entries"

# ---------------------------------------------------------------------------
# S03 — parseCsiVirtualOffsets: CSI-only fixture
# ---------------------------------------------------------------------------
timed("S03", "parseCsiVirtualOffsets: virtual offsets valid"):
  doAssert fileExists(CsiVcf & ".csi"), "CSI fixture missing — run generate_fixtures.sh"
  doAssert not fileExists(CsiVcf & ".tbi"), "CSI fixture must not have a .tbi alongside it"
  let voffs = parseCsiVirtualOffsets(CsiVcf & ".csi")
  doAssert voffs.len > 0, "parseCsiVirtualOffsets: no entries"
  for i in 1 ..< voffs.len:
    doAssert (voffs[i][0], voffs[i][1]) >= (voffs[i-1][0], voffs[i-1][1]),
      "parseCsiVirtualOffsets: not sorted"
  for v in voffs:
    let magic = readMagic(CsiVcf, v[0])
    doAssert magic[0] == 0x1f and magic[1] == 0x8b,
      &"bad BGZF magic at offset {v[0]}"

# ---------------------------------------------------------------------------
# S04 — readIndexVirtualOffsets falls through to CSI when no .tbi present
# ---------------------------------------------------------------------------
timed("S04", "readIndexVirtualOffsets via CSI"):
  let voffs = readIndexVirtualOffsets(CsiVcf)
  doAssert voffs.len > 0, "readIndexVirtualOffsets (CSI): no entries"

# ===========================================================================
# S05 — Header extraction and first block
# ===========================================================================

# ---------------------------------------------------------------------------
# S05 — testGetHeaderAndFirstBlock: header is valid BGZF, starts with '#'; firstBlock has BGZF magic
# ---------------------------------------------------------------------------
timed("S05", "getHeaderAndFirstBlock: header valid, firstBlock has BGZF magic"):
  let (hdrBytes, firstBlock, firstUOff) = getHeaderAndFirstBlock(SmallVcf)
  # Compressed header must be a valid BGZF block
  doAssert bgzfBlockSize(hdrBytes) > 0,
    "getHeaderAndFirstBlock: header not a valid BGZF block"
  # Decompress and verify it contains a VCF header line
  let hdrContent = decompressBgzf(hdrBytes)
  doAssert hdrContent.len > 0, "getHeaderAndFirstBlock: empty header"
  doAssert hdrContent[0] == byte('#'),
    "getHeaderAndFirstBlock: header does not start with '#'"
  # firstBlock must point to a valid BGZF block in the VCF
  let magic = readMagic(SmallVcf, firstBlock)
  doAssert magic[0] == 0x1f and magic[1] == 0x8b,
    &"getHeaderAndFirstBlock: firstBlock {firstBlock} has bad BGZF magic"
  doAssert firstUOff >= 0,
    &"getHeaderAndFirstBlock: firstUOff {firstUOff} must be non-negative"

# (partitionBoundaries, isValidBoundary, optimiseBoundaries) removed:
# these procs no longer exist after Milestone V — scatter now uses index virtual
# offsets directly, eliminating boundary search. End-to-end scatter correctness
# is covered by S06+ (checkShards) and C11+ in test_cli.nim.

# ===========================================================================
# S06–S10 — VCF scatter end-to-end (TBI, CSI, --force-scan)
# ===========================================================================

proc collectRecords(data: seq[byte]): seq[string] =
  ## Return all non-header lines from decompressed VCF bytes as strings.
  result = @[]
  var lineStart = 0
  for i in 0 ..< data.len:
    if data[i] == byte('\n'):
      if i > lineStart and data[lineStart] != byte('#'):
        var s = newString(i - lineStart)
        for j in lineStart ..< i: s[j - lineStart] = char(data[j])
        result.add(s)
      lineStart = i + 1
  if lineStart < data.len and data[lineStart] != byte('#'):
    var s = newString(data.len - lineStart)
    for j in lineStart ..< data.len: s[j - lineStart] = char(data[j])
    result.add(s)

proc checkShards(vcfPath: string; tmpl: string; n: int) =
  ## Verify BGZF structure, header presence, record completeness, and order.
  ## Each shard is decompressed exactly once; all checks reuse the cached bytes.
  let origRecords = collectRecords(decompressBgzfFile(vcfPath))
  var shardRecords: seq[string]

  for i in 1..n:
    let path = shardOutputPath(tmpl, i-1, n)
    doAssert fileExists(path), &"shard {i} missing: {path}"

    # BGZF magic + EOF (read-only, no decompression needed)
    let sz = getFileSize(path)
    let fz = open(path, fmRead)
    var hdrBuf = newSeq[byte](3)
    discard readBytes(fz, hdrBuf, 0, 3)
    doAssert hdrBuf[0] == 0x1f'u8 and hdrBuf[1] == 0x8b'u8,
      &"shard {i}: bad BGZF magic"
    fz.setFilePos(sz - 28)
    var eofBuf = newSeq[byte](28)
    discard readBytes(fz, eofBuf, 0, 28)
    fz.close()
    doAssert eofBuf == @BGZF_EOF, &"shard {i}: EOF block mismatch"

    # Decompress once; run all content checks on the result.
    let content = decompressBgzfFile(path)
    doAssert content.len > 0, &"shard {i}: empty after decompression"
    doAssert content[0] == byte('#'), &"shard {i}: does not start with '#'"

    # Exactly one #CHROM line per shard (catches duplicate header bug).
    # In VCF the #CHROM line is the only header line starting with '#'
    # but not '##', so counting these detects duplicated headers.
    block:
      var chromLineCount = 0
      var ls = 0
      for idx in 0 ..< content.len:
        if content[idx] == byte('\n'):
          if idx > ls and content[ls] == byte('#') and
              (idx == ls + 1 or content[ls + 1] != byte('#')):
            chromLineCount += 1
          ls = idx + 1
      doAssert chromLineCount == 1,
        &"shard {i}: {chromLineCount} #CHROM lines (expected 1, duplicate header?)"

    let recs = collectRecords(content)
    for j in 1 ..< recs.len:
      let prevFields = recs[j-1].split('\t')
      let curFields  = recs[j].split('\t')
      if prevFields.len >= 2 and curFields.len >= 2:
        if prevFields[0] == curFields[0]:
          doAssert prevFields[1].parseInt <= curFields[1].parseInt,
            &"shard {i}: records out of order at line {j}"
    shardRecords.add(recs)

  doAssert shardRecords.len == origRecords.len,
    &"record count mismatch: shards={shardRecords.len} orig={origRecords.len}"
  doAssert sorted(shardRecords) == sorted(origRecords),
    "shard records do not match original"

# ---------------------------------------------------------------------------
# S06 — testScatter4ShardsTbi: 4 shards (TBI); BGZF structure, completeness, order, size balance
# ---------------------------------------------------------------------------
timed("S06", "scatter TBI: 4 shards, completeness, order, balance"):
  let tmpDir = createTempDir("blocky_", "")
  let tmpl = tmpDir / "shard.{}.vcf.gz"
  scatter(SmallVcf, 4, tmpl)
  checkShards(SmallVcf, tmpl, 4)

  var sizes: seq[int64]
  for i in 0..3:
    sizes.add(getFileSize(shardOutputPath(tmpl, i, 4)))
  let minSz = sizes.min(); let maxSz = sizes.max()
  doAssert minSz > 0, "scatter (TBI): at least one shard is empty"
  doAssert maxSz.float / minSz.float < 2.0,
    &"scatter (TBI): shard size imbalance: max={maxSz} min={minSz}"

  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# S07 — testScatterForceScan: forceScan=true ignores index; result matches indexed scatter
# ---------------------------------------------------------------------------
timed("S07", "scatter --force-scan: completeness, order"):
  ## scatter with forceScan=true on a fully indexed file — index is ignored.
  let tmpDir = createTempDir("blocky_", "")
  let tmpl = tmpDir / "shard.{}.vcf.gz"
  scatter(SmallVcf, 4, tmpl, 1, forceScan = true)
  checkShards(SmallVcf, tmpl, 4)
  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# S08 — testScatter4ShardsCsi: 4 shards (CSI); BGZF structure, completeness, order
# ---------------------------------------------------------------------------
timed("S08", "scatter CSI: 4 shards, completeness, order"):
  doAssert fileExists(CsiVcf & ".csi"), "CSI fixture missing — run generate_fixtures.sh"
  let tmpDir = createTempDir("blocky_", "")
  let tmpl = tmpDir / "shard.{}.vcf.gz"
  scatter(CsiVcf, 4, tmpl)
  checkShards(CsiVcf, tmpl, 4)
  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# SC5.4 — scatter GZI: 4 shards (GZI scan shortcut); completeness, order
# ---------------------------------------------------------------------------
timed("S09", "scatter GZI: 4 shards, completeness, order"):
  if fileExists(GziVcf & ".gzi"):
    let tmpDir = createTempDir("blocky_", "")
    let tmpl = tmpDir / "shard.{}.vcf.gz"
    scatter(GziVcf, 4, tmpl)
    checkShards(GziVcf, tmpl, 4)
    removeDir(tmpDir)
  else:
    echo "  [skipped — small_gzi.vcf.gz.gzi not found, run generate_fixtures.sh]"

# ---------------------------------------------------------------------------
# SC5.5 — scatter unaligned blocks: records span BGZF block boundaries
# ---------------------------------------------------------------------------
timed("S10", "scatter unaligned blocks: completeness, order"):
  if fileExists(UnalignedVcf):
    let tmpDir = createTempDir("blocky_", "")
    let tmpl = tmpDir / "shard.{}.vcf.gz"
    scatter(UnalignedVcf, 4, tmpl, 1, forceScan = true)
    checkShards(UnalignedVcf, tmpl, 4)
    removeDir(tmpDir)
  else:
    echo "  [skipped — small_unaligned.vcf.gz not found, run generate_fixtures.sh]"

# ===========================================================================
# S11–S14 — BCF: header extraction and scatter end-to-end
# ===========================================================================

proc leU32At(data: seq[byte]; pos: int): uint32 =
  data[pos].uint32 or (data[pos+1].uint32 shl 8) or
  (data[pos+2].uint32 shl 16) or (data[pos+3].uint32 shl 24)

# ---------------------------------------------------------------------------
# S11 — testExtractBcfHeaderSmall: BGZF magic, BCF magic, l_text > 0, total decompressed == 5+4+l_text
# ---------------------------------------------------------------------------
timed("S11", "extractBcfHeader: small.bcf"):
  let (rawHdr, _, _) = extractBcfHeaderAndFirstOffset(SmallBcf)
  let hdrBytes = compressToBgzfMulti(rawHdr)
  # Must be a valid BGZF block sequence
  doAssert bgzfBlockSize(hdrBytes) > 0,
    "extractBcfHeader: result does not start with a valid BGZF block"
  # Decompress the first block and verify BCF magic
  let firstBlkSize = bgzfBlockSize(hdrBytes)
  let firstDecomp  = decompressBgzf(hdrBytes[0 ..< firstBlkSize])
  doAssert firstDecomp.len >= 9,
    "extractBcfHeader: first decompressed block too short to contain BCF header"
  doAssert firstDecomp[0] == byte('B') and firstDecomp[1] == byte('C') and
           firstDecomp[2] == byte('F') and firstDecomp[3] == 0x02'u8 and
           firstDecomp[4] == 0x02'u8,
    "extractBcfHeader: result does not start with BCF magic"
  # l_text must be positive
  let lText = leU32At(firstDecomp, 5).int64
  doAssert lText > 0, &"extractBcfHeader: l_text={lText} is not positive"
  # Total decompressed content must equal exactly 5 + 4 + l_text bytes
  let expectedSize = 5 + 4 + lText.int
  var totalDecomp = 0
  var pos = 0
  while pos < hdrBytes.len:
    let blkSz = bgzfBlockSize(hdrBytes[pos ..< hdrBytes.len])
    if blkSz <= 0: break
    totalDecomp += decompressBgzf(hdrBytes[pos ..< pos + blkSz]).len
    pos += blkSz
  doAssert totalDecomp == expectedSize,
    &"extractBcfHeader: decompressed {totalDecomp} bytes, expected {expectedSize}"

# ---------------------------------------------------------------------------
# S12 — testExtractBcfHeaderLarge: large BCF (2504 samples); multi-block header decompresses correctly
# ---------------------------------------------------------------------------
timed("S12", "extractBcfHeader: chr22_1kg.bcf large header"):
  # chr22_1kg.bcf has 2504 samples — verify extractBcfHeader handles it correctly.
  doAssert fileExists(KgBcf), &"large BCF fixture missing: {KgBcf}"
  let (rawHdr2, _, _) = extractBcfHeaderAndFirstOffset(KgBcf)
  let hdrBytes = compressToBgzfMulti(rawHdr2)
  doAssert bgzfBlockSize(hdrBytes) > 0,
    "extractBcfHeader large: result does not start with a valid BGZF block"
  let firstBlkSize = bgzfBlockSize(hdrBytes)
  let firstDecomp  = decompressBgzf(hdrBytes[0 ..< firstBlkSize])
  doAssert firstDecomp.len >= 9
  doAssert firstDecomp[0] == byte('B') and firstDecomp[1] == byte('C') and
           firstDecomp[2] == byte('F') and firstDecomp[3] == 0x02'u8 and
           firstDecomp[4] == 0x02'u8,
    "extractBcfHeader large: result does not start with BCF magic"
  let lText = leU32At(firstDecomp, 5).int64
  doAssert lText > 0, &"extractBcfHeader large: l_text={lText} is not positive"
  let expectedSize = 5 + 4 + lText.int
  # Total decompressed bytes across all output blocks must equal exactly 5 + 4 + l_text
  var totalDecomp = 0
  var pos = 0
  while pos < hdrBytes.len:
    let blkSz = bgzfBlockSize(hdrBytes[pos ..< hdrBytes.len])
    if blkSz <= 0: break
    totalDecomp += decompressBgzf(hdrBytes[pos ..< pos + blkSz]).len
    pos += blkSz
  doAssert totalDecomp == expectedSize,
    &"extractBcfHeader large: decompressed {totalDecomp} bytes, expected {expectedSize}"

# (checkBcfShards helper follows)

proc collectBcfRecordBytes(path: string): seq[seq[byte]] =
  ## Decompress BCF file, skip header, return raw bytes of each complete record.
  let data = decompressBgzfFile(path)
  if data.len < 9: return @[]
  let lText = leU32At(data, 5).int
  var pos = 5 + 4 + lText
  while pos + 8 <= data.len:
    let lShared = leU32At(data, pos).int
    let lIndiv  = leU32At(data, pos + 4).int
    let recLen  = 8 + lShared + lIndiv
    if pos + recLen > data.len: break
    result.add(data[pos ..< pos + recLen])
    pos += recLen

proc cmpRecBytes(a, b: seq[byte]): int =
  for i in 0 ..< min(a.len, b.len):
    if a[i] < b[i]: return -1
    if a[i] > b[i]: return 1
  cmp(a.len, b.len)

proc checkBcfShards(bcfPath: string; tmpl: string; n: int) =
  ## Verify BGZF structure, BCF magic, record completeness, and order.
  ## Each shard is decompressed exactly once; all checks reuse the cached bytes.
  let origRecs = collectBcfRecordBytes(bcfPath)
  var shardRecs: seq[seq[byte]]

  for i in 1..n:
    let path = shardOutputPath(tmpl, i-1, n)
    doAssert fileExists(path), &"BCF shard {i} missing: {path}"

    # BGZF magic + EOF (no decompression)
    let sz = getFileSize(path)
    let f = open(path, fmRead)
    var hdrBuf = newSeq[byte](3)
    discard readBytes(f, hdrBuf, 0, 3)
    doAssert hdrBuf[0] == 0x1f'u8 and hdrBuf[1] == 0x8b'u8,
      &"BCF shard {i}: bad BGZF magic"
    f.setFilePos(sz - 28)
    var eofBuf = newSeq[byte](28)
    discard readBytes(f, eofBuf, 0, 28)
    f.close()
    doAssert eofBuf == @BGZF_EOF, &"BCF shard {i}: EOF block mismatch"

    # Decompress once; run all content checks on the result.
    let data = decompressBgzfFile(path)
    doAssert data.len >= 9, &"BCF shard {i}: decompressed data too short"
    doAssert data[0] == byte('B') and data[1] == byte('C') and
             data[2] == byte('F') and data[3] == 0x02'u8 and
             data[4] == 0x02'u8, &"BCF shard {i}: bad BCF magic"

    let recs = block:
      # Walk records from data (same logic as collectBcfRecordBytes but on cached bytes).
      var res: seq[seq[byte]]
      let lText = leU32At(data, 5).int
      var pos = 5 + 4 + lText
      while pos + 8 <= data.len:
        let lShared = leU32At(data, pos).int
        let lIndiv  = leU32At(data, pos + 4).int
        let recLen  = 8 + lShared + lIndiv
        if pos + recLen > data.len: break
        res.add(data[pos ..< pos + recLen])
        pos += recLen
      res
    for j in 1 ..< recs.len:
      if recs[j].len >= 16 and recs[j-1].len >= 16:
        let prevChrom = leU32At(recs[j-1], 8)
        let curChrom  = leU32At(recs[j], 8)
        let prevPos   = leU32At(recs[j-1], 12)
        let curPos    = leU32At(recs[j], 12)
        if prevChrom == curChrom:
          doAssert prevPos <= curPos,
            &"BCF shard {i}: records out of order at record {j}"
    shardRecs.add(recs)

  doAssert shardRecs.len == origRecs.len,
    &"BCF: record count mismatch: shards={shardRecs.len} orig={origRecs.len}"
  doAssert sorted(shardRecs, cmpRecBytes) == sorted(origRecs, cmpRecBytes),
    "BCF: shard records do not match original"

# ---------------------------------------------------------------------------
# S13 — testBcfScatter4Shards: 4 BCF shards; BGZF, BCF magic, completeness, order, size balance
# ---------------------------------------------------------------------------
timed("S13", "BCF scatter: 4 shards, completeness, order, balance"):
  doAssert fileExists(SmallBcf), &"BCF fixture missing: {SmallBcf}"
  let tmpDir = createTempDir("blocky_", "")
  let tmpl = tmpDir / "shard.{}.bcf"
  scatter(SmallBcf, 4, tmpl, format = ffBcf)
  checkBcfShards(SmallBcf, tmpl, 4)
  var sizes: seq[int64]
  for i in 0..3:
    sizes.add(getFileSize(shardOutputPath(tmpl, i, 4)))
  let minSz = sizes.min(); let maxSz = sizes.max()
  doAssert minSz > 0, "BCF scatter 4 shards: at least one shard is empty"
  doAssert maxSz.float / minSz.float < 2.0,
    &"BCF scatter 4 shards: shard size imbalance: max={maxSz} min={minSz}"
  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# S14 — testBcfScatterLargeHeader: 4 BCF shards from 1KG (large header); completeness and order
# ---------------------------------------------------------------------------
timed("S14", "BCF scatter: chr22_1kg.bcf large header, 4 shards"):
  doAssert fileExists(KgBcf), &"large BCF fixture missing: {KgBcf}"
  let tmpDir = createTempDir("blocky_", "")
  let tmpl = tmpDir / "shard.{}.bcf"
  scatter(KgBcf, 4, tmpl, format = ffBcf)
  checkBcfShards(KgBcf, tmpl, 4)
  removeDir(tmpDir)

# ===========================================================================
# S15 — VCF scatter with header+data in same BGZF block (regression test)
# ===========================================================================

proc createMixedBlockVcf(path: string) =
  ## Create a VCF.gz where the first BGZF block contains both header lines
  ## and data records.  This triggers the duplicate-header bug when firstUOff
  ## is incorrectly set to 0.
  let header = "##fileformat=VCFv4.2\n" &
               "##contig=<ID=chr1,length=248956422>\n" &
               "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1\n"
  # Block 1: header + first 50 records (mixed block)
  var block1: seq[byte]
  for c in header: block1.add(byte(c))
  for i in 1..50:
    let rec = &"chr1\t{i * 1000}\t.\tA\tT\t50\tPASS\tDP=10\tGT\t0/1\n"
    for c in rec: block1.add(byte(c))
  let compressed1 = compressToBgzf(block1)
  # Blocks 2–10: data only (50 records each)
  var f = open(path, fmWrite)
  discard f.writeBytes(compressed1, 0, compressed1.len)
  for blk in 2..10:
    var content: seq[byte]
    for i in 1..50:
      let pos = ((blk - 1) * 50 + i) * 1000
      let rec = &"chr1\t{pos}\t.\tA\tT\t50\tPASS\tDP=10\tGT\t0/1\n"
      for c in rec: content.add(byte(c))
    let cb = compressToBgzf(content)
    discard f.writeBytes(cb, 0, cb.len)
  let eof = @BGZF_EOF
  discard f.writeBytes(eof, 0, eof.len)
  f.close()

# ---------------------------------------------------------------------------
# S15 — scatter with header+data sharing a BGZF block: no duplicate headers
# ---------------------------------------------------------------------------
timed("S15", "scatter: header+data in same block, no duplicate headers"):
  let tmpDir = createTempDir("blocky_", "")
  let fixturePath = tmpDir / "mixed.vcf.gz"
  createMixedBlockVcf(fixturePath)
  let tmpl = tmpDir / "shard.{}.vcf.gz"
  scatter(fixturePath, 2, tmpl, 1, forceScan = true)
  checkShards(fixturePath, tmpl, 2)
  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# S16 — scatter with forced pread/pwrite tier
# ---------------------------------------------------------------------------

timed("S16", "scatter pwrite tier: 4 shards, completeness, order"):
  gTierCopyFileRangeFailed.store(true, moRelaxed)
  gTierSendfileFailed.store(true, moRelaxed)
  let tmpDir = createTempDir("blocky_", "")
  let tmpl = tmpDir / "shard.{}.vcf.gz"
  scatter(SmallVcf, 4, tmpl)
  checkShards(SmallVcf, tmpl, 4)
  removeDir(tmpDir)
  gTierCopyFileRangeFailed.store(false, moRelaxed)
  gTierSendfileFailed.store(false, moRelaxed)
