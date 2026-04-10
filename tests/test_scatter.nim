## Tests for scatter.nim — index parsing (Step 3), boundary optimisation (Step 4),
## and shard writing (Step 5).
## Run from project root: nim c -r tests/test_scatter.nim

echo "--------------- Test Scatter ---------------"

import std/[algorithm, math, os, osproc, posix, strformat, strutils, tempfiles]
{.warning[Deprecated]: off.}
import std/threadpool
{.warning[Deprecated]: on.}
import test_utils
import "../src/vcfparty/vcf_utils"
import "../src/vcfparty/scatter"

const DataDir  = "tests/data"
const SmallVcf = DataDir / "small.vcf.gz"     # TBI indexed
const CsiVcf   = DataDir / "small_csi.vcf.gz" # CSI indexed only (no .tbi)
const SmallBcf = DataDir / "small.bcf"
const KgBcf    = DataDir / "chr22_1kg.bcf"

proc readMagic(path: string; offset: int64): array[3, byte] =
  let f = open(path, fmRead)
  defer: f.close()
  f.setFilePos(offset)
  discard readBytes(f, result, 0, 3)

# ===========================================================================
# SC1 — Index parsing: TBI and CSI virtual offsets
# ===========================================================================

# ---------------------------------------------------------------------------
# SC1.1 — parseTbiVirtualOffsets: voffs non-empty, sorted, block_off has BGZF magic
# ---------------------------------------------------------------------------
timed("SC1.1", "parseTbiVirtualOffsets: virtual offsets valid"):
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
# SC1.2 — readIndexVirtualOffsets falls back to TBI when no .csi present
# ---------------------------------------------------------------------------
timed("SC1.2", "readIndexVirtualOffsets via TBI"):
  let voffs = readIndexVirtualOffsets(SmallVcf)
  doAssert voffs.len > 0, "readIndexVirtualOffsets (TBI): no entries"

# ---------------------------------------------------------------------------
# SC1.3 — parseCsiVirtualOffsets: CSI-only fixture
# ---------------------------------------------------------------------------
timed("SC1.3", "parseCsiVirtualOffsets: virtual offsets valid"):
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
# SC1.4 — readIndexVirtualOffsets falls through to CSI when no .tbi present
# ---------------------------------------------------------------------------
timed("SC1.4", "readIndexVirtualOffsets via CSI"):
  let voffs = readIndexVirtualOffsets(CsiVcf)
  doAssert voffs.len > 0, "readIndexVirtualOffsets (CSI): no entries"

# ===========================================================================
# SC5–SC10 — Boundary computation: header extraction, lengths, partition, validation
# ===========================================================================

# ---------------------------------------------------------------------------
# SC5 — testGetHeaderAndFirstBlock: header is valid BGZF, starts with '#'; firstBlock has BGZF magic
# ---------------------------------------------------------------------------
timed("SC2.1", "getHeaderAndFirstBlock: header valid, firstBlock has BGZF magic"):
  let (hdrBytes, firstBlock) = getHeaderAndFirstBlock(SmallVcf)
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

# ---------------------------------------------------------------------------
# SC6 — testGetLengths: converts block starts to cumulative lengths correctly
# ---------------------------------------------------------------------------
timed("SC2.2", "getLengths: cumulative lengths correct"):
  let starts: seq[int64] = @[0'i64, 100, 300, 700]
  let lengths = getLengths(starts, 1000)
  doAssert lengths == @[100'i64, 200, 400, 300],
    &"getLengths: expected [100,200,400,300] got {lengths}"

# SC3.1-SC3.4 (partitionBoundaries, isValidBoundary, optimiseBoundaries) removed:
# these procs no longer exist after Milestone V — scatter now uses index virtual
# offsets directly, eliminating boundary search. End-to-end scatter correctness
# is covered by SC11+ (checkShards) and CL10+ in test_cli.nim.

# ===========================================================================
# SC11–SC15 — VCF scatter end-to-end (TBI, CSI, --force-scan)
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
# SC12 — testScatter4ShardsTbi: 4 shards (TBI); BGZF structure, completeness, order, size balance
# ---------------------------------------------------------------------------
timed("SC5.1", "scatter TBI: 4 shards, completeness, order, balance"):
  let tmpDir = createTempDir("vcfparty_", "")
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
# SC14 — testScatterForceScan: forceScan=true ignores index; result matches indexed scatter
# ---------------------------------------------------------------------------
timed("SC5.2", "scatter --force-scan: completeness, order"):
  ## scatter with forceScan=true on a fully indexed file — index is ignored.
  let tmpDir = createTempDir("vcfparty_", "")
  let tmpl = tmpDir / "shard.{}.vcf.gz"
  scatter(SmallVcf, 4, tmpl, 1, forceScan = true)
  checkShards(SmallVcf, tmpl, 4)
  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# SC15 — testScatter4ShardsCsi: 4 shards (CSI); BGZF structure, completeness, order
# ---------------------------------------------------------------------------
timed("SC5.3", "scatter CSI: 4 shards, completeness, order"):
  doAssert fileExists(CsiVcf & ".csi"), "CSI fixture missing — run generate_fixtures.sh"
  let tmpDir = createTempDir("vcfparty_", "")
  let tmpl = tmpDir / "shard.{}.vcf.gz"
  scatter(CsiVcf, 4, tmpl)
  checkShards(CsiVcf, tmpl, 4)
  removeDir(tmpDir)

# ===========================================================================
# SC16–SC20 — BCF: header extraction and scatter end-to-end
# ===========================================================================

proc leU32At(data: seq[byte]; pos: int): uint32 =
  data[pos].uint32 or (data[pos+1].uint32 shl 8) or
  (data[pos+2].uint32 shl 16) or (data[pos+3].uint32 shl 24)

# ---------------------------------------------------------------------------
# SC16 — testExtractBcfHeaderSmall: BGZF magic, BCF magic, l_text > 0, total decompressed == 5+4+l_text
# ---------------------------------------------------------------------------
timed("SC6.1", "extractBcfHeader: small.bcf"):
  let hdrBytes = extractBcfHeader(SmallBcf)
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
# SC17 — testExtractBcfHeaderLarge: large BCF (2504 samples); multi-block header decompresses correctly
# ---------------------------------------------------------------------------
timed("SC6.2", "extractBcfHeader: chr22_1kg.bcf large header"):
  # chr22_1kg.bcf has 2504 samples — verify extractBcfHeader handles it correctly.
  doAssert fileExists(KgBcf), &"large BCF fixture missing: {KgBcf}"
  let hdrBytes = extractBcfHeader(KgBcf)
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
# SC19 — testBcfScatter4Shards: 4 BCF shards; BGZF, BCF magic, completeness, order, size balance
# ---------------------------------------------------------------------------
timed("SC7.1", "BCF scatter: 4 shards, completeness, order, balance"):
  doAssert fileExists(SmallBcf), &"BCF fixture missing: {SmallBcf}"
  let tmpDir = createTempDir("vcfparty_", "")
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
# SC20 — testBcfScatterLargeHeader: 4 BCF shards from 1KG (large header); completeness and order
# ---------------------------------------------------------------------------
timed("SC7.2", "BCF scatter: chr22_1kg.bcf large header, 4 shards"):
  doAssert fileExists(KgBcf), &"large BCF fixture missing: {KgBcf}"
  let tmpDir = createTempDir("vcfparty_", "")
  let tmpl = tmpDir / "shard.{}.bcf"
  scatter(KgBcf, 4, tmpl, format = ffBcf)
  checkBcfShards(KgBcf, tmpl, 4)
  removeDir(tmpDir)

# ===========================================================================
# SC8 — interleavedBlockAssignment: round-robin chunk assignment
# ===========================================================================

proc allBlocksCovered(assignment: seq[seq[Slice[int]]]; nBlocks: int): bool =
  ## Verify every block index 0..<nBlocks appears exactly once across all shards.
  var seen = newSeq[int](nBlocks)
  for shardSlices in assignment:
    for s in shardSlices:
      for i in s:
        if i < 0 or i >= nBlocks: return false
        inc seen[i]
  for c in seen:
    if c != 1: return false
  true

# ---------------------------------------------------------------------------
# SC8.1 — every block assigned exactly once
# ---------------------------------------------------------------------------
timed("SC8.1", "interleavedBlockAssignment: every block assigned exactly once"):
  let a = interleavedBlockAssignment(20, 4, 3)
  doAssert a.len == 4, "SC8.1: expected 4 shards"
  doAssert allBlocksCovered(a, 20), "SC8.1: not all blocks covered exactly once"

# ---------------------------------------------------------------------------
# SC8.2 — round-robin order correct
# ---------------------------------------------------------------------------
timed("SC8.2", "interleavedBlockAssignment: round-robin order correct"):
  # 12 blocks, 3 shards, K=2 → chunks: [0..1]=s0, [2..3]=s1, [4..5]=s2,
  #                                      [6..7]=s0, [8..9]=s1, [10..11]=s2
  let a = interleavedBlockAssignment(12, 3, 2)
  doAssert a[0] == @[0..1, 6..7],   "SC8.2: shard 0 wrong"
  doAssert a[1] == @[2..3, 8..9],   "SC8.2: shard 1 wrong"
  doAssert a[2] == @[4..5, 10..11], "SC8.2: shard 2 wrong"

# ---------------------------------------------------------------------------
# SC8.3 — K=1 single-block chunks
# ---------------------------------------------------------------------------
timed("SC8.3", "interleavedBlockAssignment: K=1 single-block chunks"):
  let a = interleavedBlockAssignment(5, 3, 1)
  doAssert a[0] == @[0..0, 3..3], "SC8.3: shard 0 wrong"
  doAssert a[1] == @[1..1, 4..4], "SC8.3: shard 1 wrong"
  doAssert a[2] == @[2..2],       "SC8.3: shard 2 wrong"
  doAssert allBlocksCovered(a, 5), "SC8.3: coverage check failed"

# ---------------------------------------------------------------------------
# SC8.4 — nBlocks < nShards (some shards empty)
# ---------------------------------------------------------------------------
timed("SC8.4", "interleavedBlockAssignment: nBlocks < nShards, some shards empty"):
  let a = interleavedBlockAssignment(2, 5, 1)
  doAssert a[0] == @[0..0], "SC8.4: shard 0 wrong"
  doAssert a[1] == @[1..1], "SC8.4: shard 1 wrong"
  doAssert a[2].len == 0,   "SC8.4: shard 2 should be empty"
  doAssert a[3].len == 0,   "SC8.4: shard 3 should be empty"
  doAssert a[4].len == 0,   "SC8.4: shard 4 should be empty"
  doAssert allBlocksCovered(a, 2), "SC8.4: coverage check failed"

# ---------------------------------------------------------------------------
# SC8.5 — nBlocks not divisible by K (last chunk smaller)
# ---------------------------------------------------------------------------
timed("SC8.5", "interleavedBlockAssignment: last chunk smaller than K"):
  # 10 blocks, 2 shards, K=3 → chunks: [0..2]=s0, [3..5]=s1, [6..8]=s0, [9..9]=s1
  let a = interleavedBlockAssignment(10, 2, 3)
  doAssert a[0] == @[0..2, 6..8], "SC8.5: shard 0 wrong"
  doAssert a[1] == @[3..5, 9..9], "SC8.5: shard 1 wrong (last chunk should be 1 block)"
  doAssert allBlocksCovered(a, 10), "SC8.5: coverage check failed"

# ===========================================================================
# SC9 — writeInterleavedShard: inbox model integration tests
# ===========================================================================

proc countRecords(path: string): int =
  let (o, _) = execCmdEx("bcftools view -HG " & path & " 2>/dev/null | wc -l")
  o.strip.parseInt

proc writeInterleavedShards(vcfPath: string; nShards, chunkSize: int;
                             tmpDir: string; fmt: FileFormat): seq[string] =
  ## Helper: run interleaved scatter into temp files, return shard paths.
  let fileSize = getFileSize(vcfPath)
  var headerBytes: seq[byte]
  var starts: seq[int64]
  var csiVoffs: seq[(int64, int)]

  if fmt == ffBcf:
    let (hdr, firstDataBlockOff, uOff) = extractBcfHeaderAndFirstOffset(vcfPath)
    headerBytes = hdr
    starts = scanBgzfBlockStarts(vcfPath, startAt = firstDataBlockOff,
                                 endAt = fileSize - 28)
    let csi = vcfPath & ".csi"
    csiVoffs = parseCsiVirtualOffsets(csi)
    let firstVO = (firstDataBlockOff, uOff)
    if firstVO notin csiVoffs: csiVoffs.add(firstVO)
    csiVoffs.sort(proc(a, b: (int64, int)): int =
      if a[0] != b[0]: cmp(a[0], b[0]) else: cmp(a[1], b[1]))
  else:
    let (hb, fb) = getHeaderAndFirstBlock(vcfPath)
    headerBytes = decompressBgzfBytes(hb)
    starts = scanBgzfBlockStarts(vcfPath, startAt = fb, endAt = fileSize - 28)

  var sizes = getLengths(starts, fileSize)
  let assignment = interleavedBlockAssignment(starts.len, nShards, chunkSize)
  var inboxes = newInboxArray(nShards)

  setMaxPoolSize(nShards)
  result = newSeq[string](nShards)
  var fvs = newSeq[FlowVar[int]](nShards)
  for i in 0 ..< nShards:
    let ext = if fmt == ffBcf: ".bcf" else: ".vcf"
    result[i] = tmpDir / &"shard_{i+1}{ext}"
    let fd = posix.open(result[i].cstring,
                        O_WRONLY or O_CREAT or O_TRUNC, 0o666.Mode)
    doAssert fd >= 0, &"SC9: could not create {result[i]}"
    var task = InterleavedTask(
      vcfPath: vcfPath, outFd: fd,
      headerBytes: headerBytes,
      blockStarts: addr starts, blockSizes: addr sizes,
      chunkIndices: assignment[i], format: fmt,
      csiVoffs: if fmt == ffBcf: addr csiVoffs else: nil,
      shardIdx: i, nShards: nShards, chunkSize: chunkSize,
      inboxes: addr inboxes)
    fvs[i] = spawn writeInterleavedShard(task)
  for fv in fvs:
    discard ^fv
  freeInboxArray(inboxes)

# ---------------------------------------------------------------------------
# SC9.1 — 1 shard interleaved = all records present (VCF)
# ---------------------------------------------------------------------------
timed("SC9.1", "writeInterleavedShard: 1 shard VCF"):
  let tmpDir = createTempDir("vcfparty_", "")
  let paths = writeInterleavedShards(SmallVcf, 1, 3, tmpDir, ffVcf)
  let orig = countRecords(SmallVcf)
  let got  = countRecords(paths[0])
  doAssert got == orig, &"SC9.1: record count mismatch: got {got}, expected {orig}"
  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# SC9.2 — 4 shards interleaved, all records present (VCF)
# ---------------------------------------------------------------------------
timed("SC9.2", "writeInterleavedShard: 4 shards VCF"):
  let tmpDir = createTempDir("vcfparty_", "")
  let paths = writeInterleavedShards(SmallVcf, 4, 2, tmpDir, ffVcf)
  let orig = countRecords(SmallVcf)
  var total = 0
  for p in paths: total += countRecords(p)
  doAssert total == orig,
    &"SC9.2: record count mismatch: got {total}, expected {orig}"
  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# SC9.3 — BCF 4 shards interleaved, all records present
# ---------------------------------------------------------------------------
timed("SC9.3", "writeInterleavedShard: 4 shards BCF"):
  let tmpDir = createTempDir("vcfparty_", "")
  let paths = writeInterleavedShards(SmallBcf, 4, 2, tmpDir, ffBcf)
  let orig = countRecords(SmallBcf)
  var total = 0
  for p in paths: total += countRecords(p)
  doAssert total == orig,
    &"SC9.3: BCF record count mismatch: got {total}, expected {orig}"
  removeDir(tmpDir)

# ---------------------------------------------------------------------------
# SC9.4 — K=1 forces maximum chunk boundaries; still all records present
# ---------------------------------------------------------------------------
timed("SC9.4", "writeInterleavedShard: K=1, 4 shards VCF"):
  let tmpDir = createTempDir("vcfparty_", "")
  let paths = writeInterleavedShards(SmallVcf, 4, 1, tmpDir, ffVcf)
  let orig = countRecords(SmallVcf)
  var total = 0
  for p in paths: total += countRecords(p)
  doAssert total == orig,
    &"SC9.4: K=1 record count mismatch: got {total}, expected {orig}"
  removeDir(tmpDir)
