## Tests for scatter.nim — index parsing (Step 3), boundary optimisation (Step 4),
## and shard writing (Step 5).
## Run from project root: nim c -r tests/test_scatter.nim

import std/[algorithm, os, strformat, strutils]
import "../src/paravar/bgzf_utils"
import "../src/paravar/scatter"

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
# Step 3 — Index parsing: TBI
# ===========================================================================

block testParseTbi:
  let starts = parseTbiBlockStarts(SmallVcf & ".tbi")
  doAssert starts.len > 0, "parseTbiBlockStarts: no blocks"
  for i in 1 ..< starts.len:
    doAssert starts[i] > starts[i-1], "parseTbiBlockStarts: not strictly increasing"
  for off in starts:
    let magic = readMagic(SmallVcf, off)
    doAssert magic[0] == 0x1f and magic[1] == 0x8b,
      &"bad BGZF magic at offset {off}"
  echo &"PASS parseTbiBlockStarts ({starts.len} blocks)"

block testReadIndexBlockStartsTbi:
  let starts = readIndexBlockStarts(SmallVcf)
  doAssert starts.len > 0, "readIndexBlockStarts (TBI): no blocks"
  for i in 1 ..< starts.len:
    doAssert starts[i] > starts[i-1], "readIndexBlockStarts (TBI): not sorted"
  echo &"PASS readIndexBlockStarts via TBI ({starts.len} blocks)"

# ===========================================================================
# Step 3 — Index parsing: CSI
# ===========================================================================

block testParseCsi:
  doAssert fileExists(CsiVcf & ".csi"), "CSI fixture missing — run generate_fixtures.sh"
  doAssert not fileExists(CsiVcf & ".tbi"), "CSI fixture must not have a .tbi alongside it"
  let starts = parseCsiBlockStarts(CsiVcf & ".csi")
  doAssert starts.len > 0, "parseCsiBlockStarts: no blocks"
  for i in 1 ..< starts.len:
    doAssert starts[i] > starts[i-1], "parseCsiBlockStarts: not strictly increasing"
  for off in starts:
    let magic = readMagic(CsiVcf, off)
    doAssert magic[0] == 0x1f and magic[1] == 0x8b,
      &"bad BGZF magic at offset {off}"
  echo &"PASS parseCsiBlockStarts ({starts.len} blocks)"

block testReadIndexBlockStartsCsi:
  let starts = readIndexBlockStarts(CsiVcf)   # must fall through to .csi
  doAssert starts.len > 0, "readIndexBlockStarts (CSI): no blocks"
  for i in 1 ..< starts.len:
    doAssert starts[i] > starts[i-1], "readIndexBlockStarts (CSI): not sorted"
  echo &"PASS readIndexBlockStarts via CSI ({starts.len} blocks)"

# ===========================================================================
# Step 4 — Header extraction
# ===========================================================================

block testGetHeaderAndFirstBlock:
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
  echo &"PASS getHeaderAndFirstBlock (firstBlock={firstBlock})"

# ===========================================================================
# Step 4 — getLengths / partitionBoundaries
# ===========================================================================

block testGetLengths:
  let starts: seq[int64] = @[0'i64, 100, 300, 700]
  let lengths = getLengths(starts, 1000)
  doAssert lengths == @[100'i64, 200, 400, 300],
    &"getLengths: expected [100,200,400,300] got {lengths}"
  echo "PASS getLengths"

block testPartitionBoundaries2:
  # 4 equal blocks → split into 2 shards → boundary at index 1 (bisect_left on cumsum)
  let lengths: seq[int64] = @[100'i64, 100, 100, 100]
  let bounds = partitionBoundaries(lengths, 2)
  doAssert bounds.len == 1, &"partitionBoundaries 2: expected 1 bound, got {bounds.len}"
  doAssert bounds[0] == 1, &"partitionBoundaries 2: expected index 1, got {bounds[0]}"
  echo "PASS partitionBoundaries (2 shards)"

block testPartitionBoundaries4:
  # 8 equal blocks → 4 shards → boundaries at 1, 3, 5 (bisect_left on cumsum)
  let lengths: seq[int64] = @[100'i64, 100, 100, 100, 100, 100, 100, 100]
  let bounds = partitionBoundaries(lengths, 4)
  doAssert bounds.len == 3, &"partitionBoundaries 4: expected 3 bounds, got {bounds.len}"
  doAssert bounds == @[1, 3, 5], &"partitionBoundaries 4: expected [1,3,5] got {bounds}"
  echo "PASS partitionBoundaries (4 shards)"

# ===========================================================================
# Step 4 — isValidBoundary
# ===========================================================================

block testIsValidBoundary:
  # Every non-EOF data block in small.vcf.gz should be valid (contains >= 2 lines).
  let allStarts = scanBgzfBlockStarts(SmallVcf)
  var validCount = 0
  var buf = newSeq[byte](18)
  let f = open(SmallVcf, fmRead)
  for off in allStarts:
    f.setFilePos(off)
    discard readBytes(f, buf, 0, 18)
    let sz = bgzfBlockSize(buf)
    if sz == 28: continue   # skip EOF block
    let fileSize = getFileSize(SmallVcf)
    let blockLen = if off + sz.int64 < fileSize: sz.int64
                   else: fileSize - off
    if isValidBoundary(SmallVcf, off, blockLen):
      validCount += 1
  f.close()
  doAssert validCount > 0, "isValidBoundary: no valid blocks found"
  echo &"PASS isValidBoundary ({validCount} valid blocks)"

# ===========================================================================
# Step 4 — optimiseBoundaries end-to-end
# ===========================================================================

block testOptimiseBoundaries4:
  var starts = readIndexBlockStarts(SmallVcf)
  let (_, firstBlock) = getHeaderAndFirstBlock(SmallVcf)
  # Mirror Python: add first_block and scan fine-grained sub-blocks.
  if firstBlock notin starts: starts.add(firstBlock)
  starts.sort()
  if starts.len >= 2:
    for off in scanBgzfBlockStarts(SmallVcf, starts[0], starts[1]):
      if off notin starts: starts.add(off)
    starts.sort()
  let (bounds, finalStarts, lengths) = optimiseBoundaries(SmallVcf, starts, 4)
  doAssert bounds.len == 3, &"optimiseBoundaries: expected 3 bounds, got {bounds.len}"
  # Each boundary block must be valid
  for bi in bounds:
    doAssert isValidBoundary(SmallVcf, finalStarts[bi], lengths[bi]),
      &"optimiseBoundaries: boundary at {finalStarts[bi]} is invalid"
  # Lengths must be non-zero
  for l in lengths:
    doAssert l > 0, "optimiseBoundaries: zero-length block"
  echo &"PASS optimiseBoundaries 4-shard ({finalStarts.len} fine blocks)"

# ===========================================================================
# Step 5 — scatter end-to-end helpers
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

proc checkShards(vcfPath: string; prefix: string; n: int) =
  ## Verify BGZF structure, header presence, record completeness, and order.
  let nDigits = len($n)

  # 1 — each output file exists and has valid BGZF magic + EOF block
  for i in 1..n:
    let path = prefix & "." & align($i, nDigits, '0') & ".vcf.gz"
    doAssert fileExists(path), &"shard {i} missing: {path}"
    let fz = open(path, fmRead)
    var hdrBuf = newSeq[byte](3)
    discard readBytes(fz, hdrBuf, 0, 3)
    doAssert hdrBuf[0] == 0x1f'u8 and hdrBuf[1] == 0x8b'u8,
      &"shard {i}: bad BGZF magic"
    let sz = getFileSize(path)
    fz.setFilePos(sz - 28)
    var eofBuf = newSeq[byte](28)
    discard readBytes(fz, eofBuf, 0, 28)
    fz.close()
    doAssert eofBuf == @BGZF_EOF, &"shard {i}: EOF block mismatch"

  # 2 — each shard starts with a VCF header
  for i in 1..n:
    let path = prefix & "." & align($i, nDigits, '0') & ".vcf.gz"
    let content = decompressBgzfFile(path)
    doAssert content.len > 0, &"shard {i}: empty after decompression"
    doAssert content[0] == byte('#'), &"shard {i}: does not start with '#'"

  # 3 — all records present, no duplicates
  var shardRecords: seq[string]
  for i in 1..n:
    let path = prefix & "." & align($i, nDigits, '0') & ".vcf.gz"
    shardRecords.add(collectRecords(decompressBgzfFile(path)))
  let origRecords = collectRecords(decompressBgzfFile(vcfPath))
  doAssert shardRecords.len == origRecords.len,
    &"record count mismatch: shards={shardRecords.len} orig={origRecords.len}"
  doAssert sorted(shardRecords) == sorted(origRecords),
    "shard records do not match original"

  # 4 — records within each shard are in non-decreasing genomic order (CHROM:POS)
  for i in 1..n:
    let path = prefix & "." & align($i, nDigits, '0') & ".vcf.gz"
    let recs = collectRecords(decompressBgzfFile(path))
    for j in 1 ..< recs.len:
      let prevFields = recs[j-1].split('\t')
      let curFields  = recs[j].split('\t')
      if prevFields.len >= 2 and curFields.len >= 2:
        if prevFields[0] == curFields[0]:
          doAssert prevFields[1].parseInt <= curFields[1].parseInt,
            &"shard {i}: records out of order at line {j}"

# ===========================================================================
# Step 5 — scatter: TBI-indexed input
# ===========================================================================

block testScanAllBlockStarts:
  let (_, firstBlock) = getHeaderAndFirstBlock(SmallVcf)
  let starts = scanAllBlockStarts(SmallVcf, firstBlock)
  doAssert starts.len > 0, "scanAllBlockStarts: no data blocks found"
  for off in starts:
    let magic = readMagic(SmallVcf, off)
    doAssert magic[0] == 0x1f and magic[1] == 0x8b,
      &"scanAllBlockStarts: bad BGZF magic at offset {off}"
  for i in 1 ..< starts.len:
    doAssert starts[i] > starts[i-1], "scanAllBlockStarts: not strictly increasing"
  echo &"PASS scanAllBlockStarts ({starts.len} data blocks, firstBlock={firstBlock})"

block testScatter4ShardsTbi:
  let tmpDir = getTempDir() / "paravar_scatter_tbi_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"
  scatter(SmallVcf, 4, prefix)
  checkShards(SmallVcf, prefix, 4)

  var sizes: seq[int64]
  for i in 1..4:
    sizes.add(getFileSize(prefix & "." & $i & ".vcf.gz"))
  let minSz = sizes.min(); let maxSz = sizes.max()
  doAssert minSz > 0, "scatter (TBI): at least one shard is empty"
  doAssert maxSz.float / minSz.float < 2.0,
    &"scatter (TBI): shard size imbalance: max={maxSz} min={minSz}"

  echo "PASS scatter TBI: BGZF structure, header, completeness, order, balance"
  removeDir(tmpDir)

# ===========================================================================
# Step 5 — scatter: CSI-indexed input
# ===========================================================================

block testVcfScatter1Shard:
  ## 1 shard must equal the original (record set and order).
  let tmpDir = getTempDir() / "paravar_scatter_vcf_1shard_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"
  scatter(SmallVcf, 1, prefix)
  checkShards(SmallVcf, prefix, 1)
  echo "PASS VCF scatter 1 shard: BGZF structure, header, completeness"
  removeDir(tmpDir)

block testScatterNoIndexAutoScan:
  ## With no index alongside the file, scatter should warn and auto-scan.
  let tmpDir = getTempDir() / "paravar_scatter_scan_test"
  createDir(tmpDir)
  let tmpVcf = tmpDir / "noindex.vcf.gz"
  copyFile(SmallVcf, tmpVcf)
  # No .tbi or .csi copied — must fall through to auto-scan.
  let prefix = tmpDir / "shard"
  scatter(tmpVcf, 4, prefix)
  checkShards(SmallVcf, prefix, 4)
  echo "PASS scatter no-index auto-scan: BGZF structure, header, completeness, order"
  removeDir(tmpDir)

block testScatterForceScan:
  ## scatter with forceScan=true on a fully indexed file — index is ignored.
  let tmpDir = getTempDir() / "paravar_scatter_forcescan_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"
  scatter(SmallVcf, 4, prefix, 1, forceScan = true)
  checkShards(SmallVcf, prefix, 4)
  echo "PASS scatter --force-scan: BGZF structure, header, completeness, order"
  removeDir(tmpDir)

block testScatter4ShardsCsi:
  doAssert fileExists(CsiVcf & ".csi"), "CSI fixture missing — run generate_fixtures.sh"
  let tmpDir = getTempDir() / "paravar_scatter_csi_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"
  scatter(CsiVcf, 4, prefix)
  checkShards(CsiVcf, prefix, 4)
  echo "PASS scatter CSI: BGZF structure, header, completeness, order"
  removeDir(tmpDir)

# ===========================================================================
# Step B3 — BCF header extraction
# ===========================================================================

proc leU32At(data: seq[byte]; pos: int): uint32 =
  data[pos].uint32 or (data[pos+1].uint32 shl 8) or
  (data[pos+2].uint32 shl 16) or (data[pos+3].uint32 shl 24)

block testExtractBcfHeaderSmall:
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
  echo &"PASS extractBcfHeader small.bcf (l_text={lText})"

block testExtractBcfHeaderLarge:
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
  echo &"PASS extractBcfHeader chr22_1kg.bcf (l_text={lText})"

# ===========================================================================
# Step B4 — BCF scatter end-to-end
# ===========================================================================

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

proc checkBcfShards(bcfPath: string; prefix: string; n: int) =
  ## Verify BGZF structure, BCF magic, record completeness, and order.
  let nDigits = len($n)

  # 1 — each shard exists with .bcf extension, valid BGZF magic, BGZF EOF
  for i in 1..n:
    let path = prefix & "." & align($i, nDigits, '0') & ".bcf"
    doAssert fileExists(path), &"BCF shard {i} missing: {path}"
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

  # 2 — each shard decompresses and starts with BCF magic
  for i in 1..n:
    let path = prefix & "." & align($i, nDigits, '0') & ".bcf"
    let data = decompressBgzfFile(path)
    doAssert data.len >= 9, &"BCF shard {i}: decompressed data too short"
    doAssert data[0] == byte('B') and data[1] == byte('C') and
             data[2] == byte('F') and data[3] == 0x02'u8 and
             data[4] == 0x02'u8, &"BCF shard {i}: bad BCF magic"

  # 3 — all records present across shards, no duplicates
  var shardRecs: seq[seq[byte]]
  for i in 1..n:
    let path = prefix & "." & align($i, nDigits, '0') & ".bcf"
    shardRecs.add(collectBcfRecordBytes(path))
  let origRecs = collectBcfRecordBytes(bcfPath)
  doAssert shardRecs.len == origRecs.len,
    &"BCF: record count mismatch: shards={shardRecs.len} orig={origRecs.len}"
  doAssert sorted(shardRecs, cmpRecBytes) == sorted(origRecs, cmpRecBytes),
    "BCF: shard records do not match original"

  # 4 — records within each shard in non-decreasing genomic order (chrom_idx, pos)
  for i in 1..n:
    let path = prefix & "." & align($i, nDigits, '0') & ".bcf"
    let recs = collectBcfRecordBytes(path)
    for j in 1 ..< recs.len:
      if recs[j].len >= 16 and recs[j-1].len >= 16:
        let prevChrom = leU32At(recs[j-1], 8)
        let curChrom  = leU32At(recs[j], 8)
        let prevPos   = leU32At(recs[j-1], 12)
        let curPos    = leU32At(recs[j], 12)
        if prevChrom == curChrom:
          doAssert prevPos <= curPos,
            &"BCF shard {i}: records out of order at record {j}"

block testBcfScatter1Shard:
  doAssert fileExists(SmallBcf), &"BCF fixture missing: {SmallBcf}"
  let tmpDir = getTempDir() / "paravar_bcf_1shard_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"
  scatter(SmallBcf, 1, prefix, format = FileFormat.Bcf)
  checkBcfShards(SmallBcf, prefix, 1)
  echo "PASS BCF scatter 1 shard: BGZF, BCF magic, completeness"
  removeDir(tmpDir)

block testBcfScatter4Shards:
  doAssert fileExists(SmallBcf), &"BCF fixture missing: {SmallBcf}"
  let tmpDir = getTempDir() / "paravar_bcf_4shard_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"
  scatter(SmallBcf, 4, prefix, format = FileFormat.Bcf)
  checkBcfShards(SmallBcf, prefix, 4)
  var sizes: seq[int64]
  for i in 1..4:
    sizes.add(getFileSize(prefix & "." & $i & ".bcf"))
  let minSz = sizes.min(); let maxSz = sizes.max()
  doAssert minSz > 0, "BCF scatter 4 shards: at least one shard is empty"
  doAssert maxSz.float / minSz.float < 2.0,
    &"BCF scatter 4 shards: shard size imbalance: max={maxSz} min={minSz}"
  echo "PASS BCF scatter 4 shards: BGZF, BCF magic, completeness, order, balance"
  removeDir(tmpDir)

block testBcfScatterLargeHeader:
  doAssert fileExists(KgBcf), &"large BCF fixture missing: {KgBcf}"
  let tmpDir = getTempDir() / "paravar_bcf_kg_test"
  createDir(tmpDir)
  let prefix = tmpDir / "shard"
  scatter(KgBcf, 4, prefix, format = FileFormat.Bcf)
  checkBcfShards(KgBcf, prefix, 4)
  echo "PASS BCF scatter chr22_1kg.bcf 4 shards: large header handled correctly"
  removeDir(tmpDir)

echo ""
echo "All scatter tests passed."
