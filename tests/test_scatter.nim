## Tests for scatter.nim — index parsing (Step 3) + boundary optimisation (Step 4).
## Run from project root: nim c -r tests/test_scatter.nim

import std/[algorithm, os, sequtils, strformat]
import "../src/paravar/bgzf_utils"
import "../src/paravar/scatter"

const DataDir   = "tests/data"
const SmallVcf  = DataDir / "small.vcf.gz"
const SingleVcf = DataDir / "single_chrom.vcf.gz"
const TinyVcf   = DataDir / "tiny.vcf.gz"

proc readMagic(path: string; offset: int64): array[3, byte] =
  let f = open(path, fmRead)
  defer: f.close()
  f.setFilePos(offset)
  discard readBytes(f, result, 0, 3)

# ===========================================================================
# Step 3 — Index parsing
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

block testReadIndexBlockStarts:
  let starts = readIndexBlockStarts(SmallVcf)
  doAssert starts.len > 0, "readIndexBlockStarts: no blocks"
  for i in 1 ..< starts.len:
    doAssert starts[i] > starts[i-1], "readIndexBlockStarts: not sorted"
  echo &"PASS readIndexBlockStarts ({starts.len} blocks)"

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

echo ""
echo "All scatter tests passed."
