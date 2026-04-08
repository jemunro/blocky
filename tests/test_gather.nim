## Tests for gather.nim — G1: types, inferFileFormat, validateGatherConfig.
## Run from project root: nim c -d:debug -r tests/test_gather.nim

import std/[os, osproc, options, strformat, strutils]
import std/posix
import "../src/vcfparty/gather"
import "../src/vcfparty/vcf_utils"

const DataDir  = "tests/data"
const SmallVcf = DataDir / "small.vcf.gz"
const SmallBcf = DataDir / "small.bcf"

# ---------------------------------------------------------------------------
# Special exit-test modes for subprocess-based exit-1 testing.
# These execute only when the compiled binary is invoked with a special arg;
# during normal test runs (nim c -r ...) paramCount() == 0.
# ---------------------------------------------------------------------------

if paramCount() >= 1:
  case paramStr(1)
  of "--exit-test-bad-override":
    discard inferFileFormat("output.vcf.gz", "bam")
  of "--exit-test-mutual-exclusion":
    let cfg = GatherConfig(headerPattern: some("#"), headerN: some(1))
    validateGatherConfig(cfg)
  of "--exit-test-vcf-header-flag":
    let cfg = GatherConfig(format: ffVcf, headerPattern: some("#"))
    validateGatherConfig(cfg)
  of "--exit-test-bcf-header-flag":
    let cfg = GatherConfig(format: ffBcf, headerN: some(1))
    validateGatherConfig(cfg)
  else: discard

# ---------------------------------------------------------------------------
# G1.1 — testInferAllExtensions: all valid extensions (happy paths)
# ---------------------------------------------------------------------------

block testInferAllExtensions:
  block:
    let (f, c) = inferFileFormat("out.vcf.gz", "")
    doAssert f == ffVcf,          &".vcf.gz: expected ffVcf, got {f}"
    doAssert c == compBgzf,         &".vcf.gz: expected compBgzf, got {c}"
  block:
    let (f, c) = inferFileFormat("out.vcf.bgz", "")
    doAssert f == ffVcf,          &".vcf.bgz: expected ffVcf, got {f}"
    doAssert c == compBgzf,         &".vcf.bgz: expected compBgzf, got {c}"
  block:
    let (f, c) = inferFileFormat("out.bcf", "")
    doAssert f == ffBcf,          &".bcf: expected ffBcf, got {f}"
    doAssert c == compBgzf,         &".bcf: expected compBgzf, got {c}"
  block:
    let (f, c) = inferFileFormat("out.vcf", "")
    doAssert f == ffVcf,          &".vcf: expected ffVcf, got {f}"
    doAssert c == compNone, &".vcf: expected compNone, got {c}"
  block:
    let (f, c) = inferFileFormat("out.txt.gz", "")
    doAssert f == ffText,         &".txt.gz: expected ffText, got {f}"
    doAssert c == compBgzf,         &".txt.gz: expected compBgzf, got {c}"
  block:
    let (f, c) = inferFileFormat("out.txt", "")
    doAssert f == ffText,         &".txt: expected ffText, got {f}"
    doAssert c == compNone, &".txt: expected compNone, got {c}"
  block:
    let (f, c) = inferFileFormat("out.out.gz", "")
    doAssert f == ffText,         &"any.gz: expected ffText, got {f}"
    doAssert c == compBgzf,         &"any.gz: expected compBgzf, got {c}"
  block:
    let (f, c) = inferFileFormat("out.bgz", "")
    doAssert f == ffText,         &".bgz: expected ffText, got {f}"
    doAssert c == compBgzf,         &".bgz: expected compBgzf, got {c}"
  block:
    let (f, c) = inferFileFormat("out.xyz", "")
    doAssert f == ffText,         &".xyz: expected ffText (unknown ext), got {f}"
    doAssert c == compNone, &".xyz: expected compNone, got {c}"
  echo "PASS G1.1 inferFileFormat: all extensions"

# ---------------------------------------------------------------------------
# G1.2 — testInferWithOverride: fmtOverride overrides format, compression from extension
# ---------------------------------------------------------------------------

block testInferWithOverride:
  # Override format; compression still from path extension.
  block:
    let (f, c) = inferFileFormat("out.vcf.gz", "bcf")
    doAssert f == ffBcf and c == compBgzf,
      &".vcf.gz + --gather-fmt bcf: got ({f}, {c})"
  block:
    let (f, c) = inferFileFormat("out.vcf.gz", "txt")
    doAssert f == ffText and c == compBgzf,
      &".vcf.gz + --gather-fmt txt: got ({f}, {c})"
  # Unknown extension with valid override → format from override, compression compNone.
  block:
    let (f, c) = inferFileFormat("out.xyz", "vcf")
    doAssert f == ffVcf and c == compNone,
      &".xyz + --gather-fmt vcf: got ({f}, {c})"
  echo "PASS G1.2 inferFileFormat: fmtOverride overrides format"

# ---------------------------------------------------------------------------
# G1.3 — testValidateConfigOk: non-conflicting configs pass
# ---------------------------------------------------------------------------

block testValidateConfigOk:
  validateGatherConfig(GatherConfig())                                              # neither set
  validateGatherConfig(GatherConfig(format: ffText, headerPattern: some("#")))     # text + pattern
  validateGatherConfig(GatherConfig(format: ffText, headerN: some(3)))             # text + n
  echo "PASS G1.3 validateGatherConfig: non-conflicting configs pass"

# ---------------------------------------------------------------------------
# G1 — exit-1 paths via subprocess (G1.4–G1.7)
# ---------------------------------------------------------------------------

const SelfSrc   = "tests/test_gather.nim"
const HelperBin = "/tmp/vcfparty_test_gather_helper"

block buildHelper:
  let (outp, code) = execCmdEx(
    "nim c -d:debug -o:" & HelperBin & " " & SelfSrc & " 2>&1")
  doAssert code == 0, "failed to compile test helper:\n" & outp
  echo "PASS G1.0 buildHelper: compiled exit-1 test helper"

# ---------------------------------------------------------------------------
# G1.4 — testInferBadOverride: invalid fmtOverride exits 1
# ---------------------------------------------------------------------------

block testInferBadOverride:
  let (_, code) = execCmdEx(HelperBin & " --exit-test-bad-override 2>/dev/null")
  doAssert code != 0, "invalid fmtOverride should exit non-zero"
  echo "PASS G1.4 inferFileFormat: invalid fmtOverride exits 1"

# ---------------------------------------------------------------------------
# G1.5 — testMutualExclusion: --header-pattern + --header-n exits 1
# ---------------------------------------------------------------------------

block testMutualExclusion:
  let (_, code) = execCmdEx(HelperBin & " --exit-test-mutual-exclusion 2>/dev/null")
  doAssert code != 0, "--header-pattern + --header-n should exit non-zero"
  echo "PASS G1.5 validateGatherConfig: --header-pattern + --header-n exits 1"

# ---------------------------------------------------------------------------
# G1.6 — testVcfHeaderFlagRejected: --header-pattern rejected for VCF format
# ---------------------------------------------------------------------------

block testVcfHeaderFlagRejected:
  let (_, code) = execCmdEx(HelperBin & " --exit-test-vcf-header-flag 2>/dev/null")
  doAssert code != 0, "--header-pattern with VCF should exit non-zero"
  echo "PASS G1.6 validateGatherConfig: --header-pattern rejected for VCF format"

# ---------------------------------------------------------------------------
# G1.7 — testBcfHeaderFlagRejected: --header-n rejected for BCF format
# ---------------------------------------------------------------------------

block testBcfHeaderFlagRejected:
  let (_, code) = execCmdEx(HelperBin & " --exit-test-bcf-header-flag 2>/dev/null")
  doAssert code != 0, "--header-n with BCF should exit non-zero"
  echo "PASS G1.7 validateGatherConfig: --header-n rejected for BCF format"

# ---------------------------------------------------------------------------
# G3.1 — testFindBcfHeaderEnd: offset past magic+l_text+header text returned correctly
# ---------------------------------------------------------------------------

block testFindBcfHeaderEnd:
  # Buffer shorter than 9 bytes → -1.
  doAssert findBcfHeaderEnd(@[0x42'u8, 0x43, 0x46]) == -1, "short: expected -1"

  # Full header: magic(5) + l_text(4) + text.
  let hdrText = "##BCF header\n"
  let lText = hdrText.len.uint32
  var buf: seq[byte]
  buf.add([byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8])
  buf.add([byte(lText and 0xff), byte((lText shr 8) and 0xff),
           byte((lText shr 16) and 0xff), byte((lText shr 24) and 0xff)])
  for c in hdrText: buf.add(byte(c))
  let expected = 5 + 4 + hdrText.len
  doAssert findBcfHeaderEnd(buf) == expected,
    &"full header: expected {expected}, got {findBcfHeaderEnd(buf)}"

  # Partial header (one byte short) → -1.
  doAssert findBcfHeaderEnd(buf[0 ..< buf.len - 1]) == -1,
    "partial header: expected -1"

  # Extra record bytes after header → still returns headerEnd.
  var buf2 = buf & @[0x01'u8, 0x02, 0x03]
  doAssert findBcfHeaderEnd(buf2) == expected,
    "header + records: expected same headerEnd"

  echo "PASS G2.1 findBcfHeaderEnd"

# ---------------------------------------------------------------------------
# G3.2 — testFindVcfHeaderEnd: data start offset returned; all-hash returns -1
# ---------------------------------------------------------------------------

block testFindVcfHeaderEnd:
  # All '#' lines, no data → -1.
  var allHash: seq[byte]
  for c in "##format=VCFv4.2\n#CHROM\tPOS\n": allHash.add(byte(c))
  doAssert findVcfHeaderEnd(allHash) == -1, "all-hash lines: expected -1"

  # '#' lines followed by a data line → returns start of data line.
  var withData: seq[byte]
  for c in "##format=VCFv4.2\n#CHROM\tPOS\n1\t100\n": withData.add(byte(c))
  let dataStart = "##format=VCFv4.2\n#CHROM\tPOS\n".len
  doAssert findVcfHeaderEnd(withData) == dataStart,
    &"with data: expected {dataStart}, got {findVcfHeaderEnd(withData)}"

  # First byte is data (no header) → returns 0.
  var noHeader: seq[byte]
  for c in "1\t100\n2\t200\n": noHeader.add(byte(c))
  doAssert findVcfHeaderEnd(noHeader) == 0, "no header: expected 0"

  # Ends mid '#' line (no newline yet) → -1.
  var midLine: seq[byte]
  for c in "##format": midLine.add(byte(c))
  doAssert findVcfHeaderEnd(midLine) == -1, "mid-hash-line: expected -1"

  # Ends mid data line (no newline yet) → returns start of that line.
  var midData: seq[byte]
  for c in "##hdr\n1\t10": midData.add(byte(c))
  let midDataStart = "##hdr\n".len
  doAssert findVcfHeaderEnd(midData) == midDataStart,
    &"mid-data-line: expected {midDataStart}, got {findVcfHeaderEnd(midData)}"

  echo "PASS G2.2 findVcfHeaderEnd"

# ---------------------------------------------------------------------------
# G3.3 — testStripBcfHeader: record bytes returned after stripping magic+l_text+header
# ---------------------------------------------------------------------------

block testStripBcfHeader:
  # Build minimal fake uncompressed BCF: magic(5) + l_text(4) + header + records.
  let hdrText = "##BCF fake header\n"
  let lText = hdrText.len.uint32
  var bcfData: seq[byte]
  bcfData.add([byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8])
  bcfData.add([byte(lText and 0xff), byte((lText shr 8) and 0xff),
               byte((lText shr 16) and 0xff), byte((lText shr 24) and 0xff)])
  for c in hdrText: bcfData.add(byte(c))
  let recordBytes = @[0x01'u8, 0x02, 0x03, 0x04, 0x05]
  bcfData.add(recordBytes)

  let got = stripBcfHeader(bcfData)
  doAssert got == recordBytes, &"BCF strip: expected records only, got {got}"

  # Too short to hold l_text.
  doAssert stripBcfHeader(@[0x42'u8, 0x43, 0x46]) == @[],
    "short buffer: expected @[]"

  # Exactly the header, no record bytes.
  let hdrOnly = bcfData[0 ..< bcfData.len - recordBytes.len]
  doAssert stripBcfHeader(hdrOnly) == @[], "header-only: expected @[]"

  echo "PASS G2.3 stripBcfHeader"

# ---------------------------------------------------------------------------
# G3.4 — testStripLinesByPattern: leading lines matching prefix removed; no-match unchanged
# ---------------------------------------------------------------------------

block testStripLinesByPattern:
  var data: seq[byte]
  for c in "##header1\n##header2\nrecord1\nrecord2\n":
    data.add(byte(c))

  var expected: seq[byte]
  for c in "record1\nrecord2\n": expected.add(byte(c))
  doAssert stripLinesByPattern(data, "##") == expected,
    "## strip: unexpected result"

  # No matching lines → unchanged.
  doAssert stripLinesByPattern(data, "XX") == data,
    "no-match pattern: should return unchanged"

  # Empty pattern → strips nothing.
  doAssert stripLinesByPattern(data, "") == data,
    "empty pattern: should return unchanged"

  # Single-char pattern.
  var d2: seq[byte]
  for c in "#h1\n#h2\ndata\n": d2.add(byte(c))
  var e2: seq[byte]
  for c in "data\n": e2.add(byte(c))
  doAssert stripLinesByPattern(d2, "#") == e2, "single-char strip: unexpected result"

  echo "PASS G2.4 stripLinesByPattern"

# ---------------------------------------------------------------------------
# G3.5 — testStripFirstNLines: first N lines removed; strip 0 unchanged; strip > total empty
# ---------------------------------------------------------------------------

block testStripFirstNLines:
  var data: seq[byte]
  for c in "line1\nline2\nline3\nline4\n": data.add(byte(c))

  var e2: seq[byte]
  for c in "line3\nline4\n": e2.add(byte(c))
  doAssert stripFirstNLines(data, 2) == e2, "strip 2: unexpected result"

  # Strip 0 → unchanged.
  doAssert stripFirstNLines(data, 0) == data, "strip 0: should return unchanged"

  # Strip more than available → empty.
  doAssert stripFirstNLines(data, 10) == @[], "strip all: should return @[]"

  # Partial last line (no trailing newline).
  var d2: seq[byte]
  for c in "hdr\ndata": d2.add(byte(c))
  var e3: seq[byte]
  for c in "data": e3.add(byte(c))
  doAssert stripFirstNLines(d2, 1) == e3, "partial last line: unexpected result"

  echo "PASS G2.5 stripFirstNLines"

# ---------------------------------------------------------------------------
# G3.6 — testRunInterceptorBcfStrip: BCF header stripped via pipe (shard 1)
# ---------------------------------------------------------------------------

block testRunInterceptorBcfStrip:
  let hdrText = "##BCF header\n"
  let lText = hdrText.len.uint32
  var bcfData: seq[byte]
  bcfData.add([byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8])
  bcfData.add([byte(lText and 0xff), byte((lText shr 8) and 0xff),
               byte((lText shr 16) and 0xff), byte((lText shr 24) and 0xff)])
  for c in hdrText: bcfData.add(byte(c))
  let records = @[0x01'u8, 0x02, 0x03, 0x04, 0x05]
  bcfData.add(records)

  # Set global state: uncompressed BCF detected by shard 0.
  gDetectedFormat = ffBcf
  gStreamIsBgzf   = false
  gChromLine.ready = true

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr bcfData[0]), bcfData.len)
  discard posix.close(fds[1])

  const TmpBcf = "/tmp/vcfparty_test_g3_bcf.bin"
  discard runInterceptor(GatherConfig(format: ffBcf, compression: compNone),
                 shardIdx = 1, fds[0], TmpBcf)

  let f = open(TmpBcf, fmRead)
  var outBuf = newSeq[byte](1024)
  let n = readBytes(f, outBuf, 0, 1024)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpBcf)

  doAssert outBuf == records,
    &"BCF interceptor strip: expected records only, got {outBuf}"
  echo "PASS G2.6 runInterceptor: BCF header stripping"

# ---------------------------------------------------------------------------
# G3.7 — testRunInterceptorVcfStrip: VCF header stripped via pipe (shard 1)
# ---------------------------------------------------------------------------

block testRunInterceptorVcfStrip:
  var vcfData: seq[byte]
  for c in "##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t100\n":
    vcfData.add(byte(c))

  gDetectedFormat = ffVcf
  gStreamIsBgzf   = false
  # Set gChromLine.buf to match the #CHROM line in vcfData above.
  let chromStr = "#CHROM\tPOS"
  gChromLine.len = chromStr.len.int32
  for k in 0 ..< chromStr.len:
    gChromLine.buf[k] = byte(chromStr[k])
  gChromLine.ready = true

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr vcfData[0]), vcfData.len)
  discard posix.close(fds[1])

  const TmpVcf = "/tmp/vcfparty_test_g3_vcf.txt"
  let cfg = GatherConfig(format: ffVcf, compression: compNone)
  discard runInterceptor(cfg, shardIdx = 1, fds[0], TmpVcf)

  let f = open(TmpVcf, fmRead)
  var outBuf = newSeq[byte](1024)
  let n = readBytes(f, outBuf, 0, 1024)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpVcf)

  var expected: seq[byte]
  for c in "1\t100\n": expected.add(byte(c))
  doAssert outBuf == expected,
    &"VCF interceptor strip: expected data lines only, got {outBuf}"
  echo "PASS G2.7 runInterceptor: VCF header stripping"

# ---------------------------------------------------------------------------
# G3.8 — testRunInterceptorHeaderN: --header-n strips first N lines via pipe (shard 1)
# ---------------------------------------------------------------------------

block testRunInterceptorHeaderN:
  var data: seq[byte]
  for c in "hdr1\nhdr2\ndata1\ndata2\n": data.add(byte(c))

  gDetectedFormat = ffText
  gStreamIsBgzf   = false
  gChromLine.ready = true

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr data[0]), data.len)
  discard posix.close(fds[1])

  const TmpN = "/tmp/vcfparty_test_g3_headern.txt"
  let cfg = GatherConfig(format: ffText, compression: compNone, headerN: some(2))
  discard runInterceptor(cfg, shardIdx = 1, fds[0], TmpN)

  let f = open(TmpN, fmRead)
  var outBuf = newSeq[byte](1024)
  let n = readBytes(f, outBuf, 0, 1024)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpN)

  var expected: seq[byte]
  for c in "data1\ndata2\n": expected.add(byte(c))
  doAssert outBuf == expected,
    &"header-n strip: expected data lines only, got {outBuf}"
  echo "PASS G2.8 runInterceptor: text header stripping (--header-n)"

# ---------------------------------------------------------------------------
# G3.9 — testRunInterceptorNoStrip: text with no flags passes through unchanged (shard 1)
# ---------------------------------------------------------------------------

block testRunInterceptorNoStrip:
  # Text format with no flags: stream passes through unchanged.
  var data: seq[byte]
  for c in "##header\nrecord1\nrecord2\n": data.add(byte(c))

  gDetectedFormat = ffText
  gStreamIsBgzf   = false
  gChromLine.ready = true

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr data[0]), data.len)
  discard posix.close(fds[1])

  const TmpPass = "/tmp/vcfparty_test_g3_nostrip.txt"
  let cfg = GatherConfig(format: ffText, compression: compNone)
  discard runInterceptor(cfg, shardIdx = 1, fds[0], TmpPass)

  let f = open(TmpPass, fmRead)
  var outBuf = newSeq[byte](1024)
  let n = readBytes(f, outBuf, 0, 1024)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpPass)

  doAssert outBuf == data, "no-strip: text stream should pass through unchanged"
  echo "PASS G2.9 runInterceptor: no-strip pass-through"

# ---------------------------------------------------------------------------
# G4.1 — testRecompressUncompressedToBgzf: uncompressed → BGZF via runInterceptor (shard 0)
# ---------------------------------------------------------------------------

block testRecompressUncompressedToBgzf:
  var raw: seq[byte]
  for c in "col1\tcol2\nhello\tworld\n": raw.add(byte(c))

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr raw[0]), raw.len)
  discard posix.close(fds[1])

  const TmpRecomp = "/tmp/vcfparty_test_g4_recomp.bgzf"
  discard runInterceptor(GatherConfig(format: ffText, compression: compBgzf),
                 shardIdx = 0, fds[0], TmpRecomp)

  let f = open(TmpRecomp, fmRead)
  var outBuf = newSeq[byte](65536)
  let n = readBytes(f, outBuf, 0, 65536)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpRecomp)

  doAssert isBgzfStream(outBuf), "recomp: output should be BGZF"
  doAssert decompressAllBgzfBlocks(outBuf) == raw,
    "recomp: decompressed content should match input"
  echo "PASS G3.1 runInterceptor: recompress uncompressed to BGZF"

# ---------------------------------------------------------------------------
# G4.2 — testDecompressBgzfToUncompressed: BGZF → uncompressed via runInterceptor (shard 0)
# ---------------------------------------------------------------------------

block testDecompressBgzfToUncompressed:
  var raw: seq[byte]
  for c in "col1\tcol2\nhello\tworld\n": raw.add(byte(c))
  let compressed = compressToBgzfMulti(raw)

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr compressed[0]), compressed.len)
  discard posix.close(fds[1])

  const TmpDecomp = "/tmp/vcfparty_test_g4_decomp.txt"
  discard runInterceptor(GatherConfig(format: ffText, compression: compNone),
                 shardIdx = 0, fds[0], TmpDecomp)

  let f = open(TmpDecomp, fmRead)
  var outBuf = newSeq[byte](65536)
  let n = readBytes(f, outBuf, 0, 65536)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpDecomp)

  doAssert not isBgzfStream(outBuf), "decomp: output should not be BGZF"
  doAssert outBuf == raw, "decomp: output should be original uncompressed content"
  echo "PASS G3.2 runInterceptor: decompress BGZF to uncompressed"

# ---------------------------------------------------------------------------
# G4.3 — testPassThroughBgzfToBgzf: BGZF → BGZF pass-through via runInterceptor (shard 0)
# ---------------------------------------------------------------------------

block testPassThroughBgzfToBgzf:
  var raw: seq[byte]
  for c in "col1\tcol2\nhello\tworld\n": raw.add(byte(c))
  let compressed = compressToBgzfMulti(raw)

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr compressed[0]), compressed.len)
  discard posix.close(fds[1])

  const TmpPassBgzf = "/tmp/vcfparty_test_g4_passbgzf.bgzf"
  discard runInterceptor(GatherConfig(format: ffText, compression: compBgzf),
                 shardIdx = 0, fds[0], TmpPassBgzf)

  let f = open(TmpPassBgzf, fmRead)
  var outBuf = newSeq[byte](65536)
  let n = readBytes(f, outBuf, 0, 65536)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpPassBgzf)

  doAssert outBuf == compressed,
    "BGZF pass-through: output bytes should be identical to input"
  echo "PASS G3.3 runInterceptor: pass-through BGZF to BGZF"

# ---------------------------------------------------------------------------
# G4.4 — testShardStripAndRecompress: uncompressed VCF shard 1 stripped + recompressed to BGZF
# ---------------------------------------------------------------------------

block testShardStripAndRecompress:
  var vcfData: seq[byte]
  for c in "##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t100\n": vcfData.add(byte(c))

  gDetectedFormat = ffVcf
  gStreamIsBgzf   = false
  let chromStr4 = "#CHROM\tPOS"
  gChromLine.len = chromStr4.len.int32
  for k in 0 ..< chromStr4.len:
    gChromLine.buf[k] = byte(chromStr4[k])
  gChromLine.ready = true

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr vcfData[0]), vcfData.len)
  discard posix.close(fds[1])

  const TmpShard = "/tmp/vcfparty_test_g4_shard.bgzf"
  discard runInterceptor(GatherConfig(format: ffVcf, compression: compBgzf),
                 shardIdx = 1, fds[0], TmpShard)

  let f = open(TmpShard, fmRead)
  var outBuf = newSeq[byte](65536)
  let n = readBytes(f, outBuf, 0, 65536)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpShard)

  doAssert isBgzfStream(outBuf), "strip+recomp: output should be BGZF"
  var e1: seq[byte]
  for c in "1\t100\n": e1.add(byte(c))
  doAssert decompressAllBgzfBlocks(outBuf) == e1,
    "strip+recomp: decompressed output should contain only data lines"
  echo "PASS G3.4 runInterceptor: shard strip + BGZF recompression"

# ---------------------------------------------------------------------------
# G4.5 — testShardBgzfInputStripAndRecompress: BGZF VCF shard 1 stripped + recompressed to BGZF
# ---------------------------------------------------------------------------

block testShardBgzfInputStripAndRecompress:
  var raw: seq[byte]
  for c in "##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t200\n": raw.add(byte(c))
  let compressed = compressToBgzfMulti(raw)

  gDetectedFormat = ffVcf
  gStreamIsBgzf   = true
  let chromStr5 = "#CHROM\tPOS"
  gChromLine.len = chromStr5.len.int32
  for k in 0 ..< chromStr5.len:
    gChromLine.buf[k] = byte(chromStr5[k])
  gChromLine.ready = true

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr compressed[0]), compressed.len)
  discard posix.close(fds[1])

  const TmpBgzfShard = "/tmp/vcfparty_test_g4_bgzf_shard.bgzf"
  discard runInterceptor(GatherConfig(format: ffVcf, compression: compBgzf),
                 shardIdx = 1, fds[0], TmpBgzfShard)

  let f = open(TmpBgzfShard, fmRead)
  var outBuf = newSeq[byte](65536)
  let n = readBytes(f, outBuf, 0, 65536)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpBgzfShard)

  doAssert isBgzfStream(outBuf), "bgzf-strip+recomp: output should be BGZF"
  var e2: seq[byte]
  for c in "1\t200\n": e2.add(byte(c))
  doAssert decompressAllBgzfBlocks(outBuf) == e2,
    "bgzf-strip+recomp: decompressed output should contain only data lines"
  echo "PASS G3.5 runInterceptor: BGZF shard strip + recompression"

# ---------------------------------------------------------------------------
# G5.1 — testCleanupSuccess: success path — temp files and dir deleted
# ---------------------------------------------------------------------------

block testCleanupSuccess:
  const TmpDir = "/tmp/vcfparty_test_g5_cleanup_ok"
  createDir(TmpDir)
  let f1 = TmpDir / "shard1.tmp"
  let f2 = TmpDir / "shard2.tmp"
  writeFile(f1, "content1")
  writeFile(f2, "content2")

  cleanupTempDir(TmpDir, @[f1, f2], success = true)

  doAssert not fileExists(f1), "cleanup success: shard1 should be deleted"
  doAssert not fileExists(f2), "cleanup success: shard2 should be deleted"
  doAssert not dirExists(TmpDir), "cleanup success: tmpDir should be deleted"
  echo "PASS G4.1 cleanupTempDir: success, files and dir deleted"

# ---------------------------------------------------------------------------
# G5.2 — testCleanupFailure: failure path — temp files preserved on disk
# ---------------------------------------------------------------------------

block testCleanupFailure:
  const TmpDir = "/tmp/vcfparty_test_g5_cleanup_fail"
  createDir(TmpDir)
  let f1 = TmpDir / "shard1.tmp"
  writeFile(f1, "content")

  cleanupTempDir(TmpDir, @[f1], success = false)

  doAssert fileExists(f1),    "cleanup failure: file should still exist"
  doAssert dirExists(TmpDir), "cleanup failure: dir should still exist"
  removeFile(f1)
  removeDir(TmpDir)
  echo "PASS G4.2 cleanupTempDir: failure, files preserved"

# ---------------------------------------------------------------------------
# G5.3 — testConcatShardsUncompressed: two uncompressed shards concatenated, tmpDir cleaned
# ---------------------------------------------------------------------------

block testConcatShardsUncompressed:
  const TmpDir  = "/tmp/vcfparty_test_g5_concat_unc"
  const OutPath = "/tmp/vcfparty_test_g5_out.txt"
  createDir(TmpDir)
  let shard1 = TmpDir / "shard1.tmp"
  let shard2 = TmpDir / "shard2.tmp"
  writeFile(shard1, "line1\nline2\n")
  writeFile(shard2, "line3\nline4\n")

  let cfg = GatherConfig(format: ffText, compression: compNone,
                         outputPath: OutPath, tmpDir: TmpDir)
  concatenateShards(cfg, @[shard1, shard2])

  doAssert readFile(OutPath) == "line1\nline2\nline3\nline4\n",
    "concat uncompressed: unexpected content"
  doAssert not fileExists(shard1), "concat: shard1 should be deleted"
  doAssert not fileExists(shard2), "concat: shard2 should be deleted"
  doAssert not dirExists(TmpDir),  "concat: tmpDir should be deleted"
  removeFile(OutPath)
  echo "PASS G4.3 concatenateShards: uncompressed"

# ---------------------------------------------------------------------------
# G5.4 — testConcatShardsBgzf: BGZF shards concatenated, single EOF block appended, tmpDir cleaned
# ---------------------------------------------------------------------------

block testConcatShardsBgzf:
  const TmpDir  = "/tmp/vcfparty_test_g5_concat_bgzf"
  const OutPath = "/tmp/vcfparty_test_g5_out.bgzf"
  createDir(TmpDir)
  let shard1 = TmpDir / "shard1.tmp"
  let shard2 = TmpDir / "shard2.tmp"

  # Temp shard files contain raw BGZF blocks with NO trailing EOF block.
  var data1: seq[byte]
  for c in "record1\nrecord2\n": data1.add(byte(c))
  var data2: seq[byte]
  for c in "record3\nrecord4\n": data2.add(byte(c))
  let blk1 = compressToBgzfMulti(data1)
  let blk2 = compressToBgzfMulti(data2)
  block:
    let f = open(shard1, fmWrite)
    discard f.writeBytes(blk1, 0, blk1.len)
    f.close()
  block:
    let f = open(shard2, fmWrite)
    discard f.writeBytes(blk2, 0, blk2.len)
    f.close()

  let cfg = GatherConfig(format: ffVcf, compression: compBgzf,
                         outputPath: OutPath, tmpDir: TmpDir)
  concatenateShards(cfg, @[shard1, shard2])

  doAssert not fileExists(shard1), "concat bgzf: shard1 should be deleted"
  doAssert not fileExists(shard2), "concat bgzf: shard2 should be deleted"
  doAssert not dirExists(TmpDir),  "concat bgzf: tmpDir should be deleted"

  let f = open(OutPath, fmRead)
  var outBuf = newSeq[byte](65536)
  let n = readBytes(f, outBuf, 0, 65536)
  f.close()
  outBuf.setLen(n)
  removeFile(OutPath)

  # Output must end with the 28-byte BGZF EOF block.
  let eofLen = BGZF_EOF.len
  doAssert outBuf.len >= eofLen, "concat bgzf: output too short"
  var eofMatch = true
  for i in 0 ..< eofLen:
    if outBuf[outBuf.len - eofLen + i] != BGZF_EOF[i]:
      eofMatch = false
      break
  doAssert eofMatch, "concat bgzf: output should end with BGZF EOF block"

  # Decompressed content must equal data1 ++ data2.
  doAssert decompressAllBgzfBlocks(outBuf) == data1 & data2,
    "concat bgzf: decompressed content mismatch"
  echo "PASS G4.4 concatenateShards: BGZF with EOF block"

# ===========================================================================
# G5 — Integration tests via compiled binary
# ===========================================================================

const BinPath = "./vcfparty"

block buildBinary:
  if not fileExists(BinPath):
    let (outp, code) = execCmdEx("nimble build 2>&1")
    if code != 0:
      echo "nimble build failed:\n", outp
      quit(1)
  doAssert fileExists(BinPath), "binary not found: " & BinPath
  echo "PASS G5.0 binary available"

proc countRecords(path: string): int =
  let (o, _) = execCmdEx("bcftools view -HG " & path & " 2>/dev/null | wc -l")
  o.strip.parseInt

proc recordsHash(path: string): string =
  let (h, _) = execCmdEx("bcftools view -H " & path & " 2>/dev/null | sha256sum")
  h.split(" ")[0]

proc runGather(args: string): (string, int) =
  execCmdEx(BinPath & " run " & args & " 2>&1")

# ---------------------------------------------------------------------------
# G7.1 — VCF gather, compressed pipeline (-Oz) → .vcf.gz
# ---------------------------------------------------------------------------

block testGatherVcfCompressed:
  let tmpDir = getTempDir() / "vcfparty_g7_vcf_oz"
  createDir(tmpDir)
  let outPath = tmpDir / "out.vcf.gz"
  let (outp, code) = runGather(
    &"-n 4 -o {outPath} {SmallVcf} ::: bcftools view -Oz +concat+")
  doAssert code == 0, &"VCF gather -Oz exited {code}:\n{outp}"
  doAssert fileExists(outPath), "VCF gather -Oz: output missing"
  let got = countRecords(outPath)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"VCF gather -Oz: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallVcf),
    "VCF gather -Oz: content hash mismatch"
  removeDir(tmpDir)
  echo &"PASS G5.1 VCF gather -Oz: {got} records, content hash matches"

# ---------------------------------------------------------------------------
# G7.2 — VCF gather, uncompressed pipeline (-Ov) → .vcf.gz (recompression)
# ---------------------------------------------------------------------------

block testGatherVcfRecompress:
  let tmpDir = getTempDir() / "vcfparty_g7_vcf_ov"
  createDir(tmpDir)
  let outPath = tmpDir / "out.vcf.gz"
  let (outp, code) = runGather(
    &"-n 4 -o {outPath} {SmallVcf} ::: bcftools view -Ov +concat+")
  doAssert code == 0, &"VCF gather -Ov exited {code}:\n{outp}"
  doAssert fileExists(outPath), "VCF gather -Ov: output missing"
  let got = countRecords(outPath)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"VCF gather -Ov: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallVcf),
    "VCF gather -Ov: content hash mismatch (recompression corrupted data)"
  removeDir(tmpDir)
  echo &"PASS G5.2 VCF gather -Ov (recompress uncompressed→BGZF): {got} records, hash matches"

# ---------------------------------------------------------------------------
# G7.3 — BCF gather, compressed pipeline (-Ob) → .bcf
# ---------------------------------------------------------------------------

block testGatherBcfCompressed:
  let tmpDir = getTempDir() / "vcfparty_g7_bcf_ob"
  createDir(tmpDir)
  let outPath = tmpDir / "out.bcf"
  let (outp, code) = runGather(
    &"-n 4 -o {outPath} {SmallBcf} ::: bcftools view -Ob +concat+")
  doAssert code == 0, &"BCF gather -Ob exited {code}:\n{outp}"
  doAssert fileExists(outPath), "BCF gather -Ob: output missing"
  let got = countRecords(outPath)
  let orig = countRecords(SmallBcf)
  doAssert got == orig, &"BCF gather -Ob: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallBcf),
    "BCF gather -Ob: content hash mismatch"
  removeDir(tmpDir)
  echo &"PASS G5.3 BCF gather -Ob: {got} records, content hash matches"

# ---------------------------------------------------------------------------
# G7.4 — BCF gather, uncompressed pipeline (-Ou) → .bcf (recompression)
# ---------------------------------------------------------------------------

block testGatherBcfRecompress:
  let tmpDir = getTempDir() / "vcfparty_g7_bcf_ou"
  createDir(tmpDir)
  let outPath = tmpDir / "out.bcf"
  let (outp, code) = runGather(
    &"-n 4 -o {outPath} {SmallBcf} ::: bcftools view -Ou +concat+")
  doAssert code == 0, &"BCF gather -Ou exited {code}:\n{outp}"
  doAssert fileExists(outPath), "BCF gather -Ou: output missing"
  let got = countRecords(outPath)
  let orig = countRecords(SmallBcf)
  doAssert got == orig, &"BCF gather -Ou: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallBcf),
    "BCF gather -Ou: content hash mismatch (recompression corrupted data)"
  removeDir(tmpDir)
  echo &"PASS G5.4 BCF gather -Ou (recompress uncompressed→BGZF): {got} records, hash matches"

# ---------------------------------------------------------------------------
# G7.5 — Text gather (bcftools query) → .txt, no stripping
# ---------------------------------------------------------------------------

block testGatherText:
  let tmpDir = getTempDir() / "vcfparty_g7_text"
  createDir(tmpDir)
  let outPath = tmpDir / "out.txt"
  let (outp, code) = runGather(
    &"-n 4 -o {outPath} {SmallVcf} ::: bcftools query -f '%CHROM\\t%POS\\n' +concat+")
  doAssert code == 0, &"text gather exited {code}:\n{outp}"
  doAssert fileExists(outPath), "text gather: output missing"
  let (lineOut, _) = execCmdEx("wc -l < " & outPath)
  let lineCount = lineOut.strip.parseInt
  let orig = countRecords(SmallVcf)
  doAssert lineCount == orig,
    &"text gather: expected {orig} lines, got {lineCount}"
  removeDir(tmpDir)
  echo &"PASS G5.5 text gather → .txt: {lineCount} lines, matches record count"

# ---------------------------------------------------------------------------
# G7.6 — Text gather with --header-n 1: first line of shards 2..N stripped
# ---------------------------------------------------------------------------

block testGatherTextHeaderN:
  # Pipeline prepends a fixed header line to each shard's output.
  # With --header-n 1, shards 2..4 should have their header line stripped.
  # Result: 1 header line + 5000 data lines = 5001 lines total.
  let tmpDir = getTempDir() / "vcfparty_g7_text_headern"
  createDir(tmpDir)
  let outPath = tmpDir / "out.txt"
  let pipeline = "{ echo 'CHROM\tPOS'; bcftools query -f '%CHROM\\t%POS\\n'; }"
  let (outp, code) = runGather(
    &"-n 4 -o {outPath} --header-n 1 {SmallVcf} ::: sh -c {quoteShell(pipeline)} +concat+")
  doAssert code == 0, &"text gather --header-n 1 exited {code}:\n{outp}"
  doAssert fileExists(outPath), "text gather --header-n: output missing"
  let (lineOut, _) = execCmdEx("wc -l < " & outPath)
  let lineCount = lineOut.strip.parseInt
  let orig = countRecords(SmallVcf)
  # 1 header from shard 0 + orig data lines from all shards
  doAssert lineCount == orig + 1,
    &"text gather --header-n 1: expected {orig + 1} lines, got {lineCount}"
  let (firstLine, _) = execCmdEx("head -1 " & outPath)
  doAssert firstLine.strip == "CHROM\tPOS",
    &"text gather --header-n 1: first line should be header, got: {firstLine.strip}"
  removeDir(tmpDir)
  echo &"PASS G5.6 text gather --header-n 1: {lineCount} lines (1 header + {orig} data)"

# ---------------------------------------------------------------------------
# G7.7 — Unknown-extension gather: .out.gz → inferred as text + BGZF
# ---------------------------------------------------------------------------

block testGatherUnknownExt:
  # .out.gz is not a recognised VCF/BCF prefix → inferred as text, BGZF (from .gz).
  # No --gather-fmt needed under the new lenient inference rules.
  let tmpDir = getTempDir() / "vcfparty_g7_unknown_ext"
  createDir(tmpDir)
  let outPath = tmpDir / "out.out.gz"
  let (outp, code) = runGather(
    &"-n 4 -o {outPath} " &
    &"{SmallVcf} ::: bcftools query -f '%CHROM\\t%POS\\n' +concat+")
  doAssert code == 0, &"unknown-ext gather exited {code}:\n{outp}"
  doAssert fileExists(outPath), "unknown-ext gather: output missing"
  # Output should be valid BGZF (because of .gz).
  let f = open(outPath, fmRead)
  var magic = newSeq[byte](2)
  discard readBytes(f, magic, 0, 2)
  f.close()
  doAssert magic[0] == 0x1f'u8 and magic[1] == 0x8b'u8,
    "unknown-ext gather: output should be BGZF (.gz extension)"
  let (lineOut, _) = execCmdEx(
    "bgzip -d -c " & outPath & " 2>/dev/null | wc -l")
  let lineCount = lineOut.strip.parseInt
  let orig = countRecords(SmallVcf)
  doAssert lineCount == orig,
    &"unknown-ext gather: expected {orig} lines, got {lineCount}"
  removeDir(tmpDir)
  echo &"PASS G5.7 unknown-ext (.out.gz) text gather: {lineCount} lines, valid BGZF"

# ---------------------------------------------------------------------------
# G7.8 — Shard failure: temp files left on disk, exit 1, paths in stderr
# ---------------------------------------------------------------------------

block testGatherShardFailure:
  let tmpDir  = getTempDir() / "vcfparty_g7_fail"
  let tDir    = getTempDir() / "vcfparty_g7_fail_tmp"
  createDir(tmpDir)
  let outPath = tmpDir / "out.vcf.gz"
  let (outp, code) = runGather(
    &"-n 2 -o {outPath} --tmp-dir {tDir} {SmallVcf} ::: false +concat+")
  doAssert code != 0, "shard failure: vcfparty should exit non-zero"
  # At least one temp path should be printed to stderr.
  doAssert tDir in outp,
    &"shard failure: expected tmp dir path in stderr, got:\n{outp}"
  # Temp dir should still exist (files left for debugging).
  doAssert dirExists(tDir), "shard failure: tmp dir should be preserved"
  # Cleanup.
  removeDir(tmpDir)
  removeDir(tDir)
  echo "PASS G5.8 shard failure: exit non-zero, temp paths printed, files preserved"

# ---------------------------------------------------------------------------
# G7.9 — --tmp-dir custom path
# ---------------------------------------------------------------------------

block testGatherCustomTmpDir:
  let tmpDir    = getTempDir() / "vcfparty_g7_custom_tmp"
  let customTmp = getTempDir() / "vcfparty_g7_my_tmp_dir"
  createDir(tmpDir)
  let outPath = tmpDir / "out.vcf.gz"
  let (outp, code) = runGather(
    &"-n 4 -o {outPath} --tmp-dir {customTmp} " &
    &"{SmallVcf} ::: bcftools view -Oz +concat+")
  doAssert code == 0, &"--tmp-dir exited {code}:\n{outp}"
  doAssert fileExists(outPath), "--tmp-dir: output missing"
  # On success the custom tmp dir should be cleaned up.
  doAssert not dirExists(customTmp), "--tmp-dir: custom dir should be removed on success"
  let got = countRecords(outPath)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"--tmp-dir: record count {got} != {orig}"
  removeDir(tmpDir)
  echo &"PASS G5.9 --tmp-dir custom path: {got} records, tmp dir cleaned up"

# ---------------------------------------------------------------------------
# G7.10 — testGatherTextHeaderPattern: --header-pattern strips matching lines from shards 2..N
# ---------------------------------------------------------------------------

block testGatherTextHeaderPattern:
  # Pipeline prepends a "##"-prefixed header line to each shard's output.
  # With --header-pattern "##", shards 2..4 should have that line stripped.
  # Result: 1 header line + 5000 data lines = 5001 lines total.
  let tmpDir = getTempDir() / "vcfparty_g7_text_headerpattern"
  createDir(tmpDir)
  let outPath = tmpDir / "out.txt"
  let pipeline = "{ echo '##CHROM\tPOS'; bcftools query -f '%CHROM\\t%POS\\n'; }"
  let (outp, code) = runGather(
    &"-n 4 -o {outPath} --header-pattern \"##\" {SmallVcf} ::: sh -c {quoteShell(pipeline)} +concat+")
  doAssert code == 0, &"text gather --header-pattern exited {code}:\n{outp}"
  doAssert fileExists(outPath), "text gather --header-pattern: output missing"
  let (lineOut, _) = execCmdEx("wc -l < " & outPath)
  let lineCount = lineOut.strip.parseInt
  let orig = countRecords(SmallVcf)
  # 1 header from shard 0 + orig data lines from all shards
  doAssert lineCount == orig + 1,
    &"text gather --header-pattern: expected {orig + 1} lines, got {lineCount}"
  let (firstLine, _) = execCmdEx("head -1 " & outPath)
  doAssert firstLine.strip == "##CHROM\tPOS",
    &"text gather --header-pattern: first line should be header, got: {firstLine.strip}"
  removeDir(tmpDir)
  echo &"PASS G5.10 text gather --header-pattern: {lineCount} lines (1 header + {orig} data)"

# ===========================================================================
# G6 — gather subcommand integration tests
# ===========================================================================

proc runGatherSubcmd(args: string): (string, int) =
  execCmdEx(BinPath & " gather " & args & " 2>&1")

# ---------------------------------------------------------------------------
# G8.1 — VCF: scatter → gather, record count and hash match
# ---------------------------------------------------------------------------

block testGatherSubcmdVcf:
  let tmpDir = getTempDir() / "vcfparty_g8_vcf"
  createDir(tmpDir)
  let shardTemplate = tmpDir / "shard.{}.vcf.gz"
  # Scatter into 4 shards via CLI.
  let (sOutp, sCode) = execCmdEx(
    BinPath & &" scatter -n 4 -o {tmpDir}/shard.vcf.gz {SmallVcf} 2>&1")
  doAssert sCode == 0, &"G8.1 scatter exited {sCode}:\n{sOutp}"
  # Collect shard paths.
  var shards: seq[string]
  for i in 1..4:
    shards.add(tmpDir / ("shard_" & $i & ".shard.vcf.gz"))
  let shardsArg = shards.join(" ")
  let outPath = tmpDir / "gathered.vcf.gz"
  let (gOutp, gCode) = runGatherSubcmd(&"-o {outPath} {shardsArg}")
  doAssert gCode == 0, &"G8.1 gather exited {gCode}:\n{gOutp}"
  doAssert fileExists(outPath), "G8.1: output missing"
  let got = countRecords(outPath)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"G8.1: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallVcf),
    "G8.1: content hash mismatch"
  removeDir(tmpDir)
  echo &"PASS G6.1 gather subcommand VCF: {got} records, content hash matches"

# ---------------------------------------------------------------------------
# G8.2 — BCF: scatter → gather, record count and hash match
# ---------------------------------------------------------------------------

block testGatherSubcmdBcf:
  let tmpDir = getTempDir() / "vcfparty_g8_bcf"
  createDir(tmpDir)
  let (sOutp, sCode) = execCmdEx(
    BinPath & &" scatter -n 4 -o {tmpDir}/shard.bcf {SmallBcf} 2>&1")
  doAssert sCode == 0, &"G8.2 scatter exited {sCode}:\n{sOutp}"
  var shards: seq[string]
  for i in 1..4:
    shards.add(tmpDir / ("shard_" & $i & ".shard.bcf"))
  let shardsArg = shards.join(" ")
  let outPath = tmpDir / "gathered.bcf"
  let (gOutp, gCode) = runGatherSubcmd(&"-o {outPath} {shardsArg}")
  doAssert gCode == 0, &"G8.2 gather exited {gCode}:\n{gOutp}"
  doAssert fileExists(outPath), "G8.2: output missing"
  let got = countRecords(outPath)
  let orig = countRecords(SmallBcf)
  doAssert got == orig, &"G8.2: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallBcf),
    "G8.2: content hash mismatch"
  removeDir(tmpDir)
  echo &"PASS G6.2 gather subcommand BCF: {got} records, content hash matches"

# ---------------------------------------------------------------------------
# G8.3 — Omit -o: output goes to stdout (uncompressed VCF, record count matches)
# ---------------------------------------------------------------------------

block testGatherSubcmdStdout:
  let tmpDir = getTempDir() / "vcfparty_g8_stdout"
  createDir(tmpDir)
  let (sOutp, sCode) = execCmdEx(
    BinPath & &" scatter -n 4 -o {tmpDir}/shard.vcf.gz {SmallVcf} 2>&1")
  doAssert sCode == 0, &"G8.3 scatter exited {sCode}:\n{sOutp}"
  var shards: seq[string]
  for i in 1..4:
    shards.add(tmpDir / ("shard_" & $i & ".shard.vcf.gz"))
  let shardsArg = shards.join(" ")
  # Capture stdout to a file via shell redirection; no -o flag.
  let stdoutFile = tmpDir / "stdout.vcf"
  let (gOutp, gCode) = execCmdEx(
    BinPath & &" gather {shardsArg} > {stdoutFile} 2>&1")
  doAssert gCode == 0, &"G8.3 gather stdout exited {gCode}:\n{gOutp}"
  doAssert fileExists(stdoutFile), "G8.3: stdout capture file missing"
  let got  = countRecords(stdoutFile)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"G8.3: record count {got} != {orig}"
  removeDir(tmpDir)
  echo &"PASS G6.3 gather subcommand stdout: {got} records written to stdout"

# ---------------------------------------------------------------------------
# G8.4 — No input files exits non-zero
# ---------------------------------------------------------------------------

block testGatherSubcmdNoInputs:
  let tmpDir = getTempDir() / "vcfparty_g8_noinput"
  createDir(tmpDir)
  let outPath = tmpDir / "out.vcf.gz"
  let (outp, code) = runGatherSubcmd(&"-o {outPath}")
  doAssert code != 0, "G8.4: no inputs should exit non-zero"
  removeDir(tmpDir)
  echo "PASS G6.4 gather subcommand: no input files exits non-zero"

# ---------------------------------------------------------------------------
# G8.5 — Missing input file exits non-zero
# ---------------------------------------------------------------------------

block testGatherSubcmdMissingFile:
  let tmpDir = getTempDir() / "vcfparty_g8_missing"
  createDir(tmpDir)
  let outPath = tmpDir / "out.vcf.gz"
  let (outp, code) = runGatherSubcmd(&"-o {outPath} /nonexistent/shard.vcf.gz")
  doAssert code != 0, "G8.5: missing input file should exit non-zero"
  doAssert "not found" in outp.toLowerAscii,
    &"G8.5: error should mention 'not found', got: {outp}"
  removeDir(tmpDir)
  echo "PASS G6.5 gather subcommand: missing input file exits non-zero"

# ---------------------------------------------------------------------------
# G8.6 — run +concat+ without -o: output goes to stdout
# ---------------------------------------------------------------------------

block testRunGatherStdout:
  let tmpDir = getTempDir() / "vcfparty_g8_rungather_stdout"
  createDir(tmpDir)
  let stdoutFile = tmpDir / "stdout.vcf"
  # No -o; stdout is captured via shell redirection.
  let (outp, code) = execCmdEx(
    BinPath & &" run -n 4 {SmallVcf} ::: cat +concat+ > {stdoutFile} 2>&1")
  doAssert code == 0, &"G8.6 run +concat+ stdout exited {code}:\n{outp}"
  let got  = countRecords(stdoutFile)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"G8.6: record count {got} != {orig}"
  removeDir(tmpDir)
  echo &"PASS G6.6 run +concat+ stdout: {got} records written to stdout"

# ---------------------------------------------------------------------------
# G8.7 — gather --merge VCF: 4 shards → sorted output, record count and hash match
# ---------------------------------------------------------------------------

block testGatherMergeVcf:
  let tmpDir = getTempDir() / "vcfparty_g8_merge_vcf"
  createDir(tmpDir)
  let (sOutp, sCode) = execCmdEx(
    BinPath & &" scatter -n 4 -o {tmpDir}/shard.vcf.gz {SmallVcf} 2>&1")
  doAssert sCode == 0, &"G8.7 scatter exited {sCode}:\n{sOutp}"
  var shards: seq[string]
  for i in 1..4:
    shards.add(tmpDir / ("shard_" & $i & ".shard.vcf.gz"))
  let shardsArg = shards.join(" ")
  let outPath = tmpDir / "merged.vcf"
  let (gOutp, gCode) = runGatherSubcmd(&"--merge -o {outPath} {shardsArg}")
  doAssert gCode == 0, &"G8.7 gather --merge exited {gCode}:\n{gOutp}"
  doAssert fileExists(outPath), "G8.7: output missing"
  let got  = countRecords(outPath)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"G8.7: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallVcf),
    "G8.7: content hash mismatch vs original"
  removeDir(tmpDir)
  echo &"PASS G6.7 gather --merge VCF: {got} records, content hash matches"

# ---------------------------------------------------------------------------
# G8.8 — gather --merge BCF: 4 shards → sorted output, record count and hash match
# ---------------------------------------------------------------------------

block testGatherMergeBcf:
  let tmpDir = getTempDir() / "vcfparty_g8_merge_bcf"
  createDir(tmpDir)
  let (sOutp, sCode) = execCmdEx(
    BinPath & &" scatter -n 4 -o {tmpDir}/shard.bcf {SmallBcf} 2>&1")
  doAssert sCode == 0, &"G8.8 scatter exited {sCode}:\n{sOutp}"
  var shards: seq[string]
  for i in 1..4:
    shards.add(tmpDir / ("shard_" & $i & ".shard.bcf"))
  let shardsArg = shards.join(" ")
  let outPath = tmpDir / "merged.bcf"
  let (gOutp, gCode) = runGatherSubcmd(&"--merge -o {outPath} {shardsArg}")
  doAssert gCode == 0, &"G8.8 gather --merge BCF exited {gCode}:\n{gOutp}"
  doAssert fileExists(outPath), "G8.8: output missing"
  let got  = countRecords(outPath)
  let orig = countRecords(SmallBcf)
  doAssert got == orig, &"G8.8: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallBcf),
    "G8.8: content hash mismatch vs original"
  removeDir(tmpDir)
  echo &"PASS G6.8 gather --merge BCF: {got} records, content hash matches"

# ===========================================================================
# G7 — #CHROM header validation tests
# ===========================================================================

# ---------------------------------------------------------------------------
# S2.1 — extractChromLine: finds #CHROM in VCF header bytes
# ---------------------------------------------------------------------------

block testExtractChromLineVcf:
  var header: seq[byte]
  for c in "##fileformat=VCFv4.2\n##INFO=<ID=DP>\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n":
    header.add(byte(c))
  let line = extractChromLine(header)
  doAssert line.startsWith("#CHROM"), &"S2.1: expected '#CHROM...' got '{line}'"
  echo "PASS G7.1 extractChromLine: found #CHROM in VCF header bytes"

# ---------------------------------------------------------------------------
# S2.2 — extractChromLine: returns "" when not found
# ---------------------------------------------------------------------------

block testExtractChromLineNotFound:
  var data: seq[byte]
  for c in "##fileformat=VCFv4.2\n##INFO=<ID=DP>\n":
    data.add(byte(c))
  let line = extractChromLine(data)
  doAssert line == "", &"S2.2: expected '' got '{line}'"
  echo "PASS G7.2 extractChromLine: returns empty string when not found"

# ---------------------------------------------------------------------------
# S2.3 — chromLineFromBytes: works on BGZF VCF file bytes
# ---------------------------------------------------------------------------

block testChromLineFromBytesVcf:
  doAssert fileExists(SmallVcf), "S2.3: VCF fixture missing"
  let fileSize = getFileSize(SmallVcf).int
  var allBytes = newSeq[byte](fileSize)
  let f = open(SmallVcf, fmRead)
  discard readBytes(f, allBytes, 0, fileSize)
  f.close()
  let (fmt, isBgzf) = sniffStreamFormat(allBytes)
  doAssert fmt == ffVcf, &"S2.3: expected ffVcf, got {fmt}"
  let line = chromLineFromBytes(allBytes, fmt, isBgzf)
  doAssert line.startsWith("#CHROM"), &"S2.3: expected '#CHROM...' got '{line}'"
  echo "PASS G7.3 chromLineFromBytes: found #CHROM in BGZF VCF"

# ---------------------------------------------------------------------------
# S2.4 — chromLineFromBytes: works on BGZF BCF file bytes
# ---------------------------------------------------------------------------

block testChromLineFromBytesBcf:
  doAssert fileExists(SmallBcf), "S2.4: BCF fixture missing"
  let fileSize = getFileSize(SmallBcf).int
  var allBytes = newSeq[byte](fileSize)
  let f = open(SmallBcf, fmRead)
  discard readBytes(f, allBytes, 0, fileSize)
  f.close()
  let (fmt, isBgzf) = sniffStreamFormat(allBytes)
  doAssert fmt == ffBcf, &"S2.4: expected ffBcf, got {fmt}"
  let line = chromLineFromBytes(allBytes, fmt, isBgzf)
  doAssert line.startsWith("#CHROM"), &"S2.4: expected '#CHROM...' got '{line}'"
  echo "PASS G7.4 chromLineFromBytes: found #CHROM in BGZF BCF"

# ---------------------------------------------------------------------------
# S2.5 — chromLineFromFile: matches chromLineFromBytes
# ---------------------------------------------------------------------------

block testChromLineFromFile:
  doAssert fileExists(SmallVcf), "S2.5: VCF fixture missing"
  let fileSize = getFileSize(SmallVcf).int
  var allBytes = newSeq[byte](fileSize)
  let f = open(SmallVcf, fmRead)
  discard readBytes(f, allBytes, 0, fileSize)
  f.close()
  let (fmt, isBgzf) = sniffStreamFormat(allBytes)
  let fromBytes = chromLineFromBytes(allBytes, fmt, isBgzf)
  let fromFile  = chromLineFromFile(SmallVcf, fmt, isBgzf)
  doAssert fromBytes == fromFile,
    &"S2.5: chromLineFromFile != chromLineFromBytes (got '{fromFile}' vs '{fromBytes}')"
  doAssert fromBytes.startsWith("#CHROM"), &"S2.5: expected '#CHROM...' got '{fromBytes}'"
  echo "PASS G7.5 chromLineFromFile: matches chromLineFromBytes result"

# ---------------------------------------------------------------------------
# S2.6 — gather subcommand: matching #CHROM lines → success
# ---------------------------------------------------------------------------

block testGatherChromMatch:
  let tmpDir = getTempDir() / "vcfparty_s2_match"
  createDir(tmpDir)
  let (sOutp, sCode) = execCmdEx(
    BinPath & &" scatter -n 2 -o {tmpDir}/shard.vcf.gz {SmallVcf} 2>&1")
  doAssert sCode == 0, &"S2.6 scatter exited {sCode}:\n{sOutp}"
  var shards: seq[string]
  for i in 1..2:
    shards.add(tmpDir / ("shard_" & $i & ".shard.vcf.gz"))
  let outPath = tmpDir / "merged.vcf.gz"
  let (gOutp, gCode) = runGatherSubcmd(&"-o {outPath} " & shards.join(" "))
  doAssert gCode == 0, &"S2.6: gather with matching #CHROM should succeed, got {gCode}:\n{gOutp}"
  let got  = countRecords(outPath)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"S2.6: record count {got} != {orig}"
  removeDir(tmpDir)
  echo &"PASS G7.6 gather #CHROM match: {got} records, success"

# ---------------------------------------------------------------------------
# S2.7 — gather subcommand: mismatched #CHROM → exit 1, no partial output
# ---------------------------------------------------------------------------

block testGatherChromMismatch:
  # Build two synthetic BGZF-VCF files with different sample columns.
  let tmpDir = getTempDir() / "vcfparty_s2_mismatch"
  removeDir(tmpDir)
  createDir(tmpDir)
  let vcfA =
    "##fileformat=VCFv4.2\n" &
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSampleA\n" &
    "1\t100\t.\tA\tT\t.\tPASS\t.\tGT\t0/1\n"
  let vcfB =
    "##fileformat=VCFv4.2\n" &
    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSampleB\n" &
    "1\t200\t.\tG\tC\t.\tPASS\t.\tGT\t1/1\n"
  var rawA, rawB: seq[byte]
  for c in vcfA: rawA.add(byte(c))
  for c in vcfB: rawB.add(byte(c))
  let shardA = tmpDir / "shardA.vcf.gz"
  let shardB = tmpDir / "shardB.vcf.gz"
  block:
    var dataA = compressToBgzfMulti(rawA); dataA.add(@BGZF_EOF)
    let fA = open(shardA, fmWrite)
    discard fA.writeBytes(dataA, 0, dataA.len); fA.close()
  block:
    var dataB = compressToBgzfMulti(rawB); dataB.add(@BGZF_EOF)
    let fB = open(shardB, fmWrite)
    discard fB.writeBytes(dataB, 0, dataB.len); fB.close()

  let outPath = tmpDir / "merged.vcf.gz"
  let (gOutp, gCode) = runGatherSubcmd(&"-o {outPath} {shardA} {shardB}")
  doAssert gCode != 0, &"S2.7: mismatched #CHROM should exit non-zero, got {gCode}"
  doAssert not fileExists(outPath) or getFileSize(outPath) == 0,
    "S2.7: partial output should not exist"
  doAssert "chrom" in gOutp.toLowerAscii or "mismatch" in gOutp.toLowerAscii,
    &"S2.7: error message should mention mismatch, got: {gOutp}"
  removeDir(tmpDir)
  echo "PASS G7.7 gather #CHROM mismatch: exits 1, no partial output"

# ===========================================================================
# G8 — gather --concat / --merge flag tests
# ===========================================================================

# ---------------------------------------------------------------------------
# G9.2 — --merge: warning emitted, output is correct (falls back to --concat)
# ---------------------------------------------------------------------------

block testGatherMergeWarning:
  let tmpDir = getTempDir() / "vcfparty_g9_merge"
  createDir(tmpDir)
  let (sOutp, sCode) = execCmdEx(
    BinPath & &" scatter -n 4 -o {tmpDir}/shard.vcf.gz {SmallVcf} 2>&1")
  doAssert sCode == 0, &"G9.2 scatter exited {sCode}:\n{sOutp}"
  var shards: seq[string]
  for i in 1..4:
    shards.add(tmpDir / ("shard_" & $i & ".shard.vcf.gz"))
  let shardsArg = shards.join(" ")
  let outPath = tmpDir / "gathered.vcf.gz"
  # Capture stderr too to check for the warning.
  let (gOutp, gCode) = runGatherSubcmd(&"--merge -o {outPath} {shardsArg}")
  doAssert gCode == 0, &"G9.2 gather --merge exited {gCode}:\n{gOutp}"
  doAssert fileExists(outPath), "G9.2: output missing"
  let got  = countRecords(outPath)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"G9.2: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallVcf),
    "G9.2: content hash mismatch"
  removeDir(tmpDir)
  echo &"PASS G8.1 gather --merge: {got} records, content hash matches"

# ---------------------------------------------------------------------------
# G9 — extractContigTable
# ---------------------------------------------------------------------------

block testExtractContigTableVcf:
  ## Read small.vcf.gz (BGZF VCF) and verify contigs are extracted in order.
  let f = open(SmallVcf, fmRead)
  var buf = newSeq[byte](1024 * 1024)  # 1 MB — enough for the whole file
  let got = readBytes(f, buf, 0, buf.len)
  f.close()
  let headerBytes = buf[0 ..< got]
  let contigs = extractContigTable(headerBytes)
  doAssert contigs.len >= 3, &"M1 VCF: expected >= 3 contigs, got {contigs.len}"
  doAssert contigs[0] == "chr1", &"M1 VCF: contigs[0] = {contigs[0]}, expected chr1"
  doAssert contigs[1] == "chr2", &"M1 VCF: contigs[1] = {contigs[1]}, expected chr2"
  doAssert contigs[2] == "chr3", &"M1 VCF: contigs[2] = {contigs[2]}, expected chr3"
  echo &"PASS G9.1 extractContigTable VCF: {contigs.len} contigs, first = {contigs[0]}"

block testExtractContigTableBcf:
  ## Read small.bcf (BGZF BCF) and verify contigs are extracted in order.
  let f = open(SmallBcf, fmRead)
  var buf = newSeq[byte](1024 * 1024)
  let got = readBytes(f, buf, 0, buf.len)
  f.close()
  let headerBytes = buf[0 ..< got]
  let contigs = extractContigTable(headerBytes)
  doAssert contigs.len >= 3, &"M1 BCF: expected >= 3 contigs, got {contigs.len}"
  doAssert contigs[0] == "chr1", &"M1 BCF: contigs[0] = {contigs[0]}, expected chr1"
  doAssert contigs[1] == "chr2", &"M1 BCF: contigs[1] = {contigs[1]}, expected chr2"
  doAssert contigs[2] == "chr3", &"M1 BCF: contigs[2] = {contigs[2]}, expected chr3"
  echo &"PASS G9.2 extractContigTable BCF: {contigs.len} contigs, first = {contigs[0]}"

block testExtractContigTableEmpty:
  ## Empty input → empty result.
  let contigs = extractContigTable(@[])
  doAssert contigs.len == 0, &"M1 empty: expected 0 contigs, got {contigs.len}"
  echo "PASS G9.3 extractContigTable empty input: 0 contigs"

block testExtractContigTableNoContigs:
  ## VCF header with no ##contig lines → empty result.
  let header = "##fileformat=VCFv4.2\n##FILTER=<ID=PASS,Description=\"All filters passed\">\n#CHROM\tPOS\tID\n"
  var bytes = newSeq[byte](header.len)
  for i in 0 ..< header.len: bytes[i] = byte(header[i])
  let contigs = extractContigTable(bytes)
  doAssert contigs.len == 0, &"M1 no-contig: expected 0, got {contigs.len}"
  echo "PASS G9.4 extractContigTable no ##contig lines: 0 contigs"

block testExtractContigTableSynthetic:
  ## Synthetic VCF with multiple ##contig lines in a specific order.
  let header = "##fileformat=VCFv4.2\n##contig=<ID=chrX,length=100>\n##contig=<ID=chrY,length=200>\n##contig=<ID=chrM,length=300>\n#CHROM\tPOS\n"
  var bytes = newSeq[byte](header.len)
  for i in 0 ..< header.len: bytes[i] = byte(header[i])
  let contigs = extractContigTable(bytes)
  doAssert contigs == @["chrX", "chrY", "chrM"],
    &"M1 synthetic: expected [chrX,chrY,chrM], got {contigs}"
  echo "PASS G9.5 extractContigTable synthetic 3-contig header: order preserved"

# ---------------------------------------------------------------------------
# G10 — readNextVcfRecord and readNextBcfRecord
# ---------------------------------------------------------------------------

proc makePipe(): (cint, cint) =
  ## Create a pipe; returns (readFd, writeFd).
  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0, "pipe() failed"
  result = (fds[0], fds[1])

block testReadNextVcfRecordBasic:
  ## Three VCF lines → readNextVcfRecord reads each in order; EOF → empty.
  let lines = "chr1\t100\t.\tA\tT\t.\tPASS\t.\n" &
              "chr1\t200\t.\tG\tC\t.\tPASS\t.\n" &
              "chr2\t50\t.\tT\tA\t.\tPASS\t.\n"
  var data = newSeq[byte](lines.len)
  for i in 0 ..< lines.len: data[i] = byte(lines[i])
  let (rfd, wfd) = makePipe()
  discard posix.write(wfd, cast[pointer](addr data[0]), data.len)
  discard posix.close(wfd)
  let r1 = readNextVcfRecord(rfd)
  let r2 = readNextVcfRecord(rfd)
  let r3 = readNextVcfRecord(rfd)
  let eof = readNextVcfRecord(rfd)
  discard posix.close(rfd)
  doAssert r1.len > 0 and r1[r1.high] == byte('\n'), "M2 VCF r1: no trailing newline"
  doAssert r2.len > 0 and r2[r2.high] == byte('\n'), "M2 VCF r2: no trailing newline"
  doAssert r3.len > 0 and r3[r3.high] == byte('\n'), "M2 VCF r3: no trailing newline"
  doAssert eof.len == 0, "M2 VCF EOF: expected empty seq"
  let s1 = cast[string](r1)
  doAssert s1.startsWith("chr1\t100"), &"M2 VCF r1 content: {s1}"
  let s2 = cast[string](r2)
  doAssert s2.startsWith("chr1\t200"), &"M2 VCF r2 content: {s2}"
  let s3 = cast[string](r3)
  doAssert s3.startsWith("chr2\t50"), &"M2 VCF r3 content: {s3}"
  echo "PASS G10.1 readNextVcfRecord: 3 lines read correctly, EOF returns empty"

block testReadNextVcfRecordEmpty:
  ## Empty pipe → EOF immediately.
  let (rfd, wfd) = makePipe()
  discard posix.close(wfd)
  let rec = readNextVcfRecord(rfd)
  discard posix.close(rfd)
  doAssert rec.len == 0, "M2 VCF empty: expected empty"
  echo "PASS G10.2 readNextVcfRecord: empty input returns empty seq"

block testReadNextBcfRecordBasic:
  ## Synthetic BCF records: two records with known fields.
  ## Record layout: l_shared(4 LE) + l_indiv(4 LE) + shared bytes + indiv bytes.
  ## We use l_shared=8, l_indiv=0 with CHROM=0(int32) POS=99(int32) as shared data.
  proc makeRecord(chromId: int32; pos: int32; lIndiv: int = 0): seq[byte] =
    let lShared: int32 = 8   # 4 (CHROM) + 4 (POS)
    result = newSeq[byte](8 + lShared + lIndiv)
    result[0] = byte(lShared and 0xff)
    result[1] = byte((lShared shr 8) and 0xff)
    result[2] = byte((lShared shr 16) and 0xff)
    result[3] = byte((lShared shr 24) and 0xff)
    # l_indiv = 0
    result[8]  = byte(chromId and 0xff)
    result[9]  = byte((chromId shr 8) and 0xff)
    result[10] = byte((chromId shr 16) and 0xff)
    result[11] = byte((chromId shr 24) and 0xff)
    result[12] = byte(pos and 0xff)
    result[13] = byte((pos shr 8) and 0xff)
    result[14] = byte((pos shr 16) and 0xff)
    result[15] = byte((pos shr 24) and 0xff)

  let rec1 = makeRecord(0'i32, 100'i32)
  let rec2 = makeRecord(1'i32, 50'i32)
  var data: seq[byte]
  data.add(rec1)
  data.add(rec2)
  let (rfd, wfd) = makePipe()
  discard posix.write(wfd, cast[pointer](addr data[0]), data.len)
  discard posix.close(wfd)
  let r1 = readNextBcfRecord(rfd)
  let r2 = readNextBcfRecord(rfd)
  let eof = readNextBcfRecord(rfd)
  discard posix.close(rfd)
  doAssert r1.len == rec1.len, &"M2 BCF r1 len: {r1.len} != {rec1.len}"
  doAssert r2.len == rec2.len, &"M2 BCF r2 len: {r2.len} != {rec2.len}"
  doAssert eof.len == 0, "M2 BCF EOF: expected empty seq"
  # Verify CHROM and POS fields in r1.
  let c1 = int32(r1[8].uint32 or (r1[9].uint32 shl 8) or (r1[10].uint32 shl 16) or (r1[11].uint32 shl 24))
  let p1 = int32(r1[12].uint32 or (r1[13].uint32 shl 8) or (r1[14].uint32 shl 16) or (r1[15].uint32 shl 24))
  doAssert c1 == 0'i32, &"M2 BCF r1 CHROM: {c1}"
  doAssert p1 == 100'i32, &"M2 BCF r1 POS: {p1}"
  # Verify CHROM in r2.
  let c2 = int32(r2[8].uint32 or (r2[9].uint32 shl 8) or (r2[10].uint32 shl 16) or (r2[11].uint32 shl 24))
  doAssert c2 == 1'i32, &"M2 BCF r2 CHROM: {c2}"
  echo "PASS G10.3 readNextBcfRecord: 2 records read correctly, EOF returns empty"

# ---------------------------------------------------------------------------
# G11 — extractSortKey
# ---------------------------------------------------------------------------

block testExtractSortKeyVcf:
  ## VCF records: verify contig rank and 0-based pos.
  let contigs = @["chr1", "chr2", "chr3"]

  proc vcfRec(chrom: string; pos: int): seq[byte] =
    let line = chrom & "\t" & $pos & "\t.\tA\tT\t.\tPASS\t.\n"
    result = newSeq[byte](line.len)
    for i in 0 ..< line.len: result[i] = byte(line[i])

  let (rank1, pos1) = extractSortKey(vcfRec("chr1", 100), ffVcf, contigs)
  doAssert rank1 == 0, &"M3 VCF chr1 rank: {rank1}"
  doAssert pos1 == 99'i32, &"M3 VCF chr1 pos: {pos1} (VCF 100 → 0-based 99)"

  let (rank2, pos2) = extractSortKey(vcfRec("chr2", 1), ffVcf, contigs)
  doAssert rank2 == 1, &"M3 VCF chr2 rank: {rank2}"
  doAssert pos2 == 0'i32, &"M3 VCF chr2 pos: {pos2}"

  let (rank3, pos3) = extractSortKey(vcfRec("chr3", 500), ffVcf, contigs)
  doAssert rank3 == 2, &"M3 VCF chr3 rank: {rank3}"
  doAssert pos3 == 499'i32, &"M3 VCF chr3 pos: {pos3}"

  let (rankU, _) = extractSortKey(vcfRec("chrX", 1), ffVcf, contigs)
  doAssert rankU == high(int), &"M3 VCF unknown contig rank: {rankU}"

  echo "PASS G11.1 extractSortKey VCF: rank and 0-based pos correct"

block testExtractSortKeyBcf:
  ## BCF records: CHROM at offset 8, POS at offset 12 of the full record bytes.
  let contigs = @["chr1", "chr2", "chr3"]

  proc bcfRec(chromId: int32; pos: int32): seq[byte] =
    result = newSeq[byte](16)
    result[0] = 8   # l_shared = 8
    # l_indiv = 0 (bytes 4..7 already zero)
    result[8]  = byte(chromId and 0xff)
    result[9]  = byte((chromId shr 8) and 0xff)
    result[10] = byte((chromId shr 16) and 0xff)
    result[11] = byte((chromId shr 24) and 0xff)
    result[12] = byte(pos and 0xff)
    result[13] = byte((pos shr 8) and 0xff)
    result[14] = byte((pos shr 16) and 0xff)
    result[15] = byte((pos shr 24) and 0xff)

  let (rank0, pos0) = extractSortKey(bcfRec(0'i32, 0'i32), ffBcf, contigs)
  doAssert rank0 == 0, &"M3 BCF chromId=0 rank: {rank0}"
  doAssert pos0 == 0'i32, &"M3 BCF chromId=0 pos: {pos0}"

  let (rank2, pos2) = extractSortKey(bcfRec(2'i32, 999'i32), ffBcf, contigs)
  doAssert rank2 == 2, &"M3 BCF chromId=2 rank: {rank2}"
  doAssert pos2 == 999'i32, &"M3 BCF chromId=2 pos: {pos2}"

  let (rankU, _) = extractSortKey(bcfRec(99'i32, 0'i32), ffBcf, contigs)
  doAssert rankU == high(int), &"M3 BCF out-of-range chromId rank: {rankU}"

  echo "PASS G11.2 extractSortKey BCF: rank and pos from correct offsets"

block testExtractSortKeyRealVcf:
  ## Read first data record from small.vcf.gz and verify sort key is consistent.
  let (outp, code) = execCmdEx("bcftools view -HG " & SmallVcf & " 2>/dev/null | head -1")
  doAssert code == 0, "M3 real VCF: bcftools failed"
  let firstLine = outp.strip
  doAssert firstLine.len > 0, "M3 real VCF: no records found"
  let fields = firstLine.split('\t')
  let chrom = fields[0]
  let vcfPos = parseInt(fields[1])
  var rec = newSeq[byte](firstLine.len + 1)
  for i in 0 ..< firstLine.len: rec[i] = byte(firstLine[i])
  rec[firstLine.len] = byte('\n')
  let contigs = @["chr1", "chr2", "chr3"]
  let (rank, pos) = extractSortKey(rec, ffVcf, contigs)
  var expectedRank = high(int)
  for ci in 0 ..< contigs.len:
    if contigs[ci] == chrom: expectedRank = ci; break
  doAssert rank == expectedRank, &"M3 real VCF rank: {rank} != {expectedRank} (chrom={chrom})"
  doAssert pos == int32(vcfPos - 1), &"M3 real VCF pos: {pos} != {vcfPos - 1}"
  echo &"PASS G11.3 extractSortKey real VCF: {chrom}:{vcfPos} → rank={rank} pos={pos}"

# ---------------------------------------------------------------------------
# G12 — kWayMerge
# ---------------------------------------------------------------------------

block testKWayMergeVcfTwoStreams:
  ## Merge 2 sorted VCF streams: output sorted and contains all records.
  let contigs = @["chr1", "chr2"]
  proc vcfRec(chrom: string; pos: int): seq[byte] =
    let s = chrom & "\t" & $pos & "\t.\tA\tT\t.\tPASS\t.\n"
    result = newSeq[byte](s.len)
    for i in 0 ..< s.len: result[i] = byte(s[i])

  # Stream 1: chr1:100, chr1:300, chr2:50  (sorted)
  # Stream 2: chr1:200, chr2:10, chr2:100  (sorted)
  let (rfd1, wfd1) = makePipe()
  let (rfd2, wfd2) = makePipe()
  block:
    var d: seq[byte]
    for r in @[vcfRec("chr1", 100), vcfRec("chr1", 300), vcfRec("chr2", 50)]: d.add(r)
    discard posix.write(wfd1, cast[pointer](addr d[0]), d.len)
  discard posix.close(wfd1)
  block:
    var d: seq[byte]
    for r in @[vcfRec("chr1", 200), vcfRec("chr2", 10), vcfRec("chr2", 100)]: d.add(r)
    discard posix.write(wfd2, cast[pointer](addr d[0]), d.len)
  discard posix.close(wfd2)

  let tmpOut = getTempDir() / "vcfparty_m4_1.vcf"
  let outFd = posix.open(tmpOut.cstring, O_WRONLY or O_CREAT or O_TRUNC, Mode(0o644))
  doAssert outFd >= 0, "M4.1: failed to open output file"
  kWayMerge(@[rfd1, rfd2], outFd, ffVcf, contigs)
  discard posix.close(outFd)
  discard posix.close(rfd1)
  discard posix.close(rfd2)

  var resultLines: seq[string]
  for line in readFile(tmpOut).splitLines:
    if line.len > 0: resultLines.add(line)
  removeFile(tmpOut)

  doAssert resultLines.len == 6, &"M4.1: expected 6 records, got {resultLines.len}"
  let expectedPfx = @["chr1\t100", "chr1\t200", "chr1\t300",
                      "chr2\t10",  "chr2\t50",  "chr2\t100"]
  for i, pfx in expectedPfx:
    doAssert resultLines[i].startsWith(pfx),
      &"M4.1: record {i}: expected '{pfx}', got '{resultLines[i]}'"
  echo "PASS G12.1 kWayMerge VCF: 2 streams merged in sorted order, all 6 records"

block testKWayMergeSingleStream:
  ## Single stream → output equals input (passthrough).
  let contigs = @["chr1", "chr2"]
  proc vcfRec(chrom: string; pos: int): seq[byte] =
    let s = chrom & "\t" & $pos & "\t.\tA\tT\t.\tPASS\t.\n"
    result = newSeq[byte](s.len)
    for i in 0 ..< s.len: result[i] = byte(s[i])

  let (rfd, wfd) = makePipe()
  block:
    var d: seq[byte]
    for r in @[vcfRec("chr1", 10), vcfRec("chr2", 20)]: d.add(r)
    discard posix.write(wfd, cast[pointer](addr d[0]), d.len)
  discard posix.close(wfd)

  let tmpOut = getTempDir() / "vcfparty_m4_2.vcf"
  let outFd = posix.open(tmpOut.cstring, O_WRONLY or O_CREAT or O_TRUNC, Mode(0o644))
  doAssert outFd >= 0, "M4.2: failed to open output file"
  kWayMerge(@[rfd], outFd, ffVcf, contigs)
  discard posix.close(outFd)
  discard posix.close(rfd)

  var resultLines: seq[string]
  for line in readFile(tmpOut).splitLines:
    if line.len > 0: resultLines.add(line)
  removeFile(tmpOut)

  doAssert resultLines.len == 2, &"M4.2: expected 2 records, got {resultLines.len}"
  doAssert resultLines[0].startsWith("chr1\t10"),  &"M4.2: record 0: {resultLines[0]}"
  doAssert resultLines[1].startsWith("chr2\t20"),  &"M4.2: record 1: {resultLines[1]}"
  echo "PASS G12.2 kWayMerge VCF single stream: passthrough, order preserved"

block testKWayMergeBcfTwoStreams:
  ## Merge 2 sorted BCF streams (synthetic records); output sorted and complete.
  let contigs = @["chr1", "chr2"]
  proc bcfRec(chromId: int32; pos: int32): seq[byte] =
    result = newSeq[byte](16)
    result[0] = 8  # l_shared = 8 (4 CHROM + 4 POS)
    # l_indiv = 0 (bytes 4..7 zero)
    result[8]  = byte(chromId and 0xff);         result[9]  = byte((chromId shr 8) and 0xff)
    result[10] = byte((chromId shr 16) and 0xff); result[11] = byte((chromId shr 24) and 0xff)
    result[12] = byte(pos and 0xff);             result[13] = byte((pos shr 8) and 0xff)
    result[14] = byte((pos shr 16) and 0xff);    result[15] = byte((pos shr 24) and 0xff)

  # Stream 1: (0,50), (1,100)   Stream 2: (0,75), (1,200)
  # Expected merged: (0,50), (0,75), (1,100), (1,200)
  let (rfd1, wfd1) = makePipe()
  let (rfd2, wfd2) = makePipe()
  block:
    var d: seq[byte]
    for r in @[bcfRec(0'i32, 50'i32), bcfRec(1'i32, 100'i32)]: d.add(r)
    discard posix.write(wfd1, cast[pointer](addr d[0]), d.len)
  discard posix.close(wfd1)
  block:
    var d: seq[byte]
    for r in @[bcfRec(0'i32, 75'i32), bcfRec(1'i32, 200'i32)]: d.add(r)
    discard posix.write(wfd2, cast[pointer](addr d[0]), d.len)
  discard posix.close(wfd2)

  let tmpOut = getTempDir() / "vcfparty_m4_3.bcf"
  let outFd = posix.open(tmpOut.cstring, O_WRONLY or O_CREAT or O_TRUNC, Mode(0o644))
  doAssert outFd >= 0, "M4.3: failed to open output file"
  kWayMerge(@[rfd1, rfd2], outFd, ffBcf, contigs)
  discard posix.close(outFd)
  discard posix.close(rfd1)
  discard posix.close(rfd2)

  # Read back raw bytes and parse records.
  let raw = block:
    let f = open(tmpOut, fmRead)
    var buf = newSeq[byte](4096)
    let got = readBytes(f, buf, 0, buf.len)
    f.close()
    buf[0 ..< got]
  removeFile(tmpOut)

  proc leI32(d: seq[byte]; p: int): int32 =
    int32(d[p].uint32 or (d[p+1].uint32 shl 8) or (d[p+2].uint32 shl 16) or (d[p+3].uint32 shl 24))

  doAssert raw.len == 64, &"M4.3: expected 64 bytes (4 × 16), got {raw.len}"
  let expected = @[(0'i32, 50'i32), (0'i32, 75'i32), (1'i32, 100'i32), (1'i32, 200'i32)]
  for i, (expChrom, expPos) in expected:
    let base = i * 16
    doAssert leI32(raw, base + 8)  == expChrom, &"M4.3: rec {i} CHROM: {leI32(raw, base+8)}"
    doAssert leI32(raw, base + 12) == expPos,   &"M4.3: rec {i} POS: {leI32(raw, base+12)}"
  echo "PASS G12.3 kWayMerge BCF: 2 streams merged in sorted order, all 4 records"

