## Tests for gather.nim — G1: types, inferGatherFormat, validateGatherConfig.
##                         G2: isBgzfStream, sniffFormat, sniffStreamFormat.
## Run from project root: nim c -d:debug -r tests/test_gather.nim

import std/[os, osproc, options, strformat, strutils]
import std/posix
import "../src/vcfparty/gather"
import "../src/vcfparty/bgzf_utils"

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
    discard inferGatherFormat("output.vcf.gz", "bam")
  of "--exit-test-mutual-exclusion":
    let cfg = GatherConfig(headerPattern: some("#"), headerN: some(1))
    validateGatherConfig(cfg)
  of "--exit-test-vcf-header-flag":
    let cfg = GatherConfig(format: gfVcf, headerPattern: some("#"))
    validateGatherConfig(cfg)
  of "--exit-test-bcf-header-flag":
    let cfg = GatherConfig(format: gfBcf, headerN: some(1))
    validateGatherConfig(cfg)
  else: discard

# ---------------------------------------------------------------------------
# G1.1 — testInferAllExtensions: all valid extensions (happy paths)
# ---------------------------------------------------------------------------

block testInferAllExtensions:
  block:
    let (f, c) = inferGatherFormat("out.vcf.gz", "")
    doAssert f == gfVcf,          &".vcf.gz: expected gfVcf, got {f}"
    doAssert c == gcBgzf,         &".vcf.gz: expected gcBgzf, got {c}"
  block:
    let (f, c) = inferGatherFormat("out.vcf.bgz", "")
    doAssert f == gfVcf,          &".vcf.bgz: expected gfVcf, got {f}"
    doAssert c == gcBgzf,         &".vcf.bgz: expected gcBgzf, got {c}"
  block:
    let (f, c) = inferGatherFormat("out.bcf", "")
    doAssert f == gfBcf,          &".bcf: expected gfBcf, got {f}"
    doAssert c == gcBgzf,         &".bcf: expected gcBgzf, got {c}"
  block:
    let (f, c) = inferGatherFormat("out.vcf", "")
    doAssert f == gfVcf,          &".vcf: expected gfVcf, got {f}"
    doAssert c == gcUncompressed, &".vcf: expected gcUncompressed, got {c}"
  block:
    let (f, c) = inferGatherFormat("out.txt.gz", "")
    doAssert f == gfText,         &".txt.gz: expected gfText, got {f}"
    doAssert c == gcBgzf,         &".txt.gz: expected gcBgzf, got {c}"
  block:
    let (f, c) = inferGatherFormat("out.txt", "")
    doAssert f == gfText,         &".txt: expected gfText, got {f}"
    doAssert c == gcUncompressed, &".txt: expected gcUncompressed, got {c}"
  block:
    let (f, c) = inferGatherFormat("out.out.gz", "")
    doAssert f == gfText,         &"any.gz: expected gfText, got {f}"
    doAssert c == gcBgzf,         &"any.gz: expected gcBgzf, got {c}"
  block:
    let (f, c) = inferGatherFormat("out.bgz", "")
    doAssert f == gfText,         &".bgz: expected gfText, got {f}"
    doAssert c == gcBgzf,         &".bgz: expected gcBgzf, got {c}"
  block:
    let (f, c) = inferGatherFormat("out.xyz", "")
    doAssert f == gfText,         &".xyz: expected gfText (unknown ext), got {f}"
    doAssert c == gcUncompressed, &".xyz: expected gcUncompressed, got {c}"
  echo "PASS inferGatherFormat: all extensions (including .vcf.bgz, .bgz, unknown)"

# ---------------------------------------------------------------------------
# G1.2 — testInferWithOverride: fmtOverride overrides format, compression from extension
# ---------------------------------------------------------------------------

block testInferWithOverride:
  # Override format; compression still from path extension.
  block:
    let (f, c) = inferGatherFormat("out.vcf.gz", "bcf")
    doAssert f == gfBcf and c == gcBgzf,
      &".vcf.gz + --gather-fmt bcf: got ({f}, {c})"
  block:
    let (f, c) = inferGatherFormat("out.vcf.gz", "txt")
    doAssert f == gfText and c == gcBgzf,
      &".vcf.gz + --gather-fmt txt: got ({f}, {c})"
  # Unknown extension with valid override → format from override, compression gcUncompressed.
  block:
    let (f, c) = inferGatherFormat("out.xyz", "vcf")
    doAssert f == gfVcf and c == gcUncompressed,
      &".xyz + --gather-fmt vcf: got ({f}, {c})"
  echo "PASS inferGatherFormat: fmtOverride overrides format, compression from extension"

# ---------------------------------------------------------------------------
# G1.3 — testValidateConfigOk: non-conflicting configs pass
# ---------------------------------------------------------------------------

block testValidateConfigOk:
  validateGatherConfig(GatherConfig())                                              # neither set
  validateGatherConfig(GatherConfig(format: gfText, headerPattern: some("#")))     # text + pattern
  validateGatherConfig(GatherConfig(format: gfText, headerN: some(3)))             # text + n
  echo "PASS validateGatherConfig: non-conflicting configs pass"

# ---------------------------------------------------------------------------
# G1 — exit-1 paths via subprocess (G1.4–G1.7)
# ---------------------------------------------------------------------------

const SelfSrc   = "tests/test_gather.nim"
const HelperBin = "/tmp/vcfparty_test_gather_helper"

block buildHelper:
  let (outp, code) = execCmdEx(
    "nim c -d:debug -o:" & HelperBin & " " & SelfSrc & " 2>&1")
  doAssert code == 0, "failed to compile test helper:\n" & outp
  echo "PASS buildHelper: compiled exit-1 test helper"

# ---------------------------------------------------------------------------
# G1.4 — testInferBadOverride: invalid fmtOverride exits 1
# ---------------------------------------------------------------------------

block testInferBadOverride:
  let (_, code) = execCmdEx(HelperBin & " --exit-test-bad-override 2>/dev/null")
  doAssert code != 0, "invalid fmtOverride should exit non-zero"
  echo "PASS G1.4 inferGatherFormat: invalid fmtOverride exits 1"

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
# G2.1 — testIsBgzfStream: BGZF magic detected; plain gzip and random bytes rejected
# ---------------------------------------------------------------------------

block testIsBgzfStream:
  # BGZF magic: 1f 8b 08 04
  let bgzfHead = [0x1f'u8, 0x8b, 0x08, 0x04, 0x00]
  doAssert isBgzfStream(bgzfHead), "should detect BGZF magic"

  # Plain gzip (not BGZF): 1f 8b 08 00 — bit 0x04 not set
  let gzHead = [0x1f'u8, 0x8b, 0x08, 0x00, 0x00]
  doAssert not isBgzfStream(gzHead), "plain gzip is not BGZF"

  # Random bytes
  let rnd = [0x42'u8, 0x43, 0x46, 0x02]
  doAssert not isBgzfStream(rnd), "random bytes are not BGZF"

  # Too short
  let short = [0x1f'u8, 0x8b]
  doAssert not isBgzfStream(short), "too-short buffer is not BGZF"

  echo "PASS isBgzfStream"

# ---------------------------------------------------------------------------
# G2.2 — testSniffFormat: BCF/VCF/text detected from uncompressed bytes
# ---------------------------------------------------------------------------

block testSniffFormat:
  # BCF magic: B C F \x02 \x02
  let bcfBytes = [byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8, 0x00'u8]
  doAssert sniffFormat(bcfBytes) == gfBcf, "BCF magic → gfBcf"

  # VCF: starts with ##fileformat
  var vcfBytes: seq[byte]
  for c in "##fileformatVCFv4.2\n":
    vcfBytes.add(byte(c))
  doAssert sniffFormat(vcfBytes) == gfVcf, "##fileformat → gfVcf"

  # Text: something else
  var txtBytes: seq[byte]
  for c in "CHROM\tPOS\tID\n":
    txtBytes.add(byte(c))
  doAssert sniffFormat(txtBytes) == gfText, "other bytes → gfText"

  # Too short to match either magic
  doAssert sniffFormat([0x00'u8, 0x01'u8]) == gfText, "short buffer → gfText"

  # Exactly BCF magic length
  let bcfExact = [byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8]
  doAssert sniffFormat(bcfExact) == gfBcf, "exact BCF magic length → gfBcf"

  echo "PASS sniffFormat"

# ---------------------------------------------------------------------------
# G2.3–G2.7 — testSniffStreamFormat*: fixture files and synthetic streams detected correctly
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# G2.3 — testSniffStreamFormatBcf: small.bcf detected as BCF/BGZF
# ---------------------------------------------------------------------------

block testSniffStreamFormatBcf:
  doAssert fileExists(SmallBcf), "BCF fixture missing — run generate_fixtures.sh"
  let f = open(SmallBcf, fmRead)
  var buf = newSeq[byte](65536)
  let n = readBytes(f, buf, 0, 65536)
  f.close()
  buf.setLen(n)
  let (fmt, isBgzf) = sniffStreamFormat(buf)
  doAssert fmt == gfBcf,  &"small.bcf: expected gfBcf, got {fmt}"
  doAssert isBgzf,         "small.bcf: expected BGZF stream"
  echo "PASS sniffStreamFormat: small.bcf detected as BCF/BGZF"

# ---------------------------------------------------------------------------
# G2.4 — testSniffStreamFormatVcf: small.vcf.gz detected as VCF/BGZF
# ---------------------------------------------------------------------------

block testSniffStreamFormatVcf:
  doAssert fileExists(SmallVcf), "VCF fixture missing — run generate_fixtures.sh"
  let f = open(SmallVcf, fmRead)
  var buf = newSeq[byte](65536)
  let n = readBytes(f, buf, 0, 65536)
  f.close()
  buf.setLen(n)
  let (fmt, isBgzf) = sniffStreamFormat(buf)
  doAssert fmt == gfVcf,  &"small.vcf.gz: expected gfVcf, got {fmt}"
  doAssert isBgzf,         "small.vcf.gz: expected BGZF stream"
  echo "PASS sniffStreamFormat: small.vcf.gz detected as VCF/BGZF"

# ---------------------------------------------------------------------------
# G2.5 — testSniffStreamFormatText: plain uncompressed text detected as text/uncompressed
# ---------------------------------------------------------------------------

block testSniffStreamFormatText:
  # Uncompressed text (no BGZF magic, no BCF/VCF magic).
  var raw: seq[byte]
  for c in "col1\tcol2\tcol3\nhello\tworld\t42\n":
    raw.add(byte(c))
  let (fmt, isBgzf) = sniffStreamFormat(raw)
  doAssert fmt == gfText, &"plain text: expected gfText, got {fmt}"
  doAssert not isBgzf,    "plain text: should not be BGZF"
  echo "PASS sniffStreamFormat: plain text detected as text/uncompressed"

# ---------------------------------------------------------------------------
# G2.6 — testSniffStreamFormatUncompressedVcf: uncompressed VCF detected as VCF/uncompressed
# ---------------------------------------------------------------------------

block testSniffStreamFormatUncompressedVcf:
  # Uncompressed VCF (##fileformat header but no BGZF wrapper).
  var raw: seq[byte]
  for c in "##fileformatVCFv4.2\n##source=paravar\n#CHROM\tPOS\n":
    raw.add(byte(c))
  let (fmt, isBgzf) = sniffStreamFormat(raw)
  doAssert fmt == gfVcf,  &"uncompressed VCF: expected gfVcf, got {fmt}"
  doAssert not isBgzf,    "uncompressed VCF: should not be BGZF"
  echo "PASS sniffStreamFormat: uncompressed VCF detected as VCF/uncompressed"

# ---------------------------------------------------------------------------
# G2.7 — testSniffStreamFormatCompressedText: BGZF-compressed text detected as text/BGZF
# ---------------------------------------------------------------------------

block testSniffStreamFormatCompressedText:
  # BGZF-compressed plain text.
  var raw: seq[byte]
  for c in "hello world\n":
    raw.add(byte(c))
  let compressed = compressToBgzf(raw)
  let (fmt, isBgzf) = sniffStreamFormat(compressed)
  doAssert fmt == gfText, &"BGZF text: expected gfText, got {fmt}"
  doAssert isBgzf,         "BGZF text: expected BGZF stream"
  echo "PASS sniffStreamFormat: BGZF-compressed text detected as text/BGZF"

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

  echo "PASS G3.1 findBcfHeaderEnd"

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

  echo "PASS G3.2 findVcfHeaderEnd"

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

  echo "PASS G3.3 stripBcfHeader"

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

  echo "PASS G3.4 stripLinesByPattern"

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

  echo "PASS G3.5 stripFirstNLines"

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
  gDetectedFormat = gfBcf
  gStreamIsBgzf   = false
  gFormatDetected = true

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr bcfData[0]), bcfData.len)
  discard posix.close(fds[1])

  const TmpBcf = "/tmp/vcfparty_test_g3_bcf.bin"
  discard runInterceptor(GatherConfig(format: gfBcf, compression: gcUncompressed),
                 shardIdx = 1, fds[0], TmpBcf)

  let f = open(TmpBcf, fmRead)
  var outBuf = newSeq[byte](1024)
  let n = readBytes(f, outBuf, 0, 1024)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpBcf)

  doAssert outBuf == records,
    &"BCF interceptor strip: expected records only, got {outBuf}"
  echo "PASS runInterceptor BCF header stripping"

# ---------------------------------------------------------------------------
# G3.7 — testRunInterceptorVcfStrip: VCF header stripped via pipe (shard 1)
# ---------------------------------------------------------------------------

block testRunInterceptorVcfStrip:
  var vcfData: seq[byte]
  for c in "##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t100\n":
    vcfData.add(byte(c))

  gDetectedFormat = gfVcf
  gStreamIsBgzf   = false
  # Set gChromLineBuf to match the #CHROM line in vcfData above.
  let chromStr = "#CHROM\tPOS"
  gChromLineLen = chromStr.len.int32
  for k in 0 ..< chromStr.len:
    gChromLineBuf[k] = byte(chromStr[k])
  gFormatDetected = true

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr vcfData[0]), vcfData.len)
  discard posix.close(fds[1])

  const TmpVcf = "/tmp/vcfparty_test_g3_vcf.txt"
  let cfg = GatherConfig(format: gfVcf, compression: gcUncompressed)
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
  echo "PASS runInterceptor VCF header stripping (automatic)"

# ---------------------------------------------------------------------------
# G3.8 — testRunInterceptorHeaderN: --header-n strips first N lines via pipe (shard 1)
# ---------------------------------------------------------------------------

block testRunInterceptorHeaderN:
  var data: seq[byte]
  for c in "hdr1\nhdr2\ndata1\ndata2\n": data.add(byte(c))

  gDetectedFormat = gfText
  gStreamIsBgzf   = false
  gFormatDetected = true

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr data[0]), data.len)
  discard posix.close(fds[1])

  const TmpN = "/tmp/vcfparty_test_g3_headern.txt"
  let cfg = GatherConfig(format: gfText, compression: gcUncompressed, headerN: some(2))
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
  echo "PASS runInterceptor text header stripping (--header-n)"

# ---------------------------------------------------------------------------
# G3.9 — testRunInterceptorNoStrip: text with no flags passes through unchanged (shard 1)
# ---------------------------------------------------------------------------

block testRunInterceptorNoStrip:
  # Text format with no flags: stream passes through unchanged.
  var data: seq[byte]
  for c in "##header\nrecord1\nrecord2\n": data.add(byte(c))

  gDetectedFormat = gfText
  gStreamIsBgzf   = false
  gFormatDetected = true

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr data[0]), data.len)
  discard posix.close(fds[1])

  const TmpPass = "/tmp/vcfparty_test_g3_nostrip.txt"
  let cfg = GatherConfig(format: gfText, compression: gcUncompressed)
  discard runInterceptor(cfg, shardIdx = 1, fds[0], TmpPass)

  let f = open(TmpPass, fmRead)
  var outBuf = newSeq[byte](1024)
  let n = readBytes(f, outBuf, 0, 1024)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpPass)

  doAssert outBuf == data, "no-strip: text stream should pass through unchanged"
  echo "PASS runInterceptor no-strip pass-through (text format, no flags)"

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
  discard runInterceptor(GatherConfig(format: gfText, compression: gcBgzf),
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
  echo "PASS runInterceptor recompress uncompressed → BGZF"

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
  discard runInterceptor(GatherConfig(format: gfText, compression: gcUncompressed),
                 shardIdx = 0, fds[0], TmpDecomp)

  let f = open(TmpDecomp, fmRead)
  var outBuf = newSeq[byte](65536)
  let n = readBytes(f, outBuf, 0, 65536)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpDecomp)

  doAssert not isBgzfStream(outBuf), "decomp: output should not be BGZF"
  doAssert outBuf == raw, "decomp: output should be original uncompressed content"
  echo "PASS runInterceptor decompress BGZF → uncompressed"

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
  discard runInterceptor(GatherConfig(format: gfText, compression: gcBgzf),
                 shardIdx = 0, fds[0], TmpPassBgzf)

  let f = open(TmpPassBgzf, fmRead)
  var outBuf = newSeq[byte](65536)
  let n = readBytes(f, outBuf, 0, 65536)
  f.close()
  outBuf.setLen(n)
  removeFile(TmpPassBgzf)

  doAssert outBuf == compressed,
    "BGZF pass-through: output bytes should be identical to input"
  echo "PASS runInterceptor pass-through BGZF → BGZF"

# ---------------------------------------------------------------------------
# G4.4 — testShardStripAndRecompress: uncompressed VCF shard 1 stripped + recompressed to BGZF
# ---------------------------------------------------------------------------

block testShardStripAndRecompress:
  var vcfData: seq[byte]
  for c in "##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t100\n": vcfData.add(byte(c))

  gDetectedFormat = gfVcf
  gStreamIsBgzf   = false
  let chromStr4 = "#CHROM\tPOS"
  gChromLineLen = chromStr4.len.int32
  for k in 0 ..< chromStr4.len:
    gChromLineBuf[k] = byte(chromStr4[k])
  gFormatDetected = true

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr vcfData[0]), vcfData.len)
  discard posix.close(fds[1])

  const TmpShard = "/tmp/vcfparty_test_g4_shard.bgzf"
  discard runInterceptor(GatherConfig(format: gfVcf, compression: gcBgzf),
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
  echo "PASS runInterceptor shard strip (uncompressed) + BGZF recompression"

# ---------------------------------------------------------------------------
# G4.5 — testShardBgzfInputStripAndRecompress: BGZF VCF shard 1 stripped + recompressed to BGZF
# ---------------------------------------------------------------------------

block testShardBgzfInputStripAndRecompress:
  var raw: seq[byte]
  for c in "##fileformat=VCFv4.2\n#CHROM\tPOS\n1\t200\n": raw.add(byte(c))
  let compressed = compressToBgzfMulti(raw)

  gDetectedFormat = gfVcf
  gStreamIsBgzf   = true
  let chromStr5 = "#CHROM\tPOS"
  gChromLineLen = chromStr5.len.int32
  for k in 0 ..< chromStr5.len:
    gChromLineBuf[k] = byte(chromStr5[k])
  gFormatDetected = true

  var fds: array[2, cint]
  doAssert posix.pipe(fds) == 0
  discard posix.write(fds[1], cast[pointer](unsafeAddr compressed[0]), compressed.len)
  discard posix.close(fds[1])

  const TmpBgzfShard = "/tmp/vcfparty_test_g4_bgzf_shard.bgzf"
  discard runInterceptor(GatherConfig(format: gfVcf, compression: gcBgzf),
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
  echo "PASS runInterceptor shard strip (BGZF input) + BGZF recompression"

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
  echo "PASS cleanupTempDir success: files and dir deleted"

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
  echo "PASS cleanupTempDir failure: files preserved on disk"

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

  let cfg = GatherConfig(format: gfText, compression: gcUncompressed,
                         outputPath: OutPath, tmpDir: TmpDir)
  concatenateShards(cfg, @[shard1, shard2])

  doAssert readFile(OutPath) == "line1\nline2\nline3\nline4\n",
    "concat uncompressed: unexpected content"
  doAssert not fileExists(shard1), "concat: shard1 should be deleted"
  doAssert not fileExists(shard2), "concat: shard2 should be deleted"
  doAssert not dirExists(TmpDir),  "concat: tmpDir should be deleted"
  removeFile(OutPath)
  echo "PASS concatenateShards uncompressed"

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

  let cfg = GatherConfig(format: gfVcf, compression: gcBgzf,
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
  echo "PASS concatenateShards BGZF with EOF block"

echo ""
echo "All gather G1/G2/G3/G4/G5 unit tests passed."

# ===========================================================================
# G7 — Integration tests via compiled binary
# ===========================================================================

const BinPath = "./vcfparty"

block buildBinary:
  if not fileExists(BinPath):
    let (outp, code) = execCmdEx("nimble build 2>&1")
    if code != 0:
      echo "nimble build failed:\n", outp
      quit(1)
  doAssert fileExists(BinPath), "binary not found: " & BinPath
  echo "PASS binary available (G7)"

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
    &"-n 4 --gather -o {outPath} {SmallVcf} --- bcftools view -Oz")
  doAssert code == 0, &"VCF gather -Oz exited {code}:\n{outp}"
  doAssert fileExists(outPath), "VCF gather -Oz: output missing"
  let got = countRecords(outPath)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"VCF gather -Oz: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallVcf),
    "VCF gather -Oz: content hash mismatch"
  removeDir(tmpDir)
  echo &"PASS G7.1 VCF gather -Oz: {got} records, content hash matches"

# ---------------------------------------------------------------------------
# G7.2 — VCF gather, uncompressed pipeline (-Ov) → .vcf.gz (recompression)
# ---------------------------------------------------------------------------

block testGatherVcfRecompress:
  let tmpDir = getTempDir() / "vcfparty_g7_vcf_ov"
  createDir(tmpDir)
  let outPath = tmpDir / "out.vcf.gz"
  let (outp, code) = runGather(
    &"-n 4 --gather -o {outPath} {SmallVcf} --- bcftools view -Ov")
  doAssert code == 0, &"VCF gather -Ov exited {code}:\n{outp}"
  doAssert fileExists(outPath), "VCF gather -Ov: output missing"
  let got = countRecords(outPath)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"VCF gather -Ov: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallVcf),
    "VCF gather -Ov: content hash mismatch (recompression corrupted data)"
  removeDir(tmpDir)
  echo &"PASS G7.2 VCF gather -Ov (recompress uncompressed→BGZF): {got} records, hash matches"

# ---------------------------------------------------------------------------
# G7.3 — BCF gather, compressed pipeline (-Ob) → .bcf
# ---------------------------------------------------------------------------

block testGatherBcfCompressed:
  let tmpDir = getTempDir() / "vcfparty_g7_bcf_ob"
  createDir(tmpDir)
  let outPath = tmpDir / "out.bcf"
  let (outp, code) = runGather(
    &"-n 4 --gather -o {outPath} {SmallBcf} --- bcftools view -Ob")
  doAssert code == 0, &"BCF gather -Ob exited {code}:\n{outp}"
  doAssert fileExists(outPath), "BCF gather -Ob: output missing"
  let got = countRecords(outPath)
  let orig = countRecords(SmallBcf)
  doAssert got == orig, &"BCF gather -Ob: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallBcf),
    "BCF gather -Ob: content hash mismatch"
  removeDir(tmpDir)
  echo &"PASS G7.3 BCF gather -Ob: {got} records, content hash matches"

# ---------------------------------------------------------------------------
# G7.4 — BCF gather, uncompressed pipeline (-Ou) → .bcf (recompression)
# ---------------------------------------------------------------------------

block testGatherBcfRecompress:
  let tmpDir = getTempDir() / "vcfparty_g7_bcf_ou"
  createDir(tmpDir)
  let outPath = tmpDir / "out.bcf"
  let (outp, code) = runGather(
    &"-n 4 --gather -o {outPath} {SmallBcf} --- bcftools view -Ou")
  doAssert code == 0, &"BCF gather -Ou exited {code}:\n{outp}"
  doAssert fileExists(outPath), "BCF gather -Ou: output missing"
  let got = countRecords(outPath)
  let orig = countRecords(SmallBcf)
  doAssert got == orig, &"BCF gather -Ou: record count {got} != {orig}"
  doAssert recordsHash(outPath) == recordsHash(SmallBcf),
    "BCF gather -Ou: content hash mismatch (recompression corrupted data)"
  removeDir(tmpDir)
  echo &"PASS G7.4 BCF gather -Ou (recompress uncompressed→BGZF): {got} records, hash matches"

# ---------------------------------------------------------------------------
# G7.5 — Text gather (bcftools query) → .txt, no stripping
# ---------------------------------------------------------------------------

block testGatherText:
  let tmpDir = getTempDir() / "vcfparty_g7_text"
  createDir(tmpDir)
  let outPath = tmpDir / "out.txt"
  let (outp, code) = runGather(
    &"-n 4 --gather -o {outPath} {SmallVcf} --- bcftools query -f '%CHROM\\t%POS\\n'")
  doAssert code == 0, &"text gather exited {code}:\n{outp}"
  doAssert fileExists(outPath), "text gather: output missing"
  let (lineOut, _) = execCmdEx("wc -l < " & outPath)
  let lineCount = lineOut.strip.parseInt
  let orig = countRecords(SmallVcf)
  doAssert lineCount == orig,
    &"text gather: expected {orig} lines, got {lineCount}"
  removeDir(tmpDir)
  echo &"PASS G7.5 text gather → .txt: {lineCount} lines, matches record count"

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
    &"-n 4 --gather -o {outPath} --header-n 1 {SmallVcf} --- sh -c {quoteShell(pipeline)}")
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
  echo &"PASS G7.6 text gather --header-n 1: {lineCount} lines (1 header + {orig} data)"

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
    &"-n 4 --gather -o {outPath} " &
    &"{SmallVcf} --- bcftools query -f '%CHROM\\t%POS\\n'")
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
  echo &"PASS G7.7 unknown-ext (.out.gz) text gather: {lineCount} lines, valid BGZF"

# ---------------------------------------------------------------------------
# G7.8 — Shard failure: temp files left on disk, exit 1, paths in stderr
# ---------------------------------------------------------------------------

block testGatherShardFailure:
  let tmpDir  = getTempDir() / "vcfparty_g7_fail"
  let tDir    = getTempDir() / "vcfparty_g7_fail_tmp"
  createDir(tmpDir)
  let outPath = tmpDir / "out.vcf.gz"
  let (outp, code) = runGather(
    &"-n 2 --gather -o {outPath} --tmp-dir {tDir} {SmallVcf} --- false")
  doAssert code != 0, "shard failure: vcfparty should exit non-zero"
  # At least one temp path should be printed to stderr.
  doAssert tDir in outp,
    &"shard failure: expected tmp dir path in stderr, got:\n{outp}"
  # Temp dir should still exist (files left for debugging).
  doAssert dirExists(tDir), "shard failure: tmp dir should be preserved"
  # Cleanup.
  removeDir(tmpDir)
  removeDir(tDir)
  echo "PASS G7.8 shard failure: exit non-zero, temp paths printed, files preserved"

# ---------------------------------------------------------------------------
# G7.9 — --tmp-dir custom path
# ---------------------------------------------------------------------------

block testGatherCustomTmpDir:
  let tmpDir    = getTempDir() / "vcfparty_g7_custom_tmp"
  let customTmp = getTempDir() / "vcfparty_g7_my_tmp_dir"
  createDir(tmpDir)
  let outPath = tmpDir / "out.vcf.gz"
  let (outp, code) = runGather(
    &"-n 4 --gather -o {outPath} --tmp-dir {customTmp} " &
    &"{SmallVcf} --- bcftools view -Oz")
  doAssert code == 0, &"--tmp-dir exited {code}:\n{outp}"
  doAssert fileExists(outPath), "--tmp-dir: output missing"
  # On success the custom tmp dir should be cleaned up.
  doAssert not dirExists(customTmp), "--tmp-dir: custom dir should be removed on success"
  let got = countRecords(outPath)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"--tmp-dir: record count {got} != {orig}"
  removeDir(tmpDir)
  echo &"PASS G7.9 --tmp-dir custom path: {got} records, tmp dir cleaned up"

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
    &"-n 4 --gather -o {outPath} --header-pattern \"##\" {SmallVcf} --- sh -c {quoteShell(pipeline)}")
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
  echo &"PASS G7.10 text gather --header-pattern: {lineCount} lines (1 header + {orig} data)"

echo ""
echo "All gather G7 integration tests passed."

# ===========================================================================
# G8 — gather subcommand integration tests
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
  echo &"PASS G8.1 gather subcommand VCF: {got} records, content hash matches"

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
  echo &"PASS G8.2 gather subcommand BCF: {got} records, content hash matches"

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
  echo &"PASS G8.3 gather subcommand stdout: {got} records written to stdout"

# ---------------------------------------------------------------------------
# G8.3b — /dev/stdout explicit path also works
# ---------------------------------------------------------------------------

block testGatherSubcmdDevStdout:
  let tmpDir = getTempDir() / "vcfparty_g8_devstdout"
  createDir(tmpDir)
  let (sOutp, sCode) = execCmdEx(
    BinPath & &" scatter -n 4 -o {tmpDir}/shard.vcf.gz {SmallVcf} 2>&1")
  doAssert sCode == 0, &"G8.3b scatter exited {sCode}:\n{sOutp}"
  var shards: seq[string]
  for i in 1..4:
    shards.add(tmpDir / ("shard_" & $i & ".shard.vcf.gz"))
  let shardsArg = shards.join(" ")
  let stdoutFile = tmpDir / "stdout.vcf"
  let (gOutp, gCode) = execCmdEx(
    BinPath & &" gather -o /dev/stdout {shardsArg} > {stdoutFile} 2>&1")
  doAssert gCode == 0, &"G8.3b gather /dev/stdout exited {gCode}:\n{gOutp}"
  let got  = countRecords(stdoutFile)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"G8.3b: record count {got} != {orig}"
  removeDir(tmpDir)
  echo &"PASS G8.3b gather subcommand /dev/stdout: {got} records"

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
  echo "PASS G8.4 gather subcommand: no input files exits non-zero"

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
  echo "PASS G8.5 gather subcommand: missing input file exits non-zero"

# ---------------------------------------------------------------------------
# G8.6 — run --gather without -o: output goes to stdout
# ---------------------------------------------------------------------------

block testRunGatherStdout:
  let tmpDir = getTempDir() / "vcfparty_g8_rungather_stdout"
  createDir(tmpDir)
  let stdoutFile = tmpDir / "stdout.vcf"
  # No -o; stdout is captured via shell redirection.
  let (outp, code) = execCmdEx(
    BinPath & &" run --gather -n 4 {SmallVcf} ::: cat > {stdoutFile} 2>&1")
  doAssert code == 0, &"G8.6 run --gather stdout exited {code}:\n{outp}"
  let got  = countRecords(stdoutFile)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"G8.6: record count {got} != {orig}"
  removeDir(tmpDir)
  echo &"PASS G8.6 run --gather stdout: {got} records written to stdout"

echo ""
echo "All gather G8 subcommand integration tests passed."

# ===========================================================================
# S2 — #CHROM header validation tests
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
  echo "PASS S2.1 extractChromLine: found #CHROM in VCF header bytes"

# ---------------------------------------------------------------------------
# S2.2 — extractChromLine: returns "" when not found
# ---------------------------------------------------------------------------

block testExtractChromLineNotFound:
  var data: seq[byte]
  for c in "##fileformat=VCFv4.2\n##INFO=<ID=DP>\n":
    data.add(byte(c))
  let line = extractChromLine(data)
  doAssert line == "", &"S2.2: expected '' got '{line}'"
  echo "PASS S2.2 extractChromLine: returns empty string when not found"

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
  doAssert fmt == gfVcf, &"S2.3: expected gfVcf, got {fmt}"
  let line = chromLineFromBytes(allBytes, fmt, isBgzf)
  doAssert line.startsWith("#CHROM"), &"S2.3: expected '#CHROM...' got '{line}'"
  echo "PASS S2.3 chromLineFromBytes: found #CHROM in BGZF VCF"

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
  doAssert fmt == gfBcf, &"S2.4: expected gfBcf, got {fmt}"
  let line = chromLineFromBytes(allBytes, fmt, isBgzf)
  doAssert line.startsWith("#CHROM"), &"S2.4: expected '#CHROM...' got '{line}'"
  echo "PASS S2.4 chromLineFromBytes: found #CHROM in BGZF BCF"

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
  echo "PASS S2.5 chromLineFromFile: matches chromLineFromBytes result"

# ---------------------------------------------------------------------------
# S2.6 — gather subcommand: matching #CHROM lines → success
# ---------------------------------------------------------------------------

block testGatherChromMatch:
  let tmpDir = getTempDir() / "vcfparty_s2_match"
  createDir(tmpDir)
  let (sOutp, sCode) = execCmdEx(
    BinPath & &" scatter -n 4 -o {tmpDir}/shard.vcf.gz {SmallVcf} 2>&1")
  doAssert sCode == 0, &"S2.6 scatter exited {sCode}:\n{sOutp}"
  var shards: seq[string]
  for i in 1..4:
    shards.add(tmpDir / ("shard_" & $i & ".shard.vcf.gz"))
  let outPath = tmpDir / "merged.vcf.gz"
  let (gOutp, gCode) = runGatherSubcmd(&"-o {outPath} " & shards.join(" "))
  doAssert gCode == 0, &"S2.6: gather with matching #CHROM should succeed, got {gCode}:\n{gOutp}"
  let got  = countRecords(outPath)
  let orig = countRecords(SmallVcf)
  doAssert got == orig, &"S2.6: record count {got} != {orig}"
  removeDir(tmpDir)
  echo &"PASS S2.6 gather #CHROM match: {got} records, success"

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
  echo "PASS S2.7 gather #CHROM mismatch: exits 1, no partial output"

echo ""
echo "All S2 #CHROM validation tests passed."
