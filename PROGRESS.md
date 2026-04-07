# vcfparty — Project Summary

## What it is

**vcfparty** is a Nim CLI tool for splitting bgzipped VCF and BCF files into N roughly equal shards without decompressing the middle blocks, and optionally piping each shard through a tool pipeline in parallel, with optional output gathering back into a single file.

The key design goal is speed — middle BGZF blocks are byte-copied from disk without decompression or recompression. Only the boundary blocks (one per shard split) are decompressed and recompressed.

---

## Current scope

All four subcommand modes are implemented for VCF (`*.vcf.gz`) and BCF (`*.bcf`) inputs.

### `scatter`

```
vcfparty scatter -n <n> -o <o> [options] <input.vcf.gz|input.bcf>
```

| Flag | Long form | Description |
|------|-----------|-------------|
| `-n` | `--n-shards` | Number of output shards (required, ≥ 1) |
| `-o` | `--output` | Output filename (required) |
| `-t` | `--max-threads` | Max threads for scan/split/write (default: min(n-shards, 8)) |
| | `--force-scan` | Always scan BGZF blocks — VCF only; exits 1 for BCF input |
| `-v` | `--verbose` | Print progress info to stderr |
| `-h` | `--help` | Show usage |

**Output naming** — when `{}` is absent, shard number is prepended as `shard_XX.`:

```
-o output.vcf.gz, -n 8   →  shard_01.output.vcf.gz … shard_08.output.vcf.gz
-o out.bcf, -n 4          →  shard_1.out.bcf … shard_4.out.bcf
```

When `-o` contains `{}`, the placeholder is replaced with the zero-padded shard number:

```
-o output.{}.vcf.gz       →  output.01.vcf.gz … output.08.vcf.gz
-o /results/{}/batch.bcf  →  /results/01/batch.bcf … /results/08/batch.bcf
```

Parent directories are created automatically (`mkdir -p`).

**BCF:** requires a `.csi` index alongside the input. No auto-scan fallback (unlike VCF).

### `run`

Scatter → parallel per-shard tool pipelines → per-shard output files. No temporary files; shard bytes flow directly from the scatter writer into the stdin pipe of the shell pipeline.

```
vcfparty run -n <n> -o <o> [options] <input.vcf.gz|input.bcf> \
  (--- | :::) <cmd1> [args...] \
  [(--- | :::) <cmd2> [args...] ...]
```

`---` (three dashes) and `:::` are both valid pipe-stage separators and may be mixed freely. Multiple separator blocks define a pipeline joined with `|`.

| Flag | Long form | Description |
|------|-----------|-------------|
| `-n` | `--n-shards` | Number of shards (required, ≥ 1) |
| `-o` | `--output` | Output filename (required) |
| `-j` | `--max-jobs` | Max concurrent shard pipelines (default: n-shards) |
| `-t` | `--max-threads` | Max threads for scatter/validation (default: min(max-jobs, 8)) |
| | `--force-scan` | Always scan BGZF blocks — VCF only; exits 1 for BCF input |
| | `--no-kill` | On failure, let sibling shards finish (default: kill siblings) |
| `-v` | `--verbose` | Print per-shard progress to stderr |
| `-h` | `--help` | Show usage |

**Output:** per-shard files named using the same `shard_XX.` / `{}` scheme as `scatter`.

### `run --gather`

As `run`, but intercept each shard's stdout, strip duplicate headers from shards 2..N, and concatenate all shard outputs into a single output file. No external tools (bcftools etc.) used — all concatenation is native Nim.

```
vcfparty run --gather [-o <output.vcf.gz>] [options] <input.vcf.gz|input.bcf> \
  (--- | :::) <cmd> [args...]
```

`--gather` is a bare flag. `-o` is the single gather output path (no shard numbering applied). If `-o` is omitted or set to `/dev/stdout`, output is written to stdout uncompressed.

| Flag | Long form | Description |
|------|-----------|-------------|
| | `--gather` | Bare flag — gather shard outputs into a single `-o` file |
| | `--header-pattern <pat>` | Strip lines with this prefix from shards 2..N (**text format only**) |
| | `--header-n <n>` | Strip first N lines from shards 2..N (**text format only**) |
| | `--tmp-dir <dir>` | Temp dir for shard files (default: `$TMPDIR/vcfparty`) |

### `gather`

Concatenate pre-existing shard files (output of `scatter` or `run`) into a single output file. Same header-stripping and recompression logic as `run --gather`. No temp files needed — reads directly from input files.

```
vcfparty gather [-o <output.vcf.gz>] [options] <shard1> [<shard2> ...]
```

If `-o` is omitted or set to `/dev/stdout`, output is written to stdout uncompressed.

| Flag | Long form | Description |
|------|-----------|-------------|
| `-o` | `--output` | Output path (optional; omit for stdout) |
| | `--header-pattern <pat>` | Strip lines with this prefix from shards 2..N (**text format only**) |
| | `--header-n <n>` | Strip first N lines from shards 2..N (**text format only**) |
| `-v` | `--verbose` | Print progress to stderr |
| `-h` | `--help` | Show usage |

**Format inference from `-o` extension:**

| Extension | Format | Compression |
|-----------|--------|-------------|
| `.vcf.gz` or `.vcf.bgz` | VCF | BGZF |
| `.vcf` | VCF | Uncompressed |
| `.bcf` | BCF | BGZF |
| `.gz` or `.bgz` (any other stem) | Text | BGZF |
| Anything else | Text | Uncompressed |

**Header stripping:**
- **VCF**: lines starting with `#` stripped automatically from shards 2..N.
- **BCF**: the BCF header blob (`5 + 4 + l_text` bytes) stripped automatically from shards 2..N.
- **Text**: `--header-pattern` or `--header-n` apply; default is no stripping.
- Using `--header-pattern`/`--header-n` with VCF or BCF format exits 1 with an error.
- `--header-pattern` and `--header-n` are mutually exclusive.

**Recompression:** if the gather output requires BGZF but incoming bytes are uncompressed (e.g. `bcftools view -Ov`), the interceptor recompresses on the fly. The reverse (BGZF → uncompressed) is also handled.

**On failure (`run --gather`):** temp shard files are left on disk and their paths are printed to stderr. On success they are deleted.

---

## Implementation

### Module layout

| File | Responsibility |
|------|----------------|
| `src/vcfparty.nim` | Entry point (`include vcfparty/main`) |
| `src/vcfparty/main.nim` | CLI arg parsing (`parseopt`), subcommand dispatch (`scatter`/`run`/`gather`). `nextVal` supports attached short-flag values (e.g. `-j2`, `-n4`). |
| `src/vcfparty/scatter.nim` | Scatter algorithm: index parsing, boundary optimisation, shard writing. Exports `computeShards`, `doWriteShard`, `shardOutputPath`. |
| `src/vcfparty/bgzf_utils.nim` | Low-level BGZF I/O: scan blocks, decompress, compress, raw copy, boundary split, BCF record boundary split, virtual offset helpers. Vendored libdeflate (`vendor/libdeflate-1.25/`) statically linked. |
| `src/vcfparty/run.nim` | `run` subcommand: `---`/`:::` argv parsing, shell command construction, `fork`/`exec` per shard, worker pool. Gather mode: spawns per-shard interceptor threads, calls `concatenateShards`. |
| `src/vcfparty/gather.nim` | Gather module: types, format inference, sniffing, header stripping; shared shard-writing helpers (`stripTrailingEof`, `writeShardZero`, `writeShardData`); `runInterceptor`, `concatenateShards`, `cleanupTempDir`, `gatherFiles` (direct-file gather for `gather` subcommand). |

### Scatter algorithm — VCF path (4 phases)

**Phase 1 — coarse block offsets**

If a `.tbi` or `.csi` index exists alongside the input, it is parsed to extract BGZF virtual offsets, which are shifted right 16 bits to get file offsets of BGZF blocks containing indexed records. Both TBI and CSI formats are supported via hand-written binary parsers. If no index is found, all BGZF blocks in the file are scanned directly (`scanAllBlockStarts`) and a warning is printed; `--force-scan` forces this path even when an index is present.

**Phase 2 — header extraction and first data block**

Reads raw BGZF blocks from the start of the file, decompresses each, and collects all `#` lines until the first block containing a non-`#` data line. The collected bytes are recompressed via `compressToBgzfMulti`. No htslib dependency — pure file I/O and libdeflate.

**Phase 3 — shard boundary optimisation (single-pass)**

1. Computes cumulative byte lengths and uses weighted bisection (`partitionBoundaries`) to pick `n-1` initial split points from the coarse index block list.
2. For each candidate boundary block, calls `scanBgzfBlockStarts` to resolve finer BGZF sub-block offsets within that block's byte range. Runs in parallel when `--max-threads > 1`.
3. Merges the discovered sub-blocks into the full block list and repartitions once.
4. Validates each selected boundary block (decompresses, checks for a non-terminal newline). If a boundary is invalid (rare: would require a single VCF record > 65 KB uncompressed), the nearest valid neighbour in the enriched block list is used instead. If no valid neighbour exists, exits with an error.

**Phase 4 — write shards**

For each shard: recompressed header prepend, raw byte-copy of middle blocks, boundary split (decompress/recompress), BGZF EOF terminator.

### Scatter algorithm — BCF path

BCF uses an entirely different splitting strategy because BCF records can span BGZF block boundaries. The CSI `u_off` field encodes the exact uncompressed byte offset within a block where a record starts — `splitBgzfBlockAtUOffset` uses this to split without walking record lengths. Requires a `.csi` index; no auto-scan fallback.

### `shardOutputPath`

Exported from `scatter.nim`. Computes the output path for shard `i` of `n` total shards given the `-o` template:
- If the template contains `{}`, replace with zero-padded shard number.
- Otherwise, prepend `shard_XX.` to the filename component.

Zero-padding width is determined by `n` (e.g. width 2 for n ≤ 99, width 3 for n ≤ 999).

### `run` data flow (no gather)

```
input file
     │
  [computeShards]
     │
  shard 1 → posix.pipe() → sh -c "cmd1 | cmd2 | ..." → stdout → shard_01.out.vcf.gz
  shard N → posix.pipe() → sh -c "cmd1 | cmd2 | ..." → stdout → shard_0N.out.vcf.gz
```

### `run --gather` data flow

```
input file
     │
  [computeShards]
     │
  shard 0 → pipe → shell → stdout pipe → interceptor thread 0 → output file (direct)
  shard 1 → pipe → shell → stdout pipe → interceptor thread 1 → tmp/shard2.tmp
  shard N → pipe → shell → stdout pipe → interceptor thread N → tmp/shardN.tmp
                                                          │
                                              [concatenateShards]
                                              (appends shards 1..N to output)
                                                          │
                                                   output file
```

Shard 0's interceptor writes directly to the gather output file (no temp file). Shards 1..N write to numbered temp files. After all interceptors finish, `concatenateShards` opens the output file in append mode and appends each temp shard in order, then writes a single BGZF EOF block if BGZF output.

### `gather` data flow

```
shard_01.out.vcf.gz ──┐
shard_02.out.vcf.gz ──┤  [gatherFiles]  →  merged.vcf.gz
        …             │   (reads files directly, no temp files)
shard_0N.out.vcf.gz ──┘
```

`gatherFiles` reads each shard file entirely into memory, strips the trailing BGZF EOF block, detects format from shard 0, strips headers from shards 1..N, and writes a single output file with one trailing EOF block.

---

## Key technical notes

### No htslib dependency

Header extraction uses a direct BGZF block scan that collects `#` lines until the first data line. The only C dependency is libdeflate (vendored, statically linked — no runtime `.so` required).

### Gather: BGZF EOF block stripping

Each pipeline tool (e.g. `bcftools view -Ob`) writes a BGZF EOF block at the end of its stdout. The interceptor/`gatherFiles` strips this trailing 28-byte EOF block before writing, so that a single EOF block is written at the very end of the gather output.

### Gather: shard 0 direct write (`run --gather`)

Shard 0's interceptor writes directly to the gather output path instead of a temp file. `concatenateShards` opens the output in `fmAppend` mode and only appends shards 1..N.

### Gather: optimised BGZF header stripping (shards 1..N)

For BGZF VCF and BCF streams, shards 1..N decompress only the header-containing blocks (typically 1–2), find the header end via `findVcfHeaderEnd` / `findBcfHeaderEnd`, recompress the post-header tail of the boundary block, then raw-copy all remaining BGZF blocks unchanged. This avoids decompressing and recompressing the bulk of each shard's data.

### Gather: stdout output (S1)

`-o` is optional for both `gather` and `run --gather`. If omitted or set to `/dev/stdout`, `GatherConfig.toStdout = true` and output is written to `stdout` directly. `runInterceptor` skips closing the file handle for shard 0 in this mode. `concatenateShards` and `gatherFiles` both check `cfg.toStdout` before opening a file. Format compression defaults to `gcUncompressed` for stdout (avoiding a compressed stream on a terminal). The format-mismatch warning is suppressed for stdout mode.

### Gather: #CHROM line validation (S2)

For VCF and BCF formats, gather validates that the `#CHROM` line is identical across all shards before writing any output, preventing corrupt output from mismatched sample sets.

**`run --gather` path:** shard 0's interceptor extracts the `#CHROM` line via `chromLineFromBytes` after sniffing the format. The line is stored in the GC-safe globals `gChromLineBuf: array[131072, byte]` + `gChromLineLen: int32` (not a `string`, which would fail the `spawn` GC-safety check). `gFormatDetected = true` is set AFTER these globals are populated, so shards 1..N see both atomically. Each subsequent interceptor waits for `gFormatDetected`, then compares its own `#CHROM` line; a mismatch returns exit code 1 without writing any output.

**`gather` subcommand path (two-phase):** Phase 1 reads shard 0, detects format, and calls `chromLineFromFile` for each remaining shard — which reads only enough BGZF blocks to locate the `#CHROM` line. Any mismatch calls `quit(1)` before the output file is opened, guaranteeing no partial output. Phase 2 then opens the output and writes.

**New procs:**
- `extractChromLine(data: seq[byte]): string` — finds the first `#CHROM`-prefixed line in raw decompressed bytes.
- `chromLineFromBytes(rawBytes, fmt, isBgzf): string` — decompresses only header-containing blocks, then calls `extractChromLine`.
- `chromLineFromFile(path, fmt, isBgzf): string` — reads minimum BGZF blocks from a file to extract the `#CHROM` line (10 MB safety limit).

### Gather: shared shard-writing helpers

`runInterceptor` and `gatherFiles` share three helper procs extracted to eliminate duplication:

- `stripTrailingEof(bytes)` — removes the 28-byte BGZF EOF block from the end of a buffer if present. Called before any shard write.
- `writeShardZero(outFile, bytes, isBgzf, compression)` — writes shard 0 with no header stripping; handles the four recompression cases (BGZF↔uncompressed, pass-through).
- `writeShardData(outFile, bytes, fmt, isBgzf, cfg)` — writes shards 1..N with header stripping and recompression. Contains both the optimised BGZF path (decompress only header blocks, raw-copy data blocks) and the general path (decompress all, strip, recompress).

### Gather: format detection race condition

Small shards may buffer all pipeline output before shard 0 reads its first chunk and sets the global `gFormatDetected` flag. Shards 1..N spin-wait (`while not gFormatDetected: sleep(1)`) before accessing the detected format globals.

### Long header lines spanning BGZF block boundaries

`blockHasData` tracks `prevEndedWithNewline` across blocks to avoid spurious early detection of the first data block when headers span a block boundary.

### Large headers (> 65536 bytes uncompressed)

`compressToBgzfMulti` splits input into ≤ 65536-byte chunks. Used for VCF headers, BCF headers, and gather recompression.

### BCF records span BGZF block boundaries

The CSI virtual offset `u_off` field is the authoritative split point. `splitBgzfBlockAtUOffset` uses this to split a block at a record boundary without walking record lengths.

### Pipe deadlock prevention

After `fork()`, the child's inherited pipe write-end is explicitly closed before `execvp` to prevent deadlock when the shell stage reads stdin.

### O(n²) deduplication fix

Boundary merging sorts before `deduplicate(isSorted = true)` to avoid O(n²) behaviour on files with 100k+ TBI entries.

---

## Tests

### Fixtures (`tests/generate_fixtures.sh`)

Run once before testing.

| File | Description |
|------|-------------|
| `tests/data/tiny.vcf.gz` | 10 records, 1 BGZF block, TBI indexed |
| `tests/data/small.vcf.gz` | ~5000 records, 3 chromosomes, TBI indexed |
| `tests/data/small_csi.vcf.gz` | Same content, CSI indexed only |
| `tests/data/small.bcf` | BCF conversion of `small.vcf.gz`, CSI indexed |
| `tests/data/chr22_1kg.vcf.gz` | 25,000 records, 500 samples from 1000 Genomes chr22 (large header) |
| `tests/data/chr22_1kg.bcf` | BCF conversion of `chr22_1kg.vcf.gz`, CSI indexed |

### Test files

| File | Covers |
|------|--------|
| `tests/test_bgzf_utils.nim` | Block scanning, raw copy, compress/decompress round-trip, boundary split, `removeHeaderLines` multi-block, BCF record boundary split, `bcfFirstDataOffset`, `splitBgzfBlockAtUOffset`, BGZF CRC32 field validation |
| `tests/test_scatter.nim` | TBI/CSI index parsing, `parseCsiVirtualOffsets`, `scanAllBlockStarts`, partition boundaries, VCF scatter correctness (1 shard, 4 shards, CSI, no-index auto-scan, `--force-scan`), BCF header extraction, BCF scatter correctness (1 shard, 4 shards, large header) |
| `tests/test_cli.nim` | Error paths, no-index auto-scan, `--force-scan`, BCF mode, end-to-end with `small.vcf.gz` (4 shards, content hash), CSI VCF, BCF `--force-scan` rejection for `run`, optional 1KG chr22. Uses `shard_XX.` output naming throughout. |
| `tests/test_run.nim` | `parseRunArgv`/`buildShellCmd` unit tests (including `:::` separator and mixed `---`/`:::` separators); `runShards` direct calls (1 shard, 4 shards with content hash, serial `--max-jobs 1`, BCF 4 shards); CLI tests (multi-stage pipeline, `--max-jobs`, failure propagation, `:::` separator, `--` passthrough) |
| `tests/test_gather.nim` | **Unit:** `inferGatherFormat` (all extensions, overrides, error paths), `validateGatherConfig`, `isBgzfStream`, `sniffFormat`, `sniffStreamFormat`, `stripBcfHeader`, `stripLinesByPattern`, `stripFirstNLines`, `findBcfHeaderEnd`, `findVcfHeaderEnd`, `runInterceptor`; `concatenateShards`, `cleanupTempDir`. **Integration (`run --gather`):** VCF gather, BCF gather, text gather, format override, shard failure, `--tmp-dir`, stdout output (`run --gather` with no `-o`). **Integration (`gather` subcommand):** VCF gather (4 shards → record count + hash), BCF gather, stdout (`-o` omitted and `-o /dev/stdout`), no input files, missing input file. **S2 (#CHROM validation):** `extractChromLine` unit (found/not-found), `chromLineFromBytes` on BGZF VCF and BCF, `chromLineFromFile` round-trip, `gather` with matching #CHROM (success), `gather` with mismatched sample column (exits 1, no partial output). |

**Correctness verification:** scatter tests collect raw record bytes and compare sorted sets. Integration tests in `test_cli.nim`, `test_run.nim`, and `test_gather.nim` compute an ordered `sha256sum` of all records (via `bcftools view -H`) and compare to the original — catches byte-level corruption and reordering.

### Running

```bash
bash tests/generate_fixtures.sh        # once

export PATH="$HOME/.choosenim/toolchains/nim-2.2.8/bin:$PATH"

nimble build                           # builds libdeflate (first run) + debug binary
nimble release                         # release binary (-d:release)
nimble test                            # all tests
nim c -r tests/test_gather.nim        # single file
```

---

## Dependencies

| Dependency | Use |
|-----------|-----|
| libdeflate v1.25 | BGZF compress/decompress/CRC32 in `bgzf_utils.nim` |

No nimble package dependencies. libdeflate is vendored in `vendor/libdeflate-1.25/` and built automatically by `nimble build` on first run (requires cmake). The resulting binary is statically linked — no runtime `.so` dependency.

For offline/HPC use: place `vendor/libdeflate-1.25.tar.gz` in the repo before running `nimble build` to skip the download step.

---

## Out of scope (not implemented)

- `run` with pre-scattered input glob
- `--chunk` / `--stdout` flags
