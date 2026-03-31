# paravar — Project Summary

## What it is

**paravar** is a Nim CLI tool for splitting bgzipped VCF files into N roughly equal shards without decompressing the middle blocks, and optionally piping each shard through a tool pipeline in parallel.

The key design goal is speed — middle BGZF blocks are byte-copied from disk without decompression or recompression. Only the boundary blocks (one per shard split) are decompressed and recompressed.

---

## Current scope

Both `scatter` and `run` subcommands are implemented. `gather` and `index` are future work.

### `scatter`

```
paravar scatter -n <n_shards> -o <prefix> [options] <input.vcf.gz>
```

| Flag | Long form | Description |
|------|-----------|-------------|
| `-n` | `--n-shards` | Number of output shards (required, ≥ 1) |
| `-o` | `--output` | Output file prefix (required) |
| `-t` | `--max-threads` | Max threads for scan/split/write (default: min(n-shards, 8)) |
| | `--force-scan` | Always scan BGZF blocks (ignore index even if present) |
| `-v` | `--verbose` | Print progress info to stderr |
| `-h` | `--help` | Show usage |

**Output:** `<prefix>.1.vcf.gz`, `<prefix>.2.vcf.gz`, … (zero-padded to width of `n`).

### `run`

Scatter → parallel per-shard tool pipelines → per-shard output files. No temporary files; shard bytes flow directly from the scatter writer into the stdin pipe of the shell pipeline.

```
paravar run -n <n_shards> -o <prefix> [options] <input.vcf.gz> \
  --- <cmd1> [args...] \
  [--- <cmd2> [args...] ...]
```

`---` (three dashes) is the pipe-stage separator, chosen to avoid collision with tools that use `--`. Multiple `---` blocks define a pipeline joined with `|`.

| Flag | Long form | Description |
|------|-----------|-------------|
| `-n` | `--n-shards` | Number of shards (required, ≥ 1) |
| `-o` | `--output` | Output file prefix (required) |
| `-j` | `--max-jobs` | Max concurrent shard pipelines (default: n-shards) |
| `-t` | `--max-threads` | Max threads for scatter/validation (default: min(max-jobs, 8)) |
| | `--force-scan` | Always scan BGZF blocks |
| | `--no-kill` | On failure, let sibling shards finish (default: kill siblings) |
| `-v` | `--verbose` | Print per-shard progress to stderr |
| `-h` | `--help` | Show usage |

**Concurrency defaults cascade:** specify only `-n` and everything else is derived — `--max-jobs` defaults to `n-shards`, `--max-threads` defaults to `min(max-jobs, 8)`.

**Output:** `<prefix>.01.vcf.gz`, … — raw stdout of the final pipeline stage, one file per shard. paravar does not recompress or validate output; pass the right format flag (e.g. `-Oz`) to the last stage command.

**On failure:** by default, paravar sends SIGTERM to all in-flight sibling shards and exits 1. With `--no-kill`, siblings are allowed to complete (useful for debugging).

---

## Implementation

### Module layout

| File | Responsibility |
|------|----------------|
| `src/paravar.nim` | Entry point (`include paravar/main`) |
| `src/paravar/main.nim` | CLI arg parsing (`parseopt`), subcommand dispatch |
| `src/paravar/scatter.nim` | Scatter algorithm: index parsing, boundary optimisation, shard writing. Exports `computeShards` and `doWriteShard` for use by `run`. |
| `src/paravar/bgzf_utils.nim` | Low-level BGZF I/O: scan blocks, decompress, compress, raw copy, boundary split, `removeHeaderLines`. No external dependencies — only `-lz`. |
| `src/paravar/run.nim` | `run` subcommand: `---` argv parsing, shell command construction, `fork`/`exec` per shard, worker pool. |

### Scatter algorithm (4 phases)

**Phase 1 — coarse block offsets**

If a `.tbi` or `.csi` index exists alongside the input, it is parsed to extract BGZF virtual offsets, which are shifted right 16 bits to get file offsets of BGZF blocks containing indexed records. Both TBI and CSI formats are supported via hand-written binary parsers. If no index is found, all BGZF blocks in the file are scanned directly (`scanAllBlockStarts`) and a warning is printed; `--force-scan` forces this path even when an index is present.

**Phase 2 — header extraction and first data block**

Reads raw BGZF blocks from the start of the file, decompresses each, and collects all `#` lines until the first block containing a non-`#` data line. The collected bytes are recompressed via `compressToBgzfMulti` (handles headers larger than one 65536-byte BGZF block, e.g. VCFs with thousands of samples or many contig lines). No htslib dependency — pure file I/O and zlib.

**Phase 3 — shard boundary optimisation**

1. Computes cumulative byte lengths and uses weighted bisection (`partitionBoundaries`) to pick `n-1` split points producing roughly equal shard sizes.
2. For each candidate boundary block, calls `scanBgzfBlockStarts` to resolve finer sub-block offsets within the coarse index span.
3. Validates each boundary block by decompressing it and confirming a complete record line terminates before the next block. Invalid boundaries are excluded and the partition is recalculated. Up to 1000 iterations.
4. Scanning and validation run in parallel when `--max-threads > 1`.

**Phase 4 — write shards**

For each shard:
- **Prepend**: recompressed header. For shard 0, also includes the data-only portion of all blocks from file offset 0 to `starts[1]`, with all `#` lines stripped (`removeHeaderLines`). For shards 1..N-1, the tail half of the previous boundary split.
- **Middle**: raw byte-copy of whole BGZF blocks (no decompression).
- **Boundary**: decompress the split block, divide lines at the midpoint, recompress head (appended to this shard) and tail (prepended to next shard). Boundary splits run in parallel when `--max-threads > 1`.
- **Terminator**: explicit BGZF EOF block (28 bytes).

Shard writes run in parallel when `--max-threads > 1`.

### `run` data flow

```
input.vcf.gz
     │
  [computeShards]   ← scatter algorithm, writes shard bytes to pipe write-end fd
     │
  shard 1 → posix.pipe() → sh -c "cmd1 | cmd2 | ..." → stdout → prefix.01.vcf.gz
  shard 2 → posix.pipe() → sh -c "cmd1 | cmd2 | ..." → stdout → prefix.02.vcf.gz
  shard N → posix.pipe() → sh -c "cmd1 | cmd2 | ..." → stdout → prefix.0N.vcf.gz
                                                   (up to --max-jobs concurrent)
```

Each shard: `posix.pipe()` → `fork()` → child: `dup2` pipe read-end to stdin, `execvp("sh", ["-c", shellCmd])` → parent: spawn `doWriteShard` thread to write shard bytes to pipe write-end. Worker pool (sliding window) dispatches up to `--max-jobs` shards concurrently.

Shell command tokens are individually `quoteShell`-escaped before joining with `|`, so filter expressions containing `<`, `>`, `'` etc. are passed through safely without requiring the user to quote the entire stage.

---

## Key technical notes

### No htslib dependency

Header extraction was previously done via hts-nim (`bcf_hdr_format`), which also loaded the index and built internal hash tables — the main source of slow startup on large files. It is now replaced by a direct BGZF block scan that collects `#` lines until the first data line. This eliminates the `hts` nimble dependency entirely; the only C dependency is zlib (`-lz`).

### Long header lines spanning BGZF block boundaries

VCFs with many contigs or many samples can have `##contig` or `#CHROM` lines that span a BGZF block boundary. The second block starts with a line continuation that doesn't begin with `#`, which would naively look like a data record. `blockHasData` tracks `prevEndedWithNewline` across blocks: if the previous block didn't end with `\n`, the first "line" in the current block is skipped (it's a continuation). This fixes spurious early detection of the first data block.

### Large headers (> 65536 bytes uncompressed)

VCFs with thousands of samples can have headers exceeding the 65536-byte BGZF block limit. `compressToBgzfMulti` splits the input into ≤ 65536-byte chunks and produces multiple BGZF blocks, mirroring htslib's own writer behaviour.

### Interspersed `##` lines in data blocks

Some VCF generators emit `##contig=` or other meta-lines in data blocks (after the initial header region). The original `removeHeaderLines` only decompressed a single BGZF block, which meant any such lines in subsequent blocks appeared verbatim in shard 1's data section, causing bcftools to fail when it encountered records referencing an undeclared contig.

The fix mirrors the Python reference implementation: `removeHeaderLines` is called with the byte range `[0, starts[1])` — covering all header blocks and the entire first data block region — and now iterates through all BGZF blocks in that range before stripping `#` lines and recompressing.

### Pipe deadlock prevention

After `fork()`, the child inherits the pipe write-end. If not closed before `exec`, the child holds both ends of its own stdin pipe — `cat` (or any tool reading stdin) waits for EOF that never arrives. Fixed by explicitly closing the pipe write-end in the child process before `execvp`.

### EPIPE handling in writer threads

When a pipeline stage exits early (e.g. non-zero exit before consuming all stdin), writing to the pipe raises `IOError` (errno 32, Broken pipe) in the `doWriteShard` thread. This is caught and silently discarded; the actual failure is detected via `waitpid` exit code.

### O(n²) deduplication fix

The boundary-optimisation loop merges new sub-block starts into the existing `starts` list each iteration. Previously this called `deduplicate()` without `isSorted = true`, which is O(n²). For files with 100k+ TBI entries this caused multi-minute hangs. Fixed by sorting the concatenated list first and passing `isSorted = true`.

### Raw copy performance

Middle blocks (the bulk of each shard) are copied with 4 MiB read chunks via `rawCopyBytes`. No BGZF decompression or recompression is performed on these blocks.

---

## Tests

### Fixtures (`tests/generate_fixtures.sh`)

Run once before testing. Creates:

| File | Description |
|------|-------------|
| `tests/data/small.vcf.gz` | ~5000 records, 3 chromosomes, TBI indexed |
| `tests/data/small_csi.vcf.gz` | Same content, CSI indexed only (no `.tbi`) |
| `tests/data/chr22_1kg.vcf.gz` | 25,000 records subsampled from 1000 Genomes chr22 (large header: 225 KB, 2504 samples) |

The 1KG fixture is downloaded on first run by streaming `wget | bgzip -d | awk (first 25k records) | bgzip -c`. `pipefail` is disabled around this pipeline to suppress the expected SIGPIPE when awk exits early.

### Test files

| File | Covers |
|------|--------|
| `tests/test_bgzf_utils.nim` | Block scanning, raw copy, compress/decompress round-trip, boundary split, `removeHeaderLines` multi-block |
| `tests/test_scatter.nim` | TBI/CSI index parsing, `scanAllBlockStarts`, partition boundaries, full scatter correctness (record count, order, size balance) for TBI, CSI, no-index auto-scan, and `--force-scan` modes |
| `tests/test_cli.nim` | Error paths (missing `-n`, `-o`), no-index auto-scan (warning + valid shards), `--force-scan`, end-to-end with `small.vcf.gz` (4 shards) and CSI, optional 1KG chr22 (10 shards) |
| `tests/test_run.nim` | `parseRunArgv`/`buildShellCmd` unit tests; `runShards` direct calls (1 shard, 4 shards, serial `--max-jobs 1`, over-capacity jobs); CLI tests via binary (1/4 shards, multi-stage pipeline, `--max-jobs`, non-zero exit → paravar exits 1, missing `---`, `--` passthrough to bcftools plugins) |

### Running

```bash
bash tests/generate_fixtures.sh        # once

export PATH="$HOME/.choosenim/toolchains/nim-2.2.8/bin:$PATH"

nimble test                            # all tests
nim c -r tests/test_bgzf_utils.nim    # single file
nim c -r tests/test_scatter.nim
nim c -r tests/test_cli.nim
nim c -r tests/test_run.nim
```

---

## Dependencies

| Dependency | Use |
|-----------|-----|
| zlib (`-lz`) | BGZF compress/decompress in `bgzf_utils.nim` |

No nimble package dependencies. zlib is available system-wide or via conda.

---

## Out of scope (not implemented)

- BCF input/output
- `run` with pre-scattered input glob
- `--chunk` / `--stdout` flags
