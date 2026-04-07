# CLAUDE.md — vcfparty

Agentic development instructions for Claude Code. Read this file in full before starting any task.

---

## Project overview

**vcfparty** (formerly `paravar`) is a Nim CLI tool for parallelising VCF/BCF processing by exploiting BGZF block boundaries and tabix/CSI indexes.

The canonical reference for scatter behaviour is `example/scatter_vcf.py`.

---

## Current implementation state

All of the following are fully implemented and tested:

- `scatter` — split VCF/BCF into N shards
- `run` — scatter + parallel tool pipelines → per-shard outputs
- `run --gather` — as above, gathered into single output via temp files
- `gather` — concatenate existing shard files (`--concat` default, `--merge` for merge-sorted output; similar to `bcftools concat -a`)
- BCF support (CSI index required)
- Stdout output for `gather` and `run --gather`
- `#CHROM` header validation
- `{}` tool-managed output mode with `\{}` escaping

---

## Subcommands (current)

| Subcommand | Description |
|---|---|
| `scatter` | Split VCF/BCF into N shards |
| `run` | Scatter + parallel pipelines → per-shard outputs |
| `run --gather` | As above, gathered into single `-o` file |
| `gather` | Concatenate existing shard files |

---

## Module layout

| File | Responsibility |
|---|---|
| `src/paravar.nim` | Entry point |
| `src/paravar/main.nim` | CLI parsing, subcommand dispatch |
| `src/paravar/scatter.nim` | Scatter algorithm (VCF + BCF) |
| `src/paravar/bgzf_utils.nim` | Low-level BGZF I/O, no external deps beyond `-lz` |
| `src/paravar/run.nim` | Run mode: pipeline spawning, worker pool, interceptor coordination |
| `src/paravar/gather.nim` | Format inference, sniffing, stripping, interceptor thread, concatenation |

Do not restructure the module layout without asking the user.

---

## Planned work — step-by-step plan

Execute steps in order. Do not skip ahead. Check off each step before proceeding.

---

### Milestone 0: rename to `vcfparty`

- [ ] **V1** — Rename `paravar.nimble` → `vcfparty.nimble`. Update package name, binary name, and all internal `paravar` references in source files. Update all help text, error messages, and log output to say `vcfparty`. Update `README.md` and `CLAUDE.md` header. Run `nimble test` — all tests must pass with the new binary name.
- [ ] **V2** — Update all test files: any hardcoded `paravar` binary invocations → `vcfparty`. Update `generate_fixtures.sh` if it references the binary. Run `nimble test` — full suite green.

---

### Milestone 1: interface consolidation

This milestone retires `--gather`, introduces terminal operators, consolidates `-n`/`-j`, adds `--concat`/`--merge` flags to `gather`, adds `-O` and `-d` flags, and aligns scatter/run feature symmetry. It is a pure refactor — no new algorithms.

#### Design reference

**Terminal operators** — appended after the last `:::` stage in `run`. `-o` is only valid when a terminal operator is present. Without a terminal operator, tool command must contain `{}`.

| Operator | Default scatter mode | Output order | Temp files |
|---|---|---|---|
| `+concat+` | Sequential | Genomic | Yes |
| `+merge+` | Interleaved (future) | Genomic | No |
| `+collect+` | Sequential | Arrival | No |
| none | Sequential | N/A | N/A (tool-managed via `{}`) |

**Scatter mode flags** — `-i`/`--interleave` and `-s`/`--sequential` on both `scatter` and `run`:

| Context | Default | Notes |
|---|---|---|
| `scatter` from indexed file | Sequential | `-i` allowed, warn: overlapping ranges |
| `scatter` from stdin (future) | Interleaved | `-s` allowed |
| `run +concat+` | Sequential | `-i` allowed, warn |
| `run +merge+` | Interleaved (future) | `-s` is an error |
| `run +collect+` | Sequential | `-i` allowed, warn |

**`-n`/`-j` consolidation** — retire `-j`/`--max-jobs`. `-n` controls both shard count and concurrent pipeline count. All N shards run concurrently.

**`-O` output format flag** (bcftools-style):

| Flag | Format | Compression |
|---|---|---|
| `-Ov` | VCF | Uncompressed |
| `-Oz` | VCF | BGZF |
| `-Ob` | BCF | BGZF |
| `-Ou` | BCF | Uncompressed |

Native (no bcftools): compress/decompress within same format. VCF↔BCF conversion delegated to bcftools if on PATH; if not found and conversion is required, exit 1 with a clear message suggesting an explicit `bcftools view` pipeline stage. Format inferred from `-o` extension when `-O` absent. `-O` overrides with a warning on extension mismatch. Stdout with no `-o` and no `-O`: uncompressed matching detected stream format.

**`-d`/`--decompress` flag** — on both `scatter` and `run`. In sequential mode, decompress BGZF blocks before piping to subprocess stdin rather than raw-copying. Eliminates boundary block recompression and subprocess decompression cost. In interleaved mode (future `+merge+`) decompression is already implicit — accept silently as a no-op.

#### Steps

- [ ] **I1** — Retire `-j`/`--max-jobs` from `run` in `main.nim`. Remove all references in `run.nim`. Update help text. Add an error if `-j` is passed: `"error: -j is no longer supported; use -n to control both shard count and concurrency"`. Run `nimble test` — update any test that used `-j`.

- [ ] **I2** — Implement terminal operator parsing in `main.nim`. Scan `run` argv for tokens exactly equal to `+concat+`, `+merge+`, or `+collect+`. Everything after the last `:::` / `---` / `+verb+` token is parsed accordingly. Validate: `-o` without a terminal operator is an error; no terminal operator and no `{}` in tool command is an error; `+merge+` with `-s` is an error. Unit test the parser with all valid and error cases — do not wire up any new output behaviour yet, just parse and validate.

- [ ] **I3** — Wire `+concat+` as the replacement for `--gather`. `+concat+` uses the existing gather/temp-file path in `gather.nim` unchanged. `--gather` is retired — add a clear error if passed: `"error: --gather is retired; use +concat+ instead"`. Update `test_run.nim` and `test_cli.nim` to use `+concat+` syntax. Run `nimble test`.

- [ ] **I4** — Implement `+collect+` streaming output in `run.nim`. Each shard's pipeline stdout is read in a dedicated thread. As complete records arrive (VCF: `\n`-terminated; BCF: `l_shared + l_indiv` bytes), they are immediately written to the output fd (stdout or `-o` file) under a mutex. No temp files. No ordering guarantee. Write `test_collect.nim` covering: single shard, 4 shards, BCF, stdout, all records present (order-insensitive check). Run `nimble test`.

- [ ] **I5** — Add `--concat` (default) and `--merge` flags to the `gather` subcommand in `main.nim`. `gather` subcommand keeps its name. `--merge` is a no-op for now — accepted and noted but falls back to `--concat` behaviour with a warning: `"warning: --merge not yet implemented, using --concat"`. Note in help text that `vcfparty gather` is similar to `bcftools concat -a`. Update `test_gather.nim` to cover `--concat` and `--merge` (warning) flags. Run `nimble test`.

- [ ] **I6** — Add `-i`/`--interleave` and `-s`/`--sequential` flags to both `scatter` and `run` in `main.nim`. For now, both flags are parsed and stored but only `-s` (sequential) actually changes behaviour — `-i` on an indexed file emits the overlapping-ranges warning and proceeds with sequential scatter (interleaved scatter is implemented in Milestone 3). Document this in help text. Run `nimble test`.

- [ ] **I7** — Add `-O` flag to `run` and `scatter` in `main.nim`. Implement compression-only cases natively in `gather.nim` (BGZF↔uncompressed within same format). For VCF↔BCF conversion: detect if bcftools is on PATH (`findExe("bcftools")`); if yes, insert `bcftools view -O<fmt>` as an implicit final stage after the terminal operator; if no, exit 1 with a clear message. Write unit tests for each `-O` case: same-format compress, same-format decompress, cross-format with bcftools mock, cross-format without bcftools (error). Run `nimble test`.

- [ ] **I8** — Add `-d`/`--decompress` flag to `run` and `scatter`. In sequential mode: decompress BGZF blocks before writing to subprocess stdin rather than raw-copying. In interleaved mode (future): accept silently as no-op. Update `scatter.nim` to support the decompress path alongside the existing raw-copy path. Write tests: `-d` with VCF produces valid uncompressed VCF shard files; `-d` with BCF produces valid uncompressed BCF; `-d` with `+collect+` all records present. Run `nimble test`.

- [ ] **I9** — Full integration test pass. Write `test_interface.nim` covering the complete new CLI surface: all terminal operators, `-O` flag, `-d` flag, `-i`/`-s` flags, `gather --concat`/`gather --merge` flags, retired `--gather` and `-j` errors. Run full `nimble test` — all suites green.

---

### Milestone 2: `+merge+` and `+collect+` (sequential)

Implements the merge sorter for `+merge+` using sequential (contiguous) scatter. This is limited — it will block on the slowest shard — but gets the merge sorter working and tested before interleaved scatter is added in Milestone 3.

#### Design

The merge sorter reads the current head record from each subprocess stdout stream, compares by `(contig_rank, pos)`, and emits the minimum. One record per stream held in a priority queue — O(1) memory per stream.

- **BCF**: read `chrom_id` (int32 LE at uncompressed byte offset 8 of each record), look up contig rank from the header contig list
- **VCF**: parse CHROM from first `\t`-delimited field, look up in contig table built from `##contig` header lines
- Contig table is extracted at scatter time from the input file header and passed to the merge sorter

Requires uncompressed pipeline output (`-Ou` or `-Ov`) from the last stage. If BGZF output is detected, decompress transparently with a warning: `"warning: +merge+ works best with uncompressed output (-Ou/-Ov) from the last pipeline stage"`.

#### Steps

- [ ] **M1** — Implement `extractContigTable(headerBytes: seq[byte]): seq[string]` in `gather.nim` — returns ordered contig names from VCF header `##contig` lines or BCF header blob. Unit test against `small.vcf.gz` and `small.bcf` headers. Run relevant test file.

- [ ] **M2** — Implement `readNextVcfRecord(fd: cint): seq[byte]` and `readNextBcfRecord(fd: cint): seq[byte]` in `gather.nim` — read exactly one complete record from an fd (VCF: read until `\n`; BCF: read 8-byte header, then `l_shared + l_indiv` bytes). Return empty seq on EOF. Unit test with synthetic byte sequences. Run test file.

- [ ] **M3** — Implement `extractSortKey(record: seq[byte], fmt: GatherFormat, contigTable: seq[string]): (int, int32)` in `gather.nim` — returns `(contig_rank, pos)` from a record. For BCF: read int32 at offset 0 (chrom_id) and int32 at offset 8 (pos). For VCF: split on `\t`, look up CHROM in contig table. Unit test on known records from `small.vcf.gz` and `small.bcf`. Run test file.

- [ ] **M4** — Implement `kWayMerge(fds: seq[cint], outFd: cint, fmt: GatherFormat, contigTable: seq[string])` in `gather.nim` — priority queue merge. Initialise by reading first record from each fd. Loop: pop minimum, write to outFd, read next from that fd. Handle EOF per stream. Unit test: merge 2 synthetic sorted VCF streams → verify output is sorted and contains all records. Run test file.

- [ ] **M5** — Wire `+merge+` into `run.nim`. After all N subprocess pipelines complete, call `kWayMerge` with each subprocess's stdout fd and the `-o` output fd. For sequential scatter: warn that merge may block on slowest shard (this is expected until interleaved scatter is implemented). Write `test_merge.nim`: 4 shards VCF, `+merge+`, output sorted and record-complete (sha256 vs sorted baseline); BCF equivalent; stdout output; BGZF pipeline output (triggers decompress warning). Run `nimble test`.

- [ ] **M6** — Implement `gather --merge` in the `gather` subcommand: wire `kWayMerge` from existing shard files rather than live fds. Read each shard file, open as fd, pass to `kWayMerge`. Remove the "not yet implemented" warning added in I5. Unit test: `vcfparty gather --merge` on 4 shard files → sorted output matches `bcftools concat -a` output. Run `nimble test`.

---

### Milestone 3: interleaved splitting

Implements round-robin block assignment as the default scatter strategy for `+merge+`. Eliminates stalling on the slowest shard. No recompression at chunk boundaries — decompressed bytes are piped directly.

#### Design

Chunk size K defaults to `ceil(total_blocks / (n * 10))` — giving ~10 interleaved chunks per subprocess. Tunable: `-i K` takes an optional integer argument.

Block assignment:
```
chunk 0 (blocks 0..K-1)     → subprocess 0
chunk 1 (blocks K..2K-1)    → subprocess 1
...
chunk n-1                   → subprocess n-1
chunk n                     → subprocess 0  (wraps)
```

No recompression: each chunk's BGZF blocks are decompressed and piped as raw bytes. The subprocess receives a continuous uncompressed stream. This is correct because `+merge+` requires uncompressed output from the subprocess anyway.

#### Steps

- [ ] **L1** — Implement `interleavedBlockAssignment(starts: seq[int64], n: int, K: int): seq[seq[int64]]` in `scatter.nim` — returns N sequences of block start offsets, one per subprocess, in round-robin order. Unit test: known block list, verify each block assigned exactly once and round-robin order is correct. Run test file.

- [ ] **L2** — Implement `writeInterleavedShard(path: string, blockOffsets: seq[int64], headerBytes: seq[byte], outFd: cint)` in `scatter.nim` — for each block in the shard's offset list: read raw BGZF block, decompress, write raw bytes to outFd. Prepend recompressed header to the first chunk only (subprocess 0's first chunk = shard 0's header, all others = header also since each subprocess gets a full header). No BGZF EOF block — subprocess receives a raw byte stream. Unit test: two-shard interleaved scatter, concatenate decompressed output, verify all records present. Run test file.

- [ ] **L3** — Wire interleaved scatter into `run.nim` for `+merge+`. When `+merge+` is the terminal operator and `-s` is not set, use `interleavedBlockAssignment` and `writeInterleavedShard` rather than the sequential scatter path. Each subprocess receives decompressed bytes. After all subprocesses complete, call `kWayMerge`. Write integration tests in `test_merge.nim`: interleaved `+merge+` on `small.vcf.gz` 8 shards, output sorted and record-complete; BCF equivalent; `-s` with `+merge+` errors cleanly; warn message for `-i` with `+concat+`. Run `nimble test`.

- [ ] **L4** — Implement chunk size tuning. Parse optional integer after `-i`: `-i` alone uses default K; `-i 20` sets K=20. Validate K ≥ 1. Add test for explicit K: interleaved scatter with K=5 produces same records as K=default. Run `nimble test`.

- [ ] **L5** — Implement interleaved scatter for `scatter` subcommand (not just `run`). When `-i` is passed to `scatter`, use `interleavedBlockAssignment` and write decompressed shard files (or BGZF if `-d` is not passed — wait, interleaved scatter always decompresses, so shard files are uncompressed raw VCF/BCF). Emit warning about overlapping ranges. Write tests: `scatter -i` on `small.vcf.gz`, verify shard files are uncompressed VCF, all records present across shards, overlapping warning emitted. Run `nimble test`.

- [ ] **L6** — Update `concat --merge` to handle interleaved shard files (uncompressed VCF/BCF rather than BGZF). Detect format from first bytes. Run `nimble test` — full suite green.

---

### Milestone 4: stdin splitting

Implements `--stdin` flag. Accepts non-seekable stream as input, splits on the fly using a BGZF decompressor thread pool, pipes shards to subprocesses.

#### Design

```
stdin
  |
[block reader thread] → raw BGZF blocks → [decompressor pool, N threads]
                                                    |
                                          decompressed ring buffer
                                                    |
                               [demux thread] → subprocess 0 stdin pipe
                                            |→ subprocess 1 stdin pipe
                                            |→ subprocess N stdin pipe
                                                    |
                                        [+merge+ or +collect+]
```

Ring buffer: bounded, blocking. Block reader fills; decompressor threads drain and write decompressed chunks; demux thread reads decompressed chunks and routes to the current shard's subprocess pipe. Shard switching happens at record boundaries (VCF: `\n`; BCF: record length bytes).

`+concat+` is not valid with `--stdin` (no ordering guarantee). Error if attempted.

Format auto-detected from first bytes: BGZF magic → decompress → check for `BCF\x02\x02` or `##fileformat`.

#### Steps

- [ ] **X1** — Implement `RingBuffer` in a new `src/vcfparty/stdin_split.nim`. Fixed-size bounded byte buffer with blocking `write` (blocks if full) and blocking `read` (blocks until data available). Unit test: producer thread writes, consumer thread reads, verify all bytes received in order. Run test file.

- [ ] **X2** — Implement block reader thread: reads raw BGZF blocks from stdin fd into the ring buffer. Handles EOF (closes buffer). Unit test: pipe a known BGZF file through the reader, verify block boundaries are preserved. Run test file.

- [ ] **X3** — Implement decompressor thread pool: N threads each pop a raw BGZF block from the ring buffer, decompress, and write decompressed bytes to a second ring buffer. Thread count configurable. Unit test: decompress known BGZF file via pool, verify decompressed output matches `bgzip -d`. Run test file.

- [ ] **X4** — Implement demux thread: reads decompressed bytes from the second ring buffer, scans for record boundaries (VCF: `\n`; BCF: read length prefix), routes complete records to the current subprocess's stdin pipe. Switches subprocess after every K records (K = total_estimated_records / n, re-estimated from block count). Write header to each subprocess's pipe before any records. Implement backpressure: if current subprocess pipe is full, try next available subprocess (non-blocking write with fallback). Unit test: synthetic decompressed VCF byte stream, verify correct record routing to N output fds. Run test file.

- [ ] **X5** — Wire `--stdin` into `run.nim`: detect `--stdin` flag, call `stdin_split.nim` orchestration instead of `scatter.nim`. Validate: `+concat+` with `--stdin` is an error. Integrate with `+merge+` and `+collect+`. Write `test_stdin.nim`: pipe `small.vcf.gz` through `vcfparty run --stdin -n 4 ::: cat +collect+`, verify all records present; pipe through `+merge+`, verify sorted output; BCF equivalent; `+concat+` error; backpressure test (slow subprocess with fast input). Run `nimble test` — full suite green.

- [ ] **X6** — Implement BCF stdin splitting in the demux thread: walk `l_shared + l_indiv` byte boundaries rather than scanning for `\n`. Unit test: pipe `small.bcf` through `--stdin`, verify all records present. Run `nimble test`.

- [ ] **X7** — Implement uncompressed VCF stdin: if no BGZF magic detected, skip block reader and decompressor pool entirely, read raw bytes from stdin and scan for `\n` boundaries in the demux thread. Unit test: pipe uncompressed VCF through `--stdin`, verify correct output. Run `nimble test` — full suite green.

---

## Workflow rules for Claude Code

### Before starting any task

1. Re-read this file in full
2. Read the relevant source files
3. Consult `example/scatter_vcf.py` for scatter behaviour questions
4. Check which milestone step is next — do not skip ahead

### Hard rules

| Rule | Detail |
|---|---|
| **No new dependencies** | Do not add to `vcfparty.nimble` without asking |
| **Test before done** | Run `nimble test` and show full output before declaring any step complete |
| **No commits** | Stage changes, propose commit message, wait for user |
| **No layout changes** | Do not restructure modules without asking |
| **One proc at a time** | Implement, test, then proceed — never write 200+ lines without a test checkpoint |
| **Ask when uncertain** | Behaviour not covered here → stop and ask |

---

## Key constants

### BGZF EOF block

```nim
const BGZF_EOF* = [
  0x1f'u8, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
  0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
  0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00
]
```

### BCF magic

```nim
const BCF_MAGIC* = [byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8]
```

---

## Build reference

```bash
nimble build
nimble build -d:release
nimble test
nim c -d:debug -r tests/test_merge.nim   # example single file
```

---

## Performance testing

Not run by default. Requires `--perf` fixture:

```bash
bash tests/generate_fixtures.sh --perf
# → tests/data/chr22_1kg_full.vcf.gz (50k records, 2504 samples)
# → tests/data/chr22_1kg_full.bcf

# Benchmark
time vcfparty run -n 8 -o out.vcf.gz tests/data/chr22_1kg_full.vcf.gz \
  ::: bcftools view -Oz +concat+

# Baseline
time bcftools view -Oz -o out_baseline.vcf.gz tests/data/chr22_1kg_full.vcf.gz

# Correctness
bcftools view -H out.vcf.gz | sha256sum
bcftools view -H out_baseline.vcf.gz | sha256sum

# Profiling
nim c -d:release -g src/vcfparty.nim
perf record -g ./vcfparty run -n 8 tests/data/chr22_1kg_full.vcf.gz \
  ::: bcftools view -Oz +concat+
perf report
```

---

## Out of scope (do not implement)

- Windows support
- bcftools as a hard build or test dependency (soft runtime dependency for `-O` conversion only)
- Tools that do not support stdin/stdout