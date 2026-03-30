# CLAUDE.md — paravar

Agentic development instructions for Claude Code. Read this file in full before starting any task.

---

## Project overview

**paravar** is a Nim tool for parallel processing of VCF files, built on top of [hts-nim](https://github.com/brentp/hts-nim).

Subcommands (only `scatter` is in scope now):
- `scatter` — split a bgzipped VCF into N roughly equal shards without decompressing middle BGZF blocks
- `gather` — merge shards into a sorted VCF (future)
- `index` — parallelised tabix/CSI indexing (future)

The canonical reference for `scatter` behaviour is `example/scatter_vcf.py`. When in doubt about edge cases, consult that file.

---

## Repository layout

```
paravar/
├── CLAUDE.md                  # this file
├── paravar.nimble
├── src/
│   └── paravar/
│       ├── main.nim           # CLI entry point, subcommand dispatch
│       ├── scatter.nim        # scatter subcommand implementation
│       └── bgzf_utils.nim     # low-level BGZF block parsing (no hts-nim dependency)
├── tests/
│   ├── data/                  # generated test fixtures (not committed)
│   ├── generate_fixtures.sh   # script to create tests/data/ fixtures
│   ├── test_bgzf_utils.nim
│   ├── test_scatter.nim
│   └── test_cli.nim
├── example/
│   └── scatter_vcf.py         # Python reference implementation
└── README.md
```

Do not restructure the module layout without asking the user.

---

## Tech stack

- **Language**: Nim (user is a Nim novice — see style guidance below)
- **VCF/BGZF library**: [hts-nim](https://github.com/brentp/hts-nim) — use for VCF header parsing, variant iteration, and index reading
- **Test framework**: [testament](https://nim-lang.org/docs/testament.html) — Nim's native test runner
- **Platform**: Linux; htslib available system-wide or via conda
- **Build**: `nimble build` / `nimble test`

---

## Nim style guide (read carefully — user is new to Nim)

These rules exist so the code is readable to someone coming from Python/R/C.

- **Prefer explicit types** on proc signatures — never rely on type inference for proc parameters.
- **Named result variables** over `return` in non-trivial procs: use `result =` and let the proc return implicitly.
- **Error handling**: use `doAssert` in tests; use `quit(msg, 1)` or raise a typed exception in production code. Never use bare `assert` in production.
- **No `var` unless mutation is required**. Prefer `let` for bindings.
- **Avoid templates and macros** unless there is no cleaner alternative — they hurt readability for Nim novices.
- **Comment all procs** with a doc comment (`## description`) explaining purpose, not implementation.
- **Keep procs short** — if a proc exceeds ~50 lines, split it.
- **Prefer `openArray[byte]`** over raw pointer arithmetic when possible.
- For file I/O, prefer Nim's standard `streams` or `system` I/O over manual `fread`-style unless raw speed is critical (which it is for BGZF block copying — see below).

---

## Scatter subcommand — design specification

### CLI

```
paravar scatter -n <n_shards> -o <prefix> input.vcf.gz
```

Outputs: `<prefix>.01.vcf.gz`, `<prefix>.02.vcf.gz`, ... (zero-padded to width of `n_shards`).

Each shard is a valid bgzipped VCF (with header + terminating BGZF EOF block).

### Algorithm (must match `example/scatter_vcf.py`)

#### Phase 1 — read index for coarse block starts

1. Detect `.tbi` or `.csi` index alongside the input. Abort with a clear error if neither exists.
2. Parse the binary index to extract BGZF virtual offsets; shift right by 16 to get file offsets of BGZF blocks that contain indexed records.
3. This gives a coarse set of block starts — only blocks that index entries point to, not every block in the file.

#### Phase 2 — extract header + first-block offset

1. Open the VCF via hts-nim and read all `#` header lines.
2. Record the BGZF file offset of the first non-header record (virtual offset >> 16).
3. Compress the header text into a fresh BGZF block — this becomes the `header_bytes` prepended to every shard.

#### Phase 3 — optimise shard boundaries

1. Compute cumulative byte lengths of blocks; use weighted bisection to pick `n_shards - 1` split points that produce roughly equal total sizes.
2. For each candidate split block, scan the raw file bytes to enumerate every BGZF sub-block within that block (resolving finer offsets than the index provides).
3. Validate each split block: decompress it and confirm it contains at least one complete record terminated by `\n` before the next block starts. If not, mark it invalid and retry with exclusions (up to 1000 iterations).

#### Phase 4 — write shards

For each shard `i`:
- **Prepend**: `header_bytes` (for shard 0, also remove any header lines from the first data block if it overlaps with the header region).
- **Middle blocks**: raw byte-copy from file — no decompression, no recompression.
- **Boundary block** (the block straddling the shard split):
  - Decompress the block.
  - Split on record lines (`\n`): first half goes to shard `i` as its tail; second half goes to shard `i+1` as its prepend (after the header).
  - Recompress each half as a BGZF block.
- **Append**: BGZF EOF block (`\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00BC\x02\x00\x1b\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00`).

### Module split

| Module | Responsibility |
|---|---|
| `bgzf_utils.nim` | Parse BGZF block headers, scan block starts, raw byte copy, decompress/recompress boundary blocks. No hts-nim dependency — pure file I/O + zlib. |
| `scatter.nim` | Parse TBI/CSI index, orchestrate phases 1–4, write output files. Uses hts-nim for VCF header and index parsing where available; falls back to `bgzf_utils` for raw block work. |
| `main.nim` | CLI arg parsing (`parseopt` or `cligen`), subcommand dispatch. |

---

## Testing specification

### Test fixture generation

Before running tests, `tests/generate_fixtures.sh` must be run once. It uses `bcftools` and `bgzip`/`tabix` to create:

- `tests/data/small.vcf.gz` — ~200 records across 2–3 chromosomes, bgzipped, tabix indexed
- `tests/data/single_chrom.vcf.gz` — single chromosome, for edge-case testing
- `tests/data/tiny.vcf.gz` — fewer records than n_shards (for error-path tests)

The script must be idempotent (skip if files exist). Do not commit generated fixtures.

### Test files and what they cover

**`tests/test_bgzf_utils.nim`**
- `scanBgzfBlockStarts`: given a known bgzipped file, returns the correct file offsets
- `rawCopyBytes`: byte-copies a range, result is identical to the source slice
- `compressToBgzf` / `decompressBgzf`: round-trip identity
- Boundary block split: splitting a block on its midpoint line produces two valid BGZF blocks that concatenate back to the original

**`tests/test_scatter.nim`**
- Scatter into 1 shard: output equals input (after normalising the header block)
- Scatter into N shards: each shard is a valid bgzipped VCF (check magic bytes + EOF block)
- Scatter into N shards: all records present, no duplicates (parse and collect all CHROM:POS:REF:ALT from all shards, compare to original)
- Scatter into N shards: records within each shard are in correct genomic order
- Shard sizes are roughly equal (largest / smallest < 2.0 for a balanced input)
- `n_shards` > number of BGZF blocks: exits with a clear error message and non-zero exit code

**`tests/test_cli.nim`**
- Missing index file: exits non-zero with message containing "index"
- Missing `-n`: exits non-zero
- Invalid `-n 0`: exits non-zero

### Running tests

```bash
# Generate fixtures (once, or after deleting tests/data/)
bash tests/generate_fixtures.sh

# Run all tests
nimble test

# Run a single test file
nim c -r tests/test_bgzf_utils.nim
```

---

## Workflow rules for Claude Code

These are hard rules. Do not deviate without asking the user.

### Before starting any task

1. Re-read this file.
2. Read the relevant source file(s) in full.
3. Consult `example/scatter_vcf.py` if the task touches scatter logic.

### Dependency rule

**Do not add new nimble dependencies** (to `paravar.nimble`) without asking the user first. The only allowed dependencies to start are:
- `hts` (hts-nim)
- `cligen` (CLI parsing — ask before adding)

For zlib/BGZF decompression: use htslib's bundled `bgzf.h` via hts-nim's FFI, or Nim's `zlib` wrapper. Do not pull in a third-party pure-Nim zlib.

### Testing rule

**Always run `nimble test` (or the single relevant test file) before declaring any implementation step done.** Report the full test output. Do not summarise as "tests pass" without showing output.

### Commit rule

**Do not run `git commit`**. Stage changes and show a proposed commit message, then wait for the user.

### Error handling rule

All user-facing errors must:
1. Print to stderr
2. Include the offending file path or value
3. Exit with code 1

Never call `quit()` or `raise` silently.

### Code generation rule

Write code in small, testable units. Implement one proc at a time, run its unit test, then proceed. Do not write 200-line files in a single pass.

### When stuck

If you cannot determine the correct behaviour from this file or the Python reference, stop and ask the user. Do not guess on index format parsing, BGZF structure, or output file format.

---

## Build cheatsheet (for Claude's reference)

```bash
# Install hts-nim
nimble install hts -y

# Build debug
nimble build

# Build release
nimble build -d:release

# Run tests
nimble test

# Compile a single file (fast iteration)
nim c -d:debug -r tests/test_bgzf_utils.nim
```

### hts-nim VCF snippet (reference)

```nim
import hts

var vcf: VCF
doAssert open(vcf, "input.vcf.gz", index = true)
echo vcf.header.hdr  # raw htslib header pointer

for rec in vcf:
  echo rec.CHROM, "\t", rec.POS

# Write header to new VCF
var wtr: VCF
doAssert open(wtr, "out.vcf.gz", mode = "wz")
wtr.header = vcf.header
doAssert wtr.write_header()
```

### BGZF EOF block constant

Every output shard must end with this exact 28-byte sequence:

```nim
const BGZF_EOF* = [
  0x1f'u8, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
  0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
  0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00
]
```

### TBI / CSI parsing

hts-nim exposes `tbx_t` via `hts/tbx`. Prefer using the hts-nim API to open and iterate the index rather than parsing binary manually. Fall back to manual parsing (mirroring the Python reference) only if the API doesn't expose block offsets.

---

## Step-by-step plan

Execute these steps in order. Do not jump ahead. Check off each step (update this file) as it is completed.

- [x] **Step 0**: Scaffold — create `paravar.nimble`, directory layout, stub `main.nim`
- [x] **Step 1**: Write `tests/generate_fixtures.sh` and verify it produces valid indexed VCFs
- [x] **Step 2**: Implement and test `bgzf_utils.nim` — block scanning, raw copy, compress/decompress, boundary split
- [x] **Step 3**: Implement and test TBI/CSI index parsing in `scatter.nim`
- [x] **Step 4**: Implement and test shard boundary optimisation logic
- [ ] **Step 5**: Implement and test shard writing (prepend header + raw copy + boundary blocks + EOF)
- [ ] **Step 6**: Wire up `main.nim` CLI and run `test_cli.nim`
- [ ] **Step 7**: End-to-end validation: scatter `small.vcf.gz` into 4 shards, verify record completeness and order

---

## Out of scope (do not implement)

- `gather` subcommand
- `index` subcommand
- BCF input/output (design with it in mind, but do not implement)
- Multithreaded shard writing (the Python prototype supports `--threads`; defer this)
- `--chunk` / `--stdout` flags from the Python prototype (defer)
