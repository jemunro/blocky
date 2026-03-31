# CLAUDE.md — paravar

Agentic development instructions for Claude Code. Read this file in full before starting any task.

---

## Project overview

**paravar** is a Nim CLI tool for parallelising VCF/BCF processing by exploiting BGZF block boundaries and tabix/CSI indexes.

### Subcommands

| Subcommand | Status | Description |
|---|---|---|
| `scatter` | ✅ Implemented | Split bgzipped VCF or BCF into N roughly equal shards |
| `run` | ✅ Implemented | Scatter → parallel per-shard tool pipelines → per-shard output files |
| `gather` | 🔲 Planned | Merge shards into a single sorted VCF/BCF (see note below) |
| `index` | 🔲 Deferred | Parallelised tabix/CSI indexing |

**gather note**: not yet implemented. When it is, it will behave like `bcftools concat -a`, accepting the per-shard outputs of `paravar run` and producing a single sorted output file. Format (VCF or BCF) will be inferred from the input shard extension.

The canonical reference for `scatter` behaviour is `example/scatter_vcf.py`.

---

## Repository layout

```
paravar/
├── CLAUDE.md
├── paravar.nimble
├── src/
│   ├── paravar.nim                  # entry point (include paravar/main)
│   └── paravar/
│       ├── main.nim                 # CLI dispatch
│       ├── scatter.nim              # scatter algorithm (VCF + BCF)
│       ├── bgzf_utils.nim           # low-level BGZF I/O (no external deps beyond -lz)
│       └── run.nim                  # run subcommand
├── tests/
│   ├── data/                        # generated fixtures (not committed)
│   ├── generate_fixtures.sh         # creates VCF and BCF fixtures
│   ├── test_bgzf_utils.nim          # ✅
│   ├── test_scatter.nim             # ✅ (VCF); BCF cases to be added
│   ├── test_cli.nim                 # ✅ (VCF); BCF cases to be added
│   └── test_run.nim                 # ✅ (VCF); BCF cases to be added
└── example/
    └── scatter_vcf.py               # Python reference implementation (VCF only)
```

Do not restructure the module layout without asking the user.

---

## Dependencies

| Dependency | Use |
|---|---|
| zlib (`-lz`) | BGZF compress/decompress in `bgzf_utils.nim` |

No nimble package dependencies. Do not add any without asking the user. Avoid hts-nim — BCF header and record parsing is done directly from the spec using raw file I/O and zlib.

---

## Tech stack and style

- **Language**: Nim (user is a Nim beginner — coming from Python/R/C)
- **Test framework**: testament
- **Platform**: Linux; zlib available system-wide or via conda

### Nim style rules

- Explicit types on all proc signatures — no implicit parameter type inference
- `let` by default; `var` only when mutation is required
- Named `result =` over `return` in non-trivial procs
- User-facing errors: print to stderr, include offending path/value, `quit(1)`
- Doc comment (`## ...`) on every exported proc
- No templates or macros unless unavoidable
- Procs > ~50 lines should be split

---

## Format detection

Format (VCF vs BCF) is detected automatically from the input file extension:

| Extension | Format |
|---|---|
| `.vcf.gz` | bgzipped VCF |
| `.bcf` | bgzipped BCF |

Any other extension: exit 1 with a clear error message. Do not attempt content sniffing — extension is sufficient.

Output shards use the same extension as the input. VCF input → `.vcf.gz` shards. BCF input → `.bcf` shards.

---

## BCF format reference (no hts-nim needed)

### File structure

A BCF file is BGZF-compressed. After decompressing the first block(s), the uncompressed layout is:

```
[5 bytes]  magic: "BCF\x02\x02"
[4 bytes]  l_text: uint32_t (little-endian) — byte length of header text including NUL terminator
[l_text]   header text: VCF header lines as UTF-8 text, NUL-terminated
            (begins with "##fileformat=VCFv..." and ends with "#CHROM\t...\n\0")
[records]  binary BCF records (see below)
```

### BCF record structure

Each record is:

```
[4 bytes]  l_shared: uint32_t (little-endian)
[4 bytes]  l_indiv:  uint32_t (little-endian)
[l_shared bytes]  shared site data (CHROM, POS, ID, REF, ALT, QUAL, FILTER, INFO)
[l_indiv bytes]   per-sample genotype data (FORMAT fields)
```

Total record size: `8 + l_shared + l_indiv` bytes.

Records are packed sequentially within BGZF blocks. A single BGZF block may contain multiple records, and a record may span a BGZF block boundary (though this is rare in practice).

### Header extraction (BCF)

To extract the header bytes to prepend to each shard:

1. Decompress BGZF blocks from the start of the file until you have accumulated at least `5 + 4 + l_text` uncompressed bytes.
2. Verify magic bytes `BCF\x02\x02` — exit 1 if wrong.
3. Read `l_text` as little-endian uint32 at offset 5.
4. Collect `5 + 4 + l_text` bytes total — this is the exact BCF header blob.
5. Recompress with `compressToBgzfMulti` (same as VCF — handles headers > 65536 bytes).

The recompressed header blob is prepended to every shard, identical to the VCF path.

### Boundary splitting (BCF)

For VCF, a boundary block is decompressed and split on `\n` at the record midpoint.

For BCF, the equivalent is:

1. Decompress the boundary block to get a byte sequence.
2. Walk the byte sequence using the record length formula: advance `8 + l_shared + l_indiv` bytes per record, reading `l_shared` and `l_indiv` as little-endian uint32 at the current position.
3. Find the record index closest to the midpoint (by byte count).
4. Split the byte sequence at that record boundary — not mid-record.
5. Recompress each half as a BGZF block (using `compressToBgzf`).

This replaces the `splitOnNewline` call in the VCF path. The surrounding raw-copy and prepend/append logic is identical.

**Edge case**: if the decompressed block contains zero complete records (i.e. a single record is larger than one BGZF block — very rare but possible for VCFs with many samples), mark this boundary as invalid and exclude it, same as the VCF path does for blocks with no complete line.

### First data block offset (BCF)

For VCF, the first data block offset is found by scanning for the first block that contains a non-`#` line.

For BCF, the first data block offset is found by:

1. Reading `l_text` from the header.
2. Computing the byte offset of the first record: `5 + 4 + l_text` bytes into the uncompressed stream.
3. Translating that uncompressed offset back to a BGZF file offset by scanning blocks until the cumulative uncompressed size crosses `5 + 4 + l_text`.

### No-header detection for shard 0 (BCF)

For VCF, shard 0's pre-header data blocks are stripped of `#` lines via `removeHeaderLines`.

For BCF, the equivalent is: skip all bytes before `5 + 4 + l_text` in the uncompressed stream of the first data region. In practice this means the first data block is handled identically to VCF — recompress only the bytes from the first record onward.

---

## Module responsibilities (updated for BCF)

| Module | Responsibility |
|---|---|
| `bgzf_utils.nim` | BGZF block scanning, raw copy, compress/decompress. Add: `splitBcfBoundaryBlock` (BCF-aware boundary split), `bcfFirstDataOffset` (compute uncompressed offset of first BCF record). |
| `scatter.nim` | Add: `extractBcfHeader` (read magic + l_text + header text, recompress). Dispatch to VCF or BCF path based on detected format. `computeShards` and `doWriteShard` remain format-agnostic (they operate on byte ranges and fds). |
| `main.nim` | Pass detected format enum (`Vcf` / `Bcf`) down to scatter. No other changes. |
| `run.nim` | No changes — format-agnostic. |

---

## Test fixtures for BCF

`tests/generate_fixtures.sh` must be extended to produce:

| File | Description |
|---|---|
| `tests/data/small.bcf` | Convert `small.vcf.gz` to BCF via `bcftools view -Ob`, CSI index with `bcftools index` |
| `tests/data/chr22_1kg.bcf` | Convert `chr22_1kg.vcf.gz` to BCF (large header: 2504 samples) |

BCF files always use CSI (not TBI). The existing CSI parsing path should handle this — verify in tests.

---

## Testing specification for BCF

### New tests in `test_bgzf_utils.nim`

| Test | What it checks |
|---|---|
| `splitBcfBoundaryBlock` round-trip | Split a known BCF block, concatenate halves, bytes equal original |
| `splitBcfBoundaryBlock` midpoint | Split lands on a record boundary (no mid-record splits) |
| `bcfFirstDataOffset` | Returns correct byte offset for `small.bcf` (verify against `bcftools view` record 1 position) |
| Zero-record block | Block containing a single oversized record is correctly flagged as invalid boundary |

### New tests in `test_scatter.nim`

| Test | What it checks |
|---|---|
| BCF header extraction | `extractBcfHeader` returns bytes starting with `BCF\x02\x02` + correct `l_text` |
| BCF scatter, 1 shard | Output equals input (magic, l_text, record count) |
| BCF scatter, 4 shards | All records present, no duplicates, correct order |
| BCF scatter, large header | `chr22_1kg.bcf` 4 shards — header > 65536 bytes handled correctly |
| BCF shard validity | Each shard opens cleanly with `bcftools view -H` (exit 0) |
| BCF size balance | Largest / smallest shard < 2.0 for balanced input |

### New tests in `test_cli.nim`

| Test | What it checks |
|---|---|
| `.bcf` extension → BCF mode | No error, shards have `.bcf` extension |
| `.vcf.gz` extension → VCF mode | Existing behaviour unchanged |
| Unknown extension | Exits 1 with message containing the extension |
| BCF no index | Exits 1 (BCF requires CSI; no auto-scan fallback for BCF — see below) |

### New tests in `test_run.nim`

| Test | What it checks |
|---|---|
| BCF run, 4 shards | `--- bcftools view -Ob` passthrough; 4 `.bcf` outputs, all records present |

---

## BCF-specific design decisions

### No auto-scan fallback for BCF

For VCF, if no index is found, `scatter` falls back to scanning all BGZF blocks and warns the user. This works because VCF block boundaries are always valid split points (you can always find a `\n`).

For BCF, **do not implement an auto-scan fallback**. Without a CSI index, there is no way to know which BGZF blocks contain record boundaries vs. which are mid-record continuations. Require a CSI index for BCF; exit 1 with a clear error if not found.

### `--force-scan` for BCF

`--force-scan` is not meaningful for BCF (same reason as above). If passed with a BCF input, exit 1 with `"paravar: --force-scan is not supported for BCF input"`.

---

## Step-by-step plan

Execute in order. Do not skip ahead. Update checkboxes as steps complete.

### Scatter + run (complete)
- [x] All scatter steps (see git history)
- [x] R1–R5: run subcommand

### BCF support (next)
- [x] **Step B1**: Extend `tests/generate_fixtures.sh` to produce `small.bcf` and `chr22_1kg.bcf`. Verify with `bcftools view -H tests/data/small.bcf` and `bcftools index` output. Run `nimble test` — existing tests must pass.
- [x] **Step B2**: Implement `bcfFirstDataOffset` and `splitBcfBoundaryBlock` in `bgzf_utils.nim`. Write the `test_bgzf_utils.nim` BCF tests. Run the test file — all must pass.
- [x] **Step B3**: Implement `extractBcfHeader` in `scatter.nim`. Add format detection (`.vcf.gz` vs `.bcf`) to `main.nim`. Wire format enum into scatter dispatch. Write BCF header extraction tests in `test_scatter.nim`. Run — all must pass.
- [x] **Step B4**: Implement BCF scatter path end-to-end: connect `bcfFirstDataOffset`, `extractBcfHeader`, `splitBcfBoundaryBlock` into the existing scatter phases. BCF uses CSI only; enforce this with a clear error. Write full BCF scatter correctness tests. Run `nimble test` — all tests (VCF + BCF) must pass.
- [x] **Step B5**: Add BCF CLI tests to `test_cli.nim` and BCF run test to `test_run.nim`. Run `nimble test` — full suite green.

### Deferred
- [ ] `gather` subcommand
- [ ] `run` with pre-scattered input glob
- [ ] `index` subcommand

---

## Workflow rules for Claude Code

### Before starting any task

1. Re-read this file in full
2. Read the relevant source files
3. Consult `example/scatter_vcf.py` for scatter behaviour questions (VCF reference only)

### Hard rules

| Rule | Detail |
|---|---|
| **No new dependencies** | Do not add to `paravar.nimble` without asking — specifically, do not add hts-nim |
| **Test before done** | Run `nimble test` (or relevant test file) and show full output before declaring a step complete |
| **No commits** | Stage changes, propose a commit message, wait for user |
| **No layout changes** | Do not restructure modules without asking |
| **Small units** | Implement one proc at a time, test it, then proceed |
| **Ask when uncertain** | If BCF spec behaviour is ambiguous and not covered here, stop and ask — do not guess |

---

## Build reference

```bash
nimble build                              # debug
nimble build -d:release                   # release
nimble test                               # all tests
nim c -d:debug -r tests/test_bgzf_utils.nim
nim c -d:debug -r tests/test_scatter.nim
nim c -d:debug -r tests/test_cli.nim
nim c -d:debug -r tests/test_run.nim
```

### BGZF EOF block constant

Every output shard (VCF and BCF) must end with this 28-byte sequence:

```nim
const BGZF_EOF* = [
  0x1f'u8, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
  0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
  0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00
]
```

### BCF magic constant

```nim
const BCF_MAGIC* = [byte('B'), byte('C'), byte('F'), 0x02'u8, 0x02'u8]
```