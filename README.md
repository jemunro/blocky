# paravar

Parallelise VCF/BCF processing by splitting files along BGZF block boundaries and piping each shard through a tool pipeline concurrently.

---

## How it works

A bgzipped VCF or BCF is a sequence of independent BGZF blocks, each containing up to 64 KiB of uncompressed data. Because blocks are self-contained, any block boundary is a valid split point — the file can be divided into shards without decompressing the data in between.

paravar uses the tabix (TBI) or CSI index to obtain coarse block offsets, then refines those to exact BGZF block boundaries within a small byte window around each split point. Each shard receives a recompressed header and a recompressed boundary block; the blocks in between are byte-copied directly from disk without decompression.

Only one block per split point is ever decompressed and recompressed. For a file split into N shards, that is N−1 boundary blocks regardless of file size.

---

## Installation

```bash
# Requires Nim >= 2.0 and cmake (used once to build the vendored compression library)
git clone https://github.com/jemunro/paravar paravar
cd paravar
nimble release
```

The first build downloads and compiles the compression library automatically. Subsequent builds skip that step. `bcftools` (or any other tool) is a runtime dependency for pipeline use but is not needed at build time.

For offline or HPC use, place `vendor/libdeflate-1.25.tar.gz` in the repo before running `nimble release` to skip the download.

---

## Quick start

```bash
# Split a VCF into 8 shards
paravar scatter -n 8 -o output.vcf.gz input.vcf.gz

# Filter in parallel, keep per-shard outputs
paravar run -n 8 -o filtered.vcf.gz input.vcf.gz \
  ::: bcftools view -i "GT='alt'" -Oz

# Filter and gather into a single file
paravar run --gather -n 8 -o filtered.vcf.gz input.vcf.gz \
  ::: bcftools view -i "GT='alt'" -Oz
```

---

## Subcommands

### `scatter`

Split input into N shards. Each shard is a valid standalone VCF/BCF file.

```
paravar scatter -n <n> -o <o> [options] <input>
```

| Flag | Long form | Description |
|---|---|---|
| `-n` | `--n-shards` | Number of shards (required, ≥ 1) |
| `-o` | `--output` | Output path or suffix. Required. |
| `-t` | `--max-threads` | Max threads for scatter/validation (default: min(n, 8)) |
| | `--force-scan` | Ignore index, scan all BGZF blocks — VCF only; exits 1 for BCF |
| `-v` | `--verbose` | Print progress to stderr |
| `-h` | `--help` | Show usage |

```bash
# Scatter into 4 shards using the {} placeholder
paravar scatter -n 4 -o output.{}.vcf.gz input.vcf.gz
# → output.1.vcf.gz  output.2.vcf.gz  output.3.vcf.gz  output.4.vcf.gz

# BCF requires a CSI index
paravar scatter -n 4 -o output.bcf input.bcf
```

BCF input requires a `.csi` index alongside the file (`bcftools index input.bcf`). VCF input uses a TBI or CSI index if present; if no index is found, all BGZF blocks are scanned directly and a warning is printed. Use `--force-scan` to force this path explicitly.

---

### `run`

Scatter and pipe each shard through a tool pipeline in parallel. Without `--gather`, writes N per-shard output files.

```
paravar run -n <n> -o <o> [options] <input> ::: <cmd> [::: <cmd> ...]
```

| Flag | Long form | Description |
|---|---|---|
| `-n` | `--n-shards` | Number of shards (required, ≥ 1) |
| `-o` | `--output` | Output path or suffix. Required. |
| `-j` | `--max-jobs` | Max concurrent shard pipelines (default: n-shards) |
| `-t` | `--max-threads` | Max threads for scatter/validation (default: min(max-jobs, 8)) |
| | `--force-scan` | Ignore index, scan all BGZF blocks — VCF only; exits 1 for BCF |
| | `--no-kill` | On failure, let sibling shards finish (default: kill siblings) |
| `-v` | `--verbose` | Print per-shard progress to stderr |
| `-h` | `--help` | Show usage |

```bash
# 8 shards, at most 4 concurrent pipelines
paravar run -n 8 -j 4 -o filtered.vcf.gz input.vcf.gz \
  ::: bcftools view -i "GT='alt'" -Oz
```

---

### `run --gather`

As `run`, but intercept each shard's stdout, strip duplicate headers from shards 2..N, and concatenate all outputs into a single file.

```
paravar run --gather [-o <o>] -n <n> [options] <input> ::: <cmd> [::: <cmd> ...]
```

In addition to the `run` flags:

| Flag | Long form | Description |
|---|---|---|
| | `--gather` | Bare flag. Gather all shard outputs into single `-o` file |
| | `--tmp-dir <dir>` | Temp dir for gather (default: `$TMPDIR/paravar` or `/tmp/paravar`) |
| | `--header-pattern <pat>` | Strip lines with this prefix from shards 2..N — **text format only**; error if specified with VCF or BCF |
| | `--header-n <n>` | Strip first N lines from shards 2..N — **text format only**; error if specified with VCF or BCF |

`-o` is optional. If omitted or set to `/dev/stdout`, output is written to stdout uncompressed. Temp shard files are deleted on success; on failure they are left on disk and their paths are printed to stderr.

```bash
# Gather into a single BCF
paravar run --gather -n 8 -o filtered.bcf input.bcf \
  ::: bcftools view -i "GT='alt'" -Ob

# Write to stdout
paravar run --gather -n 8 input.vcf.gz \
  ::: bcftools view -i "GT='alt'" -Oz | bcftools stats
```

---

### `gather`

Concatenate pre-existing shard files (output of `scatter` or `run`) into a single file. No temp files — reads directly from input files.

```
paravar gather [-o <o>] [options] <shard1> [<shard2> ...]
```

| Flag | Long form | Description |
|---|---|---|
| `-o` | `--output` | Output path (optional; omit for stdout) |
| | `--header-pattern <pat>` | Strip lines with this prefix from shards 2..N — **text format only**; error if specified with VCF or BCF |
| | `--header-n <n>` | Strip first N lines from shards 2..N — **text format only**; error if specified with VCF or BCF |
| `-v` | `--verbose` | Print progress to stderr |
| `-h` | `--help` | Show usage |

`--header-pattern` and `--header-n` are mutually exclusive.

```bash
paravar gather -o merged.vcf.gz shard_*.vcf.gz

# Stdout (pipe to bcftools)
paravar gather shard_*.vcf.gz | bcftools stats > stats.txt
```

---

## Output file naming

### Without `{}`

The shard number is prepended as `shard_XX.` to the filename component. Zero-padding width is determined by the number of shards:

```
-o output.vcf.gz, -n 8   →  shard_01.output.vcf.gz ... shard_08.output.vcf.gz
-o out.bcf, -n 4          →  shard_1.out.bcf ... shard_4.out.bcf
-o results.txt, -n 100    →  shard_001.results.txt ... shard_100.results.txt
```

### With `{}`

`{}` is replaced with the zero-padded shard number. It may appear anywhere in the path, including in a directory component:

```
-o output.{}.vcf.gz       →  output.01.vcf.gz ... output.08.vcf.gz
-o /results/{}/batch.bcf  →  /results/01/batch.bcf ... /results/08/batch.bcf
```

Parent directories are created automatically. With `--gather`, `-o` is the final output path — no shard numbering is applied to it.

---

## Pipeline separator

`:::` separates pipeline stages, which are joined with `|` and executed via `sh -c`. Multiple `:::` blocks define a multi-stage pipeline. `---` (three dashes) is also accepted as an alternative to `:::`.

Both are chosen to avoid collision with tools (such as bcftools plugins) that use `--` as their own argument separator.

```bash
# Multi-stage pipeline
paravar run -n 8 -o out.vcf.gz input.vcf.gz \
  ::: bcftools view -i "GT='alt'" -Ou \
  ::: bcftools view -s Sample -Oz
```

---

## Supported formats

| Format | Input | Output | Index required |
|--------|-------|--------|----------------|
| bgzipped VCF (`.vcf.gz`) | ✓ | ✓ | TBI or CSI (auto-scan if absent, with warning) |
| BCF (`.bcf`) | ✓ | ✓ | CSI (required) |

Format conversion between VCF and BCF is the user's responsibility via the pipeline (e.g. `::: bcftools view -Ob`). If the input and output extensions suggest different formats, a warning is printed to stderr but processing continues.

---

## Gather format inference

The output format for `run --gather` and `gather` is inferred from the `-o` extension (case-insensitive). `.gz` and `.bgz` are treated identically.

| Extension | Format | Compression |
|---|---|---|
| `.vcf.gz` or `.vcf.bgz` | VCF | BGZF |
| `.vcf` | VCF | Uncompressed |
| `.bcf` | BCF | BGZF |
| `.gz` or `.bgz` (any other stem) | Text | BGZF |
| Anything else | Text | Uncompressed |

Any extension not matching VCF or BCF is treated as text. When `-o` is omitted or set to `/dev/stdout`, output is written uncompressed.

---

## Advanced examples

```bash
# Multi-stage pipeline: filter then annotate
paravar run --gather -n 8 -o annotated.bcf input.vcf.gz \
  ::: bcftools view -i "GT='alt'" -Ou \
  ::: bcftools +fill-tags -Ob -- -t AF,AC

# VEP annotation in parallel
paravar run --gather -n 8 -o annotated.vcf.gz input.vcf.gz \
  ::: vep --format vcf --vcf --cache --offline --no_stats -o stdout

# bcftools query to text
paravar run --gather -n 8 -o variants.txt input.vcf.gz \
  ::: bcftools query -f '%CHROM\t%POS\t%REF\t%ALT\n'

# Pipe gather output to stdout
paravar gather shard_*.vcf.gz | bcftools stats > stats.txt

# Per-shard output directories (created automatically)
paravar run -n 8 -o /results/{}/out.vcf.gz input.vcf.gz \
  ::: bcftools view -i "GT='alt'" -Oz

# Limit concurrency on a shared node: 32 shards, 8 at a time
paravar run --gather -n 32 -j 8 -o filtered.bcf input.bcf \
  ::: bcftools view -i "GT='alt'" -Ob

# BCF scatter with CSI index
paravar scatter -n 4 -o output.bcf input.bcf
```

---

## Header validation

When gathering VCF or BCF output, paravar checks that the `#CHROM` line (the sample column header) is byte-for-byte identical across all shards before writing any output. A mismatch exits with code 1 and no partial output is written. This catches cases where shards were inadvertently produced from different sample sets.

---

## Limitations

- BCF input requires a CSI index (`bcftools index input.bcf`). There is no auto-scan fallback for BCF.
- Tools in the pipeline must read from stdin and write to stdout. Tools requiring seekable input are not supported.
- Format conversion between VCF and BCF is not automatic — use `bcftools view -Ob` or similar within the pipeline.

---

## Performance notes

Middle BGZF blocks are byte-copied at disk bandwidth with no decompression. Only the boundary blocks — one per split point — are decompressed and recompressed. The cost of splitting scales with the number of shards, not the file size.

`-j` controls how many shard pipelines run concurrently and is independent of `-t` (scatter thread count). Set `-j` based on available cores and the concurrency of the tool being run.

For CPU-heavy tools such as VEP, combining paravar's `-j` with the tool's own threading flag (e.g. `--fork`) can reduce wall time further, keeping the total thread count within available cores.

---

## Development

```bash
# Build
nimble release

# Tests (requires bcftools, bgzip, tabix on PATH)
bash tests/generate_fixtures.sh
nimble test

# Performance benchmark (downloads large fixture)
bash tests/generate_fixtures.sh --perf
time paravar run --gather -n 8 -j 4 -o out.vcf.gz \
  tests/data/chr22_1kg_full.vcf.gz ::: bcftools view -Oz
```
