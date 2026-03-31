# CLAUDE.md — paravar

Agentic development instructions for Claude Code. Read this file in full before starting any task.

---

## Project overview

**paravar** is a Nim CLI tool for parallelising VCF processing by exploiting BGZF block boundaries and tabix/CSI indexes.

### Subcommands

| Subcommand | Status | Description |
|---|---|---|
| `scatter` | ✅ Implemented | Split bgzipped VCF into N roughly equal shards |
| `run` | ✅ Implemented | Scatter → parallel per-shard tool pipelines → per-shard output files |

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
│       ├── scatter.nim              # scatter algorithm
│       ├── bgzf_utils.nim           # low-level BGZF I/O (no hts-nim)
│       └── run.nim                  # run subcommand
├── tests/
│   ├── data/                        # generated fixtures (not committed)
│   ├── generate_fixtures.sh
│   ├── test_bgzf_utils.nim          # ✅
│   ├── test_scatter.nim             # ✅
│   ├── test_cli.nim                 # ✅
│   └── test_run.nim                 # 🔲 to be created
└── example/
    └── scatter_vcf.py               # Python reference implementation
```

Do not restructure the module layout without asking the user.

---

## Dependencies

| Dependency | Use |
|---|---|
| zlib (`-lz`) | BGZF compress/decompress in `bgzf_utils.nim` |

No nimble package dependencies. Do not add any without asking the user.

---

## Tech stack and style

- **Language**: Nim (user is a Nim novice — coming from Python/R/C)
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

## Current state of scatter

Scatter is implemented and passing tests. Known gap relevant to `run`:

> **Scatter currently writes shards to files only.** The shard writer takes a file path, not a file descriptor. Before implementing `run`, this must be refactored so the writer accepts a generic writable target — specifically a pipe write-end fd — without changing existing file-write behaviour.

---

## `run` subcommand — specification

### Purpose

Scatter a VCF into N shards, pipe each shard through a user-supplied tool pipeline, and collect each pipeline's stdout into a numbered output file. All shards run concurrently up to `--jobs`.

### CLI

```
paravar run --shards N [--jobs J] -o <prefix> <input.vcf.gz> \
  --- <cmd1> [args...] \
  [--- <cmd2> [args...] ...]
```

### `---` separator

`---` (three dashes) is the pipe-stage separator. It was chosen over `--` to avoid collision with tools (e.g. bcftools plugins) that use `--` as their own argument separator.

- Every token in `argv` that is exactly `"---"` is a stage boundary
- Everything before the first `---` belongs to paravar's own arguments
- Everything after the first `---` is one or more stage definitions
- Multiple `---` blocks define a pipeline: stages are joined with `|` and executed via `sh -c`
- Quoting is optional for simple cases; required only if a stage command itself contains `---` (essentially never)

**Examples:**

```bash
# Single stage
paravar run --shards 10 -o out input.vcf.gz \
  --- bcftools view -i "GT='alt'" -Oz

# Multi-stage pipeline (bcftools plugin using its own --)
paravar run --shards 10 -o out input.vcf.gz \
  --- bcftools +split-vep -Ou -- -f '%SYMBOL\n' \
  --- bcftools view -s Sample -Oz

# Concurrency control
paravar run --shards 50 --jobs 8 -o out input.vcf.gz \
  --- bcftools view -i "GT='alt'" -Oz
```

### Options

| Flag | Description |
|---|---|
| `-n` / `--shards` | Number of shards (required) |
| `--jobs N` | Max simultaneous shard pipelines (default: nproc; 0 = all CPUs) |
| `-o <prefix>` | Output prefix. Shards written to `<prefix>.01.vcf.gz`, `<prefix>.02.vcf.gz`, … |
| `-v` / `--verbose` | Print per-shard progress to stderr |

### Output files

Outputs are named `<prefix>.<N>.vcf.gz` (zero-padded to width of `--shards`), one per shard. The content is the raw stdout of the final pipeline stage — no post-processing. If the tool writes bgzipped output (`-Oz`), the file will be valid bgzipped VCF. If the tool writes uncompressed VCF, the file will be uncompressed. **paravar does not recompress or validate the output** — it is the user's responsibility to pass the right output format flag to the last stage.

### Data flow

```
input.vcf.gz
     │
  [scatter]   ← existing scatter logic, shard bytes written to pipe write-end
     │
  shard 1 → pipe → sh -c "cmd1 | cmd2 | ..." → stdout → prefix.01.vcf.gz  ┐
  shard 2 → pipe → sh -c "cmd1 | cmd2 | ..." → stdout → prefix.02.vcf.gz  ├ (up to --jobs concurrent)
  shard N → pipe → sh -c "cmd1 | cmd2 | ..." → stdout → prefix.0N.vcf.gz  ┘
```

No temporary files. Each shard's bytes flow directly from the scatter writer into the stdin pipe of its shell pipeline.

### Per-shard pipeline construction

For a given shard, build the shell command string by joining stages with ` | `:

```nim
let shellCmd = stages.join(" | ")
# e.g. "bcftools +split-vep -Ou -- -f '%SYMBOL\n' | bcftools view -s Sample -Oz"
```

Then spawn `sh -c <shellCmd>` with:
- `stdin` = pipe read-end (scatter writes shard bytes to write-end)
- `stdout` = output file (opened for writing)
- `stderr` = inherited (passes tool stderr through to terminal)

Use Nim's `osproc.startProcess` with `{poUsePath}` and explicit pipe fds.

### Concurrency model

Use a simple worker-pool pattern:

1. Build a sequence of N shard work items (index, byte range, output path)
2. Dispatch up to `--jobs` items concurrently using a channel or counter semaphore
3. For each completed shard, check exit code — if non-zero, print shard index and exit code to stderr, set a global failure flag, but allow other running shards to complete
4. After all shards finish: if any failed, exit 1; otherwise exit 0

**By default, kill all sibling shards when any shard fails** (send SIGTERM to all in-flight children, wait for them, then exit 1). This avoids wasting CPU on a doomed run.

Add a `--no-kill` flag that disables this: with `--no-kill`, sibling shards are allowed to complete before exiting 1. Useful for debugging (all partial outputs remain on disk).

### Error handling requirements

- Missing `---`: exit 1 with `"paravar run: at least one --- stage is required"`
- Empty stage (two consecutive `---`): exit 1 with `"paravar run: empty pipeline stage"`
- Shard pipeline exits non-zero: print `"shard N: pipeline exited with code X"` to stderr; continue remaining shards; exit 1 at end
- Output file open failure: exit 1 immediately with path and errno

---

## Refactor required before implementing `run`

### Task: abstract the scatter shard writer

In `scatter.nim` (or `bgzf_utils.nim`, wherever `writeShard` lives):

1. Change the writer to accept a file descriptor (`cint`) rather than a `string` path
2. For the existing `scatter` subcommand: open the output file, pass its fd — behaviour unchanged
3. For `run`: pass the write-end of a pipe created with `posix.pipe()`
4. Ensure the BGZF EOF block is still written in both cases
5. Run `nimble test` — all existing scatter and CLI tests must pass unchanged

This is a pure internal refactor. Do not change the scatter CLI or public test interface.

---

## Testing specification for `run`

### Fixtures

Reuse `tests/data/small.vcf.gz` (already generated by `generate_fixtures.sh`). No new fixtures needed for basic `run` tests.

### `tests/test_run.nim`

| Test | What it checks |
|---|---|
| Single stage, 1 shard | Output file exists, records match input |
| Single stage, 4 shards | 4 output files; union of records matches input (no duplicates, no missing) |
| Multi-stage pipeline | Two `---` stages; output is correct |
| `--jobs 1` | Serial fallback; same correctness as parallel |
| `--jobs` > shards | No hang or error |
| Stage exits non-zero | paravar exits 1; stderr contains shard index |
| Missing `---` | Exits 1 with appropriate message |
| bcftools plugin `--` passthrough | `--- bcftools +fill-tags -Ou -- -t AF` works without quoting issues |

### Running

```bash
bash tests/generate_fixtures.sh    # if not already done

export PATH="$HOME/.choosenim/toolchains/nim-2.2.8/bin:$PATH"

nimble test
nim c -r tests/test_run.nim        # single file
```

---

## Workflow rules for Claude Code

### Before starting any task

1. Re-read this file in full
2. Read the relevant source files
3. Consult `example/scatter_vcf.py` for any scatter-related behaviour questions

### Hard rules

| Rule | Detail |
|---|---|
| **No new dependencies** | Do not add to `paravar.nimble` without asking |
| **Test before done** | Run `nimble test` (or the relevant test file) and show full output before declaring a step complete |
| **No commits** | Stage changes, propose a commit message, wait for user |
| **No layout changes** | Do not restructure modules without asking |
| **Small units** | Implement one proc at a time, test it, then proceed |
| **Ask when uncertain** | If behaviour is ambiguous and not covered by this file or the Python reference, stop and ask |

---

## Build reference

```bash
nimble build                            # debug
nimble build -d:release                 # release
nimble test                             # all tests
nim c -d:debug -r tests/test_run.nim    # single test file
```

### BGZF EOF block constant

Every scatter output shard must end with this 28-byte sequence:

```nim
const BGZF_EOF* = [
  0x1f'u8, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00,
  0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
  0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00
]
```

---

## Step-by-step plan

Execute in order. Do not skip ahead. Update checkboxes as steps complete.

### Scatter (complete)
- [x] Scaffold: `paravar.nimble`, directory layout, stub `main.nim`
- [x] `tests/generate_fixtures.sh` — synthetic indexed VCFs
- [x] `bgzf_utils.nim` — block scanning, raw copy, compress/decompress, boundary split
- [x] TBI/CSI index parsing in `scatter.nim`
- [x] Shard boundary optimisation
- [x] Shard writing (header prepend + raw copy + boundary blocks + EOF)
- [x] `main.nim` CLI wiring + `test_cli.nim`
- [x] End-to-end validation

### Run (next)
- [x] **Step R1**: Refactor scatter shard writer to accept a `cint` fd instead of a file path. Run `nimble test` — all existing tests must pass.
- [x] **Step R2**: Implement `---` argv parsing: extract paravar args and stage list, build shell command string. Unit test the parser in isolation (pure string logic, no I/O).
- [x] **Step R3**: Implement single-shard pipe execution in `run.nim`: `posix.pipe()` → `startProcess("sh -c", shellCmd)` → write shard bytes to pipe write-end → collect stdout to output file → wait for exit code. Test with 1 shard using `cat` as the stage command.
- [x] **Step R4**: Implement worker pool (`--jobs`): dispatch N shards concurrently up to job limit, collect exit codes, handle failures per spec. Test with 4 shards.
- [x] **Step R5**: Wire `run` into `main.nim` dispatch. Write `tests/test_run.nim` covering all cases in the testing spec. Run full `nimble test`.

---

## Out of scope (do not implement)

- BCF input/output
- `run` with pre-scattered input glob
- Windows support
- Tools that do not support stdin/stdout (explicitly unsupported — no workaround attempted)
- `-o` flags inside pipeline stage commands (user's responsibility to avoid)