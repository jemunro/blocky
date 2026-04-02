#!/usr/bin/env bash
# benchmark.sh — paravar performance grid: j={2,4} × n={4,8} × input={vcf,bcf} × output={vcf,bcf}
#
# Usage:
#   bash tests/benchmark.sh [--fixture <vcf.gz>] [--binary <paravar>]
#
# Defaults:
#   --fixture  tests/data/chr22_1kg_50k.vcf.gz
#   --binary   ./paravar
#
# Prerequisites: bcftools, paravar binary, fixture + TBI index.
# The BCF version of the fixture is created automatically if absent.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "${SCRIPT_DIR}")"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
FIXTURE="${SCRIPT_DIR}/data/chr22_1kg_50k.vcf.gz"
BINARY="${ROOT_DIR}/paravar"
TMPDIR_BASE="${TMPDIR:-/tmp}/paravar_bench"

# ---------------------------------------------------------------------------
# Arg parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --fixture) FIXTURE="$2"; shift 2 ;;
    --binary)  BINARY="$2";  shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Validate inputs
# ---------------------------------------------------------------------------
if [[ ! -f "${FIXTURE}" ]]; then
  echo "error: fixture not found: ${FIXTURE}"
  echo "Run: bash tests/generate_fixtures.sh --perf"
  exit 1
fi
if [[ ! -f "${BINARY}" ]]; then
  echo "error: binary not found: ${BINARY}"
  echo "Run: nimble build -d:release"
  exit 1
fi

# Derive BCF fixture from VCF fixture (same stem, .bcf extension)
FIXTURE_BCF="${FIXTURE%.vcf.gz}.bcf"
if [[ ! -f "${FIXTURE_BCF}" ]]; then
  echo "Creating BCF fixture: ${FIXTURE_BCF} ..."
  bcftools view -Ob "${FIXTURE}" > "${FIXTURE_BCF}"
  bcftools index "${FIXTURE_BCF}"
  echo "  done"
fi

mkdir -p "${TMPDIR_BASE}"

# ---------------------------------------------------------------------------
# Helper: time one command, write elapsed seconds to stdout
# Uses a temp file so that any stderr from the command (e.g. paravar
# format-mismatch warnings) does not contaminate the captured time value.
# ---------------------------------------------------------------------------
run_time() {
  local tf
  tf=$(mktemp)
  /usr/bin/time -f "%e" -o "${tf}" "$@" > /dev/null 2>/dev/null
  cat "${tf}"
  rm -f "${tf}"
}

# ---------------------------------------------------------------------------
# Fixture metadata
# ---------------------------------------------------------------------------
FIXTURE_NREC=$(bcftools view -HG "${FIXTURE}" | wc -l)
FIXTURE_NSAM=$(bcftools query -l "${FIXTURE}" | wc -l)
FIXTURE_SIZE=$(du -sh "${FIXTURE}" | cut -f1)
FIXTURE_BCF_SIZE=$(du -sh "${FIXTURE_BCF}" | cut -f1)

echo "================================================================"
echo "paravar benchmark"
printf "  binary:  %s\n" "${BINARY}"
printf "  fixture: %s  (%s, %d records, %d samples)\n" \
  "${FIXTURE}" "${FIXTURE_SIZE}" "${FIXTURE_NREC}" "${FIXTURE_NSAM}"
printf "  bcf:     %s  (%s)\n" "${FIXTURE_BCF}" "${FIXTURE_BCF_SIZE}"
echo "================================================================"
echo ""

# ---------------------------------------------------------------------------
# Grid parameters
# ---------------------------------------------------------------------------
N_VALUES=(4 8)
J_VALUES=(2 4)

declare -A INPUT_PATH
INPUT_PATH[vcf]="${FIXTURE}"
INPUT_PATH[bcf]="${FIXTURE_BCF}"

declare -A OUTPUT_FLAG
OUTPUT_FLAG[vcf]="-Oz"
OUTPUT_FLAG[bcf]="-Ob"

declare -A OUTPUT_EXT
OUTPUT_EXT[vcf]="vcf.gz"
OUTPUT_EXT[bcf]="bcf"

# ---------------------------------------------------------------------------
# Phase 1: baselines (bcftools only, no paravar)
# ---------------------------------------------------------------------------
echo "--- Baselines (bcftools view, single-threaded) ---"
printf "%-6s  %-6s  %-8s  %s\n" "input" "output" "time(s)" "cmd"

declare -A BASELINE_TIME
declare -A BASELINE_HASH

for input_fmt in vcf bcf; do
  for out_fmt in vcf bcf; do
    infile="${INPUT_PATH[$input_fmt]}"
    outflag="${OUTPUT_FLAG[$out_fmt]}"
    outext="${OUTPUT_EXT[$out_fmt]}"
    outfile="${TMPDIR_BASE}/baseline_${input_fmt}_to_${out_fmt}.${outext}"
    key="${input_fmt}_${out_fmt}"

    t=$(run_time bcftools view "${outflag}" -o "${outfile}" "${infile}")
    BASELINE_TIME[$key]="${t}"
    BASELINE_HASH[$key]=$(bcftools view -H "${outfile}" | sha256sum | cut -d' ' -f1)

    printf "%-6s  %-6s  %-8s  bcftools view %s\n" \
      "${input_fmt}" "${out_fmt}" "${t}" "${outflag}"
  done
done
echo ""

# ---------------------------------------------------------------------------
# Phase 2: paravar grid
# ---------------------------------------------------------------------------
echo "--- paravar grid ---"
printf "%-6s  %-6s  %4s  %4s  %-8s  %-8s  %s\n" \
  "input" "output" "n" "j" "time(s)" "speedup" "status"

for input_fmt in vcf bcf; do
  for out_fmt in vcf bcf; do
    infile="${INPUT_PATH[$input_fmt]}"
    outflag="${OUTPUT_FLAG[$out_fmt]}"
    outext="${OUTPUT_EXT[$out_fmt]}"
    key="${input_fmt}_${out_fmt}"
    baseline="${BASELINE_TIME[$key]}"
    ref_hash="${BASELINE_HASH[$key]}"

    for n in "${N_VALUES[@]}"; do
      for j in "${J_VALUES[@]}"; do
        # Skip j > n (more concurrent workers than shards)
        if (( j > n )); then continue; fi

        outfile="${TMPDIR_BASE}/pv_${input_fmt}_to_${out_fmt}_n${n}_j${j}.${outext}"

        t=$(run_time \
          "${BINARY}" run -n "${n}" -j "${j}" --gather \
            -o "${outfile}" "${infile}" \
            ::: bcftools view "${outflag}")

        speedup=$(awk -v b="${baseline}" -v p="${t}" 'BEGIN{printf "%.2f", b/p}')

        got_hash=$(bcftools view -H "${outfile}" | sha256sum | cut -d' ' -f1)
        if [[ "${got_hash}" == "${ref_hash}" ]]; then
          status="OK"
        else
          status="MISMATCH"
        fi

        printf "%-6s  %-6s  %4d  %4d  %-8s  %-8s  %s\n" \
          "${input_fmt}" "${out_fmt}" "${n}" "${j}" "${t}" "${speedup}x" "${status}"
      done
    done
    echo ""
  done
done

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
rm -rf "${TMPDIR_BASE}"
echo "Done."
