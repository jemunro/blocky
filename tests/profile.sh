#!/usr/bin/env bash
# profile.sh — perf flat-profile for 4 paravar gather configurations
#
# Usage:
#   bash tests/profile.sh [--fixture <vcf.gz>] [--binary <paravar>] [--outdir <dir>]
#
# Defaults:
#   --fixture  tests/data/chr22_1kg_50k.vcf.gz
#   --binary   ./paravar
#   --outdir   /tmp/paravar_profiles
#
# For each of 4 input×output combinations, records a perf flat profile and
# writes a <name>.txt report to outdir.  Prints a summary table to stdout.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "${SCRIPT_DIR}")"

FIXTURE="${SCRIPT_DIR}/data/chr22_1kg_50k.vcf.gz"
BINARY="${ROOT_DIR}/paravar"
OUTDIR="/tmp/paravar_profiles"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fixture) FIXTURE="$2"; shift 2 ;;
    --binary)  BINARY="$2";  shift 2 ;;
    --outdir)  OUTDIR="$2";  shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

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

FIXTURE_BCF="${FIXTURE%.vcf.gz}.bcf"
if [[ ! -f "${FIXTURE_BCF}" ]]; then
  echo "Creating BCF fixture: ${FIXTURE_BCF} ..."
  bcftools view -Ob "${FIXTURE}" > "${FIXTURE_BCF}"
  bcftools index "${FIXTURE_BCF}"
fi

mkdir -p "${OUTDIR}"

N=8
J=4

# ---------------------------------------------------------------------------
# Configurations: name, input file, bcftools output flag, output extension
# ---------------------------------------------------------------------------
declare -a NAMES=(vcf_to_vcf vcf_to_bcf bcf_to_vcf bcf_to_bcf)
declare -A INPUT_FILE
declare -A OUT_FLAG
declare -A OUT_EXT

INPUT_FILE[vcf_to_vcf]="${FIXTURE}"
INPUT_FILE[vcf_to_bcf]="${FIXTURE}"
INPUT_FILE[bcf_to_vcf]="${FIXTURE_BCF}"
INPUT_FILE[bcf_to_bcf]="${FIXTURE_BCF}"

OUT_FLAG[vcf_to_vcf]="-Oz"
OUT_FLAG[vcf_to_bcf]="-Ob"
OUT_FLAG[bcf_to_vcf]="-Oz"
OUT_FLAG[bcf_to_bcf]="-Ob"

OUT_EXT[vcf_to_vcf]="vcf.gz"
OUT_EXT[vcf_to_bcf]="bcf"
OUT_EXT[bcf_to_vcf]="vcf.gz"
OUT_EXT[bcf_to_bcf]="bcf"

# ---------------------------------------------------------------------------
# Profile each configuration
# ---------------------------------------------------------------------------
echo "Profiling with: n=${N} j=${J}"
echo ""

for name in "${NAMES[@]}"; do
  infile="${INPUT_FILE[$name]}"
  outflag="${OUT_FLAG[$name]}"
  outext="${OUT_EXT[$name]}"
  outfile="${OUTDIR}/${name}.${outext}"
  perf_data="${OUTDIR}/${name}.perf.data"
  report_file="${OUTDIR}/${name}.txt"

  echo "--- ${name} ---"
  perf record -o "${perf_data}" \
    "${BINARY}" run -n "${N}" -j "${J}" --gather \
      -o "${outfile}" "${infile}" \
      ::: bcftools view "${outflag}" \
    2>/dev/null

  { perf report -i "${perf_data}" --stdio --no-children 2>/dev/null \
      | grep -v '^#' | grep -v '^$' | head -40 > "${report_file}"; } || true

  echo "  written: ${report_file}"
done

echo ""
echo "Top symbols per config (overhead >= 0.5%):"
echo ""

for name in "${NAMES[@]}"; do
  report_file="${OUTDIR}/${name}.txt"
  echo "=== ${name} ==="
  # Show lines with a leading percentage
  { grep -E '^\s+[0-9]+\.[0-9]+%' "${report_file}" | head -20; } || true
  echo ""
done

echo "Profile data written to: ${OUTDIR}/"
