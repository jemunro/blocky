#!/usr/bin/env bash
# profile.sh — 4-layer profiling for blocky
#
# Usage:
#   bash tests/profile.sh [--layers 1,2,3,4] [--outdir <dir>] [--reps N]
#
# Layers:
#   1  Wall-clock + resource counters (/usr/bin/time -v), 5 reps, n=2 and n=4
#   2  Syscall profile (strace -c -f), 1 rep, n=2 and n=4
#   3  Call-graph sampling (perf record -g), n=4, blocky-dominated workloads
#   4  Instruction-level (callgrind), n=2, scatter and run-cat only
set -uo pipefail

PROF="./blocky.prof"       # non-stripped, for perf/callgrind
BENCH="./blocky.baseline"  # stripped, for wall-clock
VCF="tests/data/chr22_1kg_50k.vcf.gz"
BCF="tests/data/chr22_1kg_50k.bcf"
LAYERS="1,2,3,4"
OUTDIR="profiling_results"
REPS=5

while [[ $# -gt 0 ]]; do
  case "$1" in
    --layers) LAYERS="$2"; shift 2 ;;
    --outdir) OUTDIR="$2"; shift 2 ;;
    --reps)   REPS="$2";   shift 2 ;;
    *) echo "Unknown: $1" >&2; exit 1 ;;
  esac
done

for f in "${PROF}" "${BENCH}" "${VCF}" "${BCF}"; do
  [[ -f "$f" ]] || { echo "error: not found: $f" >&2; exit 1; }
done

mkdir -p "${OUTDIR}/layer2_strace" "${OUTDIR}/layer3_perf" "${OUTDIR}/layer4_callgrind"
TMP=$(mktemp -d /tmp/blocky_profile.XXXXXX)
trap "rm -rf ${TMP}" EXIT

# Warmup page cache
cat "${VCF}" "${BCF}" >/dev/null

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
parse_time_v() {
  local tf="$1"
  local wall user sys rss vctx ictx
  wall=$(awk '/wall clock/{n=split($NF,t,":");if(n==2)printf"%.3f",t[1]*60+t[2];if(n==3)printf"%.3f",t[1]*3600+t[2]*60+t[3]}' "${tf}")
  user=$(awk '/User time/{printf"%.3f",$NF}' "${tf}")
  sys=$(awk '/System time/{printf"%.3f",$NF}' "${tf}")
  rss=$(awk '/Maximum resident/{print $NF}' "${tf}")
  vctx=$(awk '/Voluntary context/{print $NF}' "${tf}")
  ictx=$(awk '/Involuntary context/{print $NF}' "${tf}")
  echo "${wall}	${user}	${sys}	${rss}	${vctx}	${ictx}"
}

prepare_gather_shards() {
  local bin="$1" n="$2" input="$3" ext="$4"
  rm -f "${TMP}"/gather_shard_*.${ext}
  ${bin} scatter -n "${n}" -o "${TMP}/gather_shard_{}.${ext}" "${input}" 2>/dev/null
}

clean_tmp() { rm -f "${TMP}"/* 2>/dev/null || true; }

# =========================================================================
# LAYER 1: Wall-clock + resource counters
# =========================================================================
if [[ "${LAYERS}" == *1* ]]; then
  echo "=== Layer 1: Wall-clock benchmarks (${REPS} reps) ===" >&2
  L1="${OUTDIR}/layer1_timings.tsv"
  echo -e "workload\tn\trep\twall_s\tuser_s\tsys_s\tpeak_rss_kb\tvol_ctx\tinvol_ctx" > "${L1}"

  for n in 2 4; do
    for rep in $(seq 1 "${REPS}"); do
      tf=$(mktemp)

      # W1: scatter VCF
      clean_tmp
      /usr/bin/time -v bash -c "${BENCH} scatter -n ${n} -o ${TMP}/s_{}.vcf.gz ${VCF}" >/dev/null 2>"${tf}"
      echo -e "W1_scatter_vcf\t${n}\t${rep}\t$(parse_time_v "${tf}")" >> "${L1}"

      # W2: scatter BCF
      clean_tmp
      /usr/bin/time -v bash -c "${BENCH} scatter -n ${n} -o ${TMP}/s_{}.bcf ${BCF}" >/dev/null 2>"${tf}"
      echo -e "W2_scatter_bcf\t${n}\t${rep}\t$(parse_time_v "${tf}")" >> "${L1}"

      # W3: run cat VCF
      clean_tmp
      /usr/bin/time -v bash -c "${BENCH} run -n ${n} -o ${TMP}/out.vcf.gz ${VCF} ::: cat" >/dev/null 2>"${tf}"
      echo -e "W3_run_cat_vcf\t${n}\t${rep}\t$(parse_time_v "${tf}")" >> "${L1}"

      # W4: run cat BCF
      clean_tmp
      /usr/bin/time -v bash -c "${BENCH} run -n ${n} -o ${TMP}/out.bcf ${BCF} ::: cat" >/dev/null 2>"${tf}"
      echo -e "W4_run_cat_bcf\t${n}\t${rep}\t$(parse_time_v "${tf}")" >> "${L1}"

      # W5: run bcftools VCF
      clean_tmp
      /usr/bin/time -v bash -c "${BENCH} run -n ${n} -o ${TMP}/out.vcf.gz ${VCF} ::: bcftools view -Oz" >/dev/null 2>"${tf}"
      echo -e "W5_run_bt_vcf\t${n}\t${rep}\t$(parse_time_v "${tf}")" >> "${L1}"

      # W6: run bcftools BCF
      clean_tmp
      /usr/bin/time -v bash -c "${BENCH} run -n ${n} -o ${TMP}/out.bcf ${BCF} ::: bcftools view -Ob" >/dev/null 2>"${tf}"
      echo -e "W6_run_bt_bcf\t${n}\t${rep}\t$(parse_time_v "${tf}")" >> "${L1}"

      # W7: gather VCF
      clean_tmp
      prepare_gather_shards "${BENCH}" "${n}" "${VCF}" "vcf.gz"
      /usr/bin/time -v bash -c "${BENCH} gather -o ${TMP}/gathered.vcf.gz ${TMP}/gather_shard_*.vcf.gz" >/dev/null 2>"${tf}"
      echo -e "W7_gather_vcf\t${n}\t${rep}\t$(parse_time_v "${tf}")" >> "${L1}"

      # W8: gather BCF
      clean_tmp
      prepare_gather_shards "${BENCH}" "${n}" "${BCF}" "bcf"
      /usr/bin/time -v bash -c "${BENCH} gather -o ${TMP}/gathered.bcf ${TMP}/gather_shard_*.bcf" >/dev/null 2>"${tf}"
      echo -e "W8_gather_bcf\t${n}\t${rep}\t$(parse_time_v "${tf}")" >> "${L1}"

      rm -f "${tf}"
    done
    echo "  n=${n} complete" >&2
  done
  echo "  -> ${L1}" >&2
fi

# =========================================================================
# LAYER 2: Syscall profile (strace -c -f)
# =========================================================================
if [[ "${LAYERS}" == *2* ]]; then
  echo "=== Layer 2: Syscall profiles ===" >&2

  for n in 2 4; do
    for wid_cmd in \
      "W1_scatter_vcf:${BENCH} scatter -n ${n} -o ${TMP}/s_{}.vcf.gz ${VCF}" \
      "W2_scatter_bcf:${BENCH} scatter -n ${n} -o ${TMP}/s_{}.bcf ${BCF}" \
      "W3_run_cat_vcf:${BENCH} run -n ${n} -o ${TMP}/out.vcf.gz ${VCF} ::: cat" \
      "W4_run_cat_bcf:${BENCH} run -n ${n} -o ${TMP}/out.bcf ${BCF} ::: cat" \
      "W5_run_bt_vcf:${BENCH} run -n ${n} -o ${TMP}/out.vcf.gz ${VCF} ::: bcftools view -Oz" \
      "W6_run_bt_bcf:${BENCH} run -n ${n} -o ${TMP}/out.bcf ${BCF} ::: bcftools view -Ob"
    do
      wid="${wid_cmd%%:*}"
      cmd="${wid_cmd#*:}"
      clean_tmp
      outf="${OUTDIR}/layer2_strace/${wid}_n${n}.txt"
      strace -c -f bash -c "${cmd}" >/dev/null 2>"${outf}"
      echo "  ${wid} n=${n}" >&2
    done

    # Gather workloads
    for fmt_info in "W7_gather_vcf:vcf.gz:${VCF}" "W8_gather_bcf:bcf:${BCF}"; do
      wid="${fmt_info%%:*}"
      rest="${fmt_info#*:}"
      ext="${rest%%:*}"
      input="${rest#*:}"
      clean_tmp
      prepare_gather_shards "${BENCH}" "${n}" "${input}" "${ext}"
      outf="${OUTDIR}/layer2_strace/${wid}_n${n}.txt"
      strace -c -f bash -c "${BENCH} gather -o ${TMP}/gathered.${ext} ${TMP}/gather_shard_*.${ext}" >/dev/null 2>"${outf}"
      echo "  ${wid} n=${n}" >&2
    done
  done
  echo "  -> ${OUTDIR}/layer2_strace/" >&2
fi

# =========================================================================
# LAYER 3: perf record call-graph sampling
# =========================================================================
if [[ "${LAYERS}" == *3* ]]; then
  echo "=== Layer 3: perf call-graph sampling (n=4) ===" >&2
  n=4

  for wid_cmd in \
    "W1_scatter_vcf:${PROF} scatter -n ${n} -o ${TMP}/s_{}.vcf.gz ${VCF}" \
    "W2_scatter_bcf:${PROF} scatter -n ${n} -o ${TMP}/s_{}.bcf ${BCF}" \
    "W3_run_cat_vcf:${PROF} run -n ${n} -o ${TMP}/out.vcf.gz ${VCF} ::: cat" \
    "W4_run_cat_bcf:${PROF} run -n ${n} -o ${TMP}/out.bcf ${BCF} ::: cat"
  do
    wid="${wid_cmd%%:*}"
    cmd="${wid_cmd#*:}"
    clean_tmp
    perfdata="${TMP}/${wid}.data"
    outf="${OUTDIR}/layer3_perf/${wid}_n${n}.perf"
    perf record -g -o "${perfdata}" bash -c "${cmd}" >/dev/null 2>/dev/null
    perf report -i "${perfdata}" --stdio --no-children --percent-limit 0.5 > "${outf}" 2>/dev/null
    rm -f "${perfdata}"
    echo "  ${wid}" >&2
  done

  # Gather
  clean_tmp
  prepare_gather_shards "${PROF}" "${n}" "${VCF}" "vcf.gz"
  perfdata="${TMP}/W7.data"
  perf record -g -o "${perfdata}" bash -c "${PROF} gather -o ${TMP}/gathered.vcf.gz ${TMP}/gather_shard_*.vcf.gz" >/dev/null 2>/dev/null
  perf report -i "${perfdata}" --stdio --no-children --percent-limit 0.5 > "${OUTDIR}/layer3_perf/W7_gather_vcf_n${n}.perf" 2>/dev/null
  rm -f "${perfdata}"
  echo "  W7_gather_vcf" >&2

  echo "  -> ${OUTDIR}/layer3_perf/" >&2
fi

# =========================================================================
# LAYER 4: callgrind instruction profiling
# =========================================================================
if [[ "${LAYERS}" == *4* ]]; then
  echo "=== Layer 4: callgrind (n=2) ===" >&2
  n=2

  for wid_cmd in \
    "W1_scatter_vcf:${PROF} scatter -n ${n} -o ${TMP}/s_{}.vcf.gz ${VCF}" \
    "W3_run_cat_vcf:${PROF} run -n ${n} -o ${TMP}/out.vcf.gz ${VCF} ::: cat"
  do
    wid="${wid_cmd%%:*}"
    cmd="${wid_cmd#*:}"
    clean_tmp
    cgout="${TMP}/${wid}.cg"
    annot="${OUTDIR}/layer4_callgrind/${wid}_n${n}.txt"
    echo "  ${wid} (slow — valgrind) ..." >&2
    valgrind --tool=callgrind --callgrind-out-file="${cgout}" bash -c "${cmd}" >/dev/null 2>/dev/null
    callgrind_annotate "${cgout}" > "${annot}" 2>/dev/null
    rm -f "${cgout}"
    echo "  ${wid} done" >&2
  done
  echo "  -> ${OUTDIR}/layer4_callgrind/" >&2
fi

# =========================================================================
# Summary
# =========================================================================
echo "" >&2
echo "=== Profiling complete ===" >&2
echo "Results in: ${OUTDIR}/" >&2
if [[ -f "${OUTDIR}/layer1_timings.tsv" ]]; then
  echo "" >&2
  echo "--- Layer 1 median wall_s ---" >&2
  awk -F'\t' 'NR>1{k=$1"\tn="$2; a[k]=a[k] $4 " "}
    END{for(k in a){n=split(a[k],v," "); asort(v); printf "  %-25s %s\n", k, v[int(n/2)+1]}}' \
    "${OUTDIR}/layer1_timings.tsv" | sort >&2
fi
