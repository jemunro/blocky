#!/usr/bin/env bash
# bench_profile.sh — blocky profiling benchmark
set -uo pipefail

BINARY="./blocky.baseline"
REPS=3
LABEL="baseline"
VCF="tests/data/chr22_1kg_full.vcf.gz"
BCF="tests/data/chr22_1kg_full.bcf"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary) BINARY="$2"; shift 2 ;;
    --reps)   REPS="$2";   shift 2 ;;
    --label)  LABEL="$2";  shift 2 ;;
    *) echo "Unknown: $1" >&2; exit 1 ;;
  esac
done

TMPD="/tmp/blocky_bench_$$"
mkdir -p "${TMPD}"

echo "binary: ${BINARY}"
echo "vcf: ${VCF} ($(du -sh ${VCF}|cut -f1))"
echo "bcf: ${BCF} ($(du -sh ${BCF}|cut -f1))"
echo "reps: ${REPS}  label: ${LABEL}"
echo ""
echo -e "label\tworkload\twall_s\tpeak_rss_kb\tvol_ctx\tinvol_ctx"

# warmup
cat "${VCF}" "${BCF}" >/dev/null

for rep in $(seq 1 "${REPS}"); do
  rm -f "${TMPD}"/* 2>/dev/null || true

  for testcase in \
    "scatter-vcf-n4:${BINARY} scatter -n 4 -o ${TMPD}/s_{}.vcf.gz ${VCF}" \
    "run-cat-vcf-n4:${BINARY} run -n 4 -o ${TMPD}/c4.vcf.gz ${VCF} ::: cat" \
    "run-bt-vcf-n4:${BINARY} run -n 4 -o ${TMPD}/b4.vcf.gz ${VCF} ::: bcftools view -Oz" \
    "run-cat-bcf-n4:${BINARY} run -n 4 -o ${TMPD}/c4.bcf ${BCF} ::: cat" \
    "run-bt-bcf-n4:${BINARY} run -n 4 -o ${TMPD}/b4.bcf ${BCF} ::: bcftools view -Ob"
  do
    name="${testcase%%:*}"
    cmd="${testcase#*:}"
    tf=$(mktemp)
    /usr/bin/time -v bash -c "${cmd}" >/dev/null 2>"${tf}"
    wall=$(awk '/wall clock/{n=split($NF,t,":");if(n==2)printf"%.2f",t[1]*60+t[2];if(n==3)printf"%.2f",t[1]*3600+t[2]*60+t[3]}' "${tf}")
    rss=$(awk '/Maximum resident/{print $NF}' "${tf}")
    vctx=$(awk '/Voluntary context/{print $NF}' "${tf}")
    ictx=$(awk '/Involuntary context/{print $NF}' "${tf}")
    echo -e "${LABEL}\t${name}\t${wall}\t${rss}\t${vctx}\t${ictx}"
    rm -f "${tf}" "${TMPD}"/* 2>/dev/null || true
  done
done

rm -rf "${TMPD}"
