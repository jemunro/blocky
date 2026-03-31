#!/usr/bin/env bash
# generate_fixtures.sh — create test VCF fixtures in tests/data/.
#
# Requires: bcftools, bgzip, tabix (all available system-wide on this cluster).
# Idempotent: skips files that already exist.
#
# Produces:
#   tests/data/small.vcf.gz         ~5000 records, 3 chromosomes, TBI indexed
#   tests/data/small_csi.vcf.gz     same content, CSI indexed only (no .tbi)
#   tests/data/chr22_1kg.vcf.gz     25000 records from 1000 Genomes chr22 (optional)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
mkdir -p "${DATA_DIR}"

# ---------------------------------------------------------------------------
# Helper: write a minimal VCF header
# ---------------------------------------------------------------------------
write_header() {
  cat <<'EOF'
##fileformat=VCFv4.2
##FILTER=<ID=PASS,Description="All filters passed">
##contig=<ID=chr1,length=248956422>
##contig=<ID=chr2,length=242193529>
##contig=<ID=chr3,length=198295559>
##INFO=<ID=DP,Number=1,Type=Integer,Description="Total depth">
##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency">
##INFO=<ID=MQ,Number=1,Type=Float,Description="RMS mapping quality">
##INFO=<ID=FS,Number=1,Type=Float,Description="Fisher strand bias">
##INFO=<ID=SOR,Number=1,Type=Float,Description="Strand odds ratio">
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Sample depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype quality">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
EOF
}

# ---------------------------------------------------------------------------
# tiny.vcf.gz — 10 records, used for unit-level splitChunk tests.
# Small enough that all records fit in a single BGZF block.
# ---------------------------------------------------------------------------
TINY="${DATA_DIR}/tiny.vcf.gz"
if [[ ! -f "${TINY}" ]]; then
  echo "Generating ${TINY} ..."
  {
    write_header
    for i in $(seq 1 10); do
      printf "chr1\t%d\t.\tA\tT\t50\tPASS\tDP=10\tGT\t0/1\n" $((i * 1000))
    done
  } | bgzip -c > "${TINY}"
  tabix -p vcf "${TINY}"
  echo "  -> $(bcftools view -HG "${TINY}" | wc -l) records, TBI index: ${TINY}.tbi"
else
  echo "Skipping ${TINY} (already exists)"
fi

# ---------------------------------------------------------------------------
# small.vcf.gz — 5000 records across chr1/chr2/chr3 with a substantial INFO
# field so the uncompressed data exceeds 65536 bytes and spans multiple BGZF
# blocks.  This is required for multi-shard scatter tests.
# ---------------------------------------------------------------------------
SMALL="${DATA_DIR}/small.vcf.gz"
if [[ ! -f "${SMALL}" ]]; then
  echo "Generating ${SMALL} ..."
  {
    write_header
    # chr1: 2500 records — ~90 bytes each → ~225 KB
    for i in $(seq 1 2500); do
      printf "chr1\t%d\t.\tACGT\tTGCA\t50\tPASS\tDP=100;AF=0.25;MQ=60;FS=1.234;SOR=0.500\tGT:DP:GQ\t0/1:50:99\n" \
        $((i * 1000))
    done
    # chr2: 1500 records
    for i in $(seq 1 1500); do
      printf "chr2\t%d\t.\tACGT\tTGCA\t50\tPASS\tDP=100;AF=0.25;MQ=60;FS=1.234;SOR=0.500\tGT:DP:GQ\t0/1:50:99\n" \
        $((i * 1000))
    done
    # chr3: 1000 records
    for i in $(seq 1 1000); do
      printf "chr3\t%d\t.\tACGT\tTGCA\t50\tPASS\tDP=100;AF=0.25;MQ=60;FS=1.234;SOR=0.500\tGT:DP:GQ\t0/1:50:99\n" \
        $((i * 1000))
    done
  } | bcftools sort | bgzip -c > "${SMALL}"
  tabix -p vcf "${SMALL}"
  echo "  -> $(bcftools view -HG "${SMALL}" | wc -l) records, TBI index: ${SMALL}.tbi"
else
  echo "Skipping ${SMALL} (already exists)"
fi

# ---------------------------------------------------------------------------
# small_csi.vcf.gz — same content as small.vcf.gz but CSI indexed only.
# No .tbi is created alongside it so paravar's index-detection falls through
# to the .csi path — exercising parseCsiBlockStarts.
# ---------------------------------------------------------------------------
CSI="${DATA_DIR}/small_csi.vcf.gz"
if [[ ! -f "${CSI}" ]]; then
  echo "Generating ${CSI} (CSI index only) ..."
  cp "${SMALL}" "${CSI}"
  tabix --csi -p vcf "${CSI}"
  echo "  -> $(bcftools view -HG "${CSI}" | wc -l) records, CSI index: ${CSI}.csi"
else
  echo "Skipping ${CSI} (already exists)"
fi

# ---------------------------------------------------------------------------
# small.bcf — BCF conversion of small.vcf.gz, CSI indexed.
# BCF files always use CSI; bcftools index creates .bcf.csi by default.
# ---------------------------------------------------------------------------
SMALL_BCF="${DATA_DIR}/small.bcf"
if [[ ! -f "${SMALL_BCF}" ]]; then
  echo "Generating ${SMALL_BCF} ..."
  bcftools view -Ob "${SMALL}" > "${SMALL_BCF}"
  bcftools index "${SMALL_BCF}"
  echo "  -> $(bcftools view -HG "${SMALL_BCF}" | wc -l) records, CSI index: ${SMALL_BCF}.csi"
else
  echo "Skipping ${SMALL_BCF} (already exists)"
fi

# ---------------------------------------------------------------------------
# chr22_1kg.vcf.gz — real 1000 Genomes chr22 release (optional, ~1 GB)
# Downloaded once; skipped if already present or if no network tool found.
# ---------------------------------------------------------------------------
KG="${DATA_DIR}/chr22_1kg.vcf.gz"
KG_TBI="${KG}.tbi"
KG_VCF_URL="https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz"

SUBSAMPLE_AWK='/^#/{print; next} c<25000{print; c++} c>=25000{exit}'

if [[ ! -f "${KG}" ]]; then
  if command -v wget &>/dev/null; then
    echo "Downloading and subsampling ${KG} (first 25000 records) ..."
    # pipefail off: awk exits early (SIGPIPE) once it has 25000 records — that's expected.
    set +o pipefail
    wget -q -O - "${KG_VCF_URL}" | bgzip -d | awk "${SUBSAMPLE_AWK}" | bgzip -c > "${KG}"
    set -o pipefail
    tabix -p vcf "${KG}"
    echo "  -> $(bcftools view -HG "${KG}" | wc -l) records, index: ${KG_TBI}"
  elif command -v curl &>/dev/null; then
    echo "Downloading and subsampling ${KG} (first 25000 records) ..."
    set +o pipefail
    curl -s -L "${KG_VCF_URL}" | bgzip -d | awk "${SUBSAMPLE_AWK}" | bgzip -c > "${KG}"
    set -o pipefail
    tabix -p vcf "${KG}"
    echo "  -> $(bcftools view -HG "${KG}" | wc -l) records, index: ${KG_TBI}"
  else
    echo "Skipping ${KG} (no wget or curl found)"
  fi
else
  echo "Skipping ${KG} (already exists)"
fi

# ---------------------------------------------------------------------------
# chr22_1kg.bcf — BCF conversion of chr22_1kg.vcf.gz, CSI indexed.
# Large header: 2504 samples.
# ---------------------------------------------------------------------------
KG_BCF="${DATA_DIR}/chr22_1kg.bcf"
if [[ ! -f "${KG_BCF}" ]]; then
  if [[ -f "${KG}" ]]; then
    echo "Generating ${KG_BCF} ..."
    bcftools view -Ob "${KG}" > "${KG_BCF}"
    bcftools index "${KG_BCF}"
    echo "  -> $(bcftools view -HG "${KG_BCF}" | wc -l) records, CSI index: ${KG_BCF}.csi"
  else
    echo "Skipping ${KG_BCF} (${KG} not present)"
  fi
else
  echo "Skipping ${KG_BCF} (already exists)"
fi

echo ""
echo "All fixtures ready in ${DATA_DIR}/"
ls -lh "${DATA_DIR}"/*.vcf.gz "${DATA_DIR}"/*.bcf "${DATA_DIR}"/*.tbi "${DATA_DIR}"/*.csi 2>/dev/null || true
