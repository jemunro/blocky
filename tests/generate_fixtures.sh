#!/usr/bin/env bash
# generate_fixtures.sh — create test VCF fixtures in tests/data/.
#
# Requires: bcftools, bgzip, tabix (all available system-wide on this cluster).
# Idempotent: skips files that already exist.
#
# Produces:
#   tests/data/small.vcf.gz        ~200 records, 3 chromosomes, tabix indexed
#   tests/data/single_chrom.vcf.gz single chromosome, tabix indexed
#   tests/data/tiny.vcf.gz         3 records (fewer than typical n_shards), tabix indexed

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
  echo "  -> $(bcftools view -H "${SMALL}" | wc -l) records, index: ${SMALL}.tbi"
else
  echo "Skipping ${SMALL} (already exists)"
fi

# ---------------------------------------------------------------------------
# single_chrom.vcf.gz — 80 records on chr1 only
# ---------------------------------------------------------------------------
SINGLE="${DATA_DIR}/single_chrom.vcf.gz"
if [[ ! -f "${SINGLE}" ]]; then
  echo "Generating ${SINGLE} ..."
  {
    write_header
    for i in $(seq 1 80); do
      printf "chr1\t%d\t.\tA\tC\t60\tPASS\t.\tGT\t1/1\n" $((i * 2000))
    done
  } | bgzip -c > "${SINGLE}"
  tabix -p vcf "${SINGLE}"
  echo "  -> $(bcftools view -H "${SINGLE}" | wc -l) records, index: ${SINGLE}.tbi"
else
  echo "Skipping ${SINGLE} (already exists)"
fi

# ---------------------------------------------------------------------------
# tiny.vcf.gz — 3 records (fewer than typical n_shards=4)
# ---------------------------------------------------------------------------
TINY="${DATA_DIR}/tiny.vcf.gz"
if [[ ! -f "${TINY}" ]]; then
  echo "Generating ${TINY} ..."
  {
    write_header
    printf "chr1\t1000\t.\tA\tT\t50\tPASS\t.\tGT\t0/1\n"
    printf "chr1\t2000\t.\tC\tG\t50\tPASS\t.\tGT\t0/1\n"
    printf "chr1\t3000\t.\tG\tA\t50\tPASS\t.\tGT\t0/1\n"
  } | bgzip -c > "${TINY}"
  tabix -p vcf "${TINY}"
  echo "  -> $(bcftools view -H "${TINY}" | wc -l) records, index: ${TINY}.tbi"
else
  echo "Skipping ${TINY} (already exists)"
fi

echo ""
echo "All fixtures ready in ${DATA_DIR}/"
ls -lh "${DATA_DIR}"/*.vcf.gz "${DATA_DIR}"/*.tbi 2>/dev/null || true
