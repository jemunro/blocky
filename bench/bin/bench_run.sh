#!/usr/bin/env bash
# Usage: bench_run.sh <test> <mode> <ncpus> <nthreads> <rep> <format> -- <command...>
set -euo pipefail

test=$1; mode=$2; ncpus=$3; nthreads=$4; rep=$5; format=$6
shift 6; shift  # skip --

/usr/bin/time -v -o timing.txt "$@"

parse_time.awk timing.txt > timing.tsv
printf 'test\tmode\tncpus\tnthreads\trep\tformat\n' > meta.tsv
printf '%s\t%s\t%d\t%d\t%d\t%s\n' "$test" "$mode" "$ncpus" "$nthreads" "$rep" "$format" >> meta.tsv
paste meta.tsv timing.tsv > result.tsv
