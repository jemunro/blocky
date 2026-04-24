process BLOCKY_SCATTER {
    label 'process_bcftools'
    cpus 4
    stageInMode 'copy'
    tag "$index_type:$rep"

    input:
    tuple val(index_type), val(rep), path(input), path(tbi), path(csi), path(gzi)

    output:
    path("result.tsv"), emit: results

    script:
    def mode = "scatter_${index_type}"
    """
    # Only keep the index for this test
    if [ "${index_type}" != "tbi" ]; then rm -f *.tbi; fi
    if [ "${index_type}" != "csi" ]; then rm -f *.csi; fi
    if [ "${index_type}" != "gzi" ]; then rm -f *.gzi; fi

    /usr/bin/time -v -o timing.txt bash -c '
        for i in 1 2 3 4 5; do
            blocky scatter ${input} -n 4 -o shard_{}.vcf.gz
            rm -f shard_*.vcf.gz
        done
    '

    # Build result (can't use bench_run.sh due to internal loop)
    parse_time.awk timing.txt > timing.tsv
    printf 'test\\tmode\\tncpus\\tnthreads\\trep\\tformat\\n' > meta.tsv
    printf '%s\\t%s\\t%d\\t%d\\t%d\\t%s\\n' 'scatter' '${mode}' 4 1 ${rep} 'vcf' >> meta.tsv
    paste meta.tsv timing.tsv > result.tsv

    rm $input
    """
}
