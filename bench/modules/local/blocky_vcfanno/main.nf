process BLOCKY_VCFANNO {
    label 'process_vcfanno'
    cpus { ncpus }
    stageInMode 'copy'

    input:
    tuple val(rep), val(ncpus), val(nthreads), path(input), path(index), path(vcfanno_conf)

    output:
    path("result.tsv"), emit: results

    script:
    def nworkers = ncpus.intdiv(nthreads)
    def mode     = nthreads == 1 ? 'blocky' : "blocky_t${nthreads}"
    """
    /usr/bin/time -v -o timing.txt bash -c 'blocky run -n ${nworkers} -o output.vcf.gz ${input} ::: vcfanno -p ${nthreads} ${vcfanno_conf} /dev/stdin ::: bgzip'

    # Parse timing and combine with metadata into single result
    parse_time.awk timing.txt > timing.tsv
    printf 'test\\tmode\\tncpus\\tnthreads\\trep\\tformat\\n' > meta.tsv
    printf '%s\\t%s\\t%d\\t%d\\t%d\\t%s\\n' 'vcfanno' '${mode}' ${ncpus} ${nthreads} ${rep} 'vcf' >> meta.tsv
    paste meta.tsv timing.tsv > result.tsv

    rm $input $index
    """
}
