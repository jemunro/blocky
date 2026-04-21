process VCFANNO_BASELINE {
    label 'process_vcfanno'
    cpus { ncpus }

    input:
    tuple val(rep), val(ncpus), path(input), path(index), path(vcfanno_conf)

    output:
    path("result.tsv"), emit: results

    script:
    def mode = 'native'
    """
    /usr/bin/time -v -o timing.txt bash -c 'vcfanno -p ${ncpus} ${vcfanno_conf} ${input} | bgzip > output.vcf.gz'

    # Parse timing and combine with metadata into single result
    parse_time.awk timing.txt > timing.tsv
    printf 'test\\tmode\\tncpus\\tnthreads\\trep\\tformat\\n' > meta.tsv
    printf '%s\\t%s\\t%d\\t%d\\t%d\\t%s\\n' 'vcfanno' '${mode}' ${ncpus} ${ncpus} ${rep} 'vcf' >> meta.tsv
    paste meta.tsv timing.tsv > result.tsv
    """
}
