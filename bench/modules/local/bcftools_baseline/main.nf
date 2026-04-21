process BCFTOOLS_BASELINE {
    label 'process_bcftools'
    cpus { ncpus }
    stageInMode 'copy'
    tag "$test:$format:$ncpus:$rep"

    input:
    tuple val(test), val(format), val(rep), val(ncpus), path(input), path(index)

    output:
    path("result.tsv"), emit: results

    script:
    def mode     = 'native'
    def threads  = ncpus == 1 ? 0 : ncpus
    def out_flag = format == 'bcf' ? '-Ob' : '-Oz'
    def out_ext  = format == 'bcf' ? 'bcf' : 'vcf.gz'
    def out_file = "output.${out_ext}"
    def cmd = ''
    if (test == 'norm') {
        cmd = "bcftools norm -m-any --threads ${threads} ${out_flag} -o ${out_file} ${input}"
    } else if (test == 'fill_tags') {
        cmd = "bcftools +fill-tags --threads ${threads} ${out_flag} -o ${out_file} ${input} -- -t AC,AF,AN"
    } else if (test == 'norm_fill_tags') {
        cmd = "bcftools norm -m-any --threads ${threads} -Ou ${input} | bcftools +fill-tags --threads ${threads} ${out_flag} -o ${out_file} -- -t AC,AF,AN"
    }
    """
    /usr/bin/time -v -o timing.txt bash -c '${cmd}'

    # Parse timing and combine with metadata into single result
    parse_time.awk timing.txt > timing.tsv
    printf 'test\\tmode\\tncpus\\tnthreads\\trep\\tformat\\n' > meta.tsv
    printf '%s\\t%s\\t%d\\t%d\\t%d\\t%s\\n' '${test}' '${mode}' ${ncpus} ${threads} ${rep} '${format}' >> meta.tsv
    paste meta.tsv timing.tsv > result.tsv
    
    rm $input $index
    """
}
