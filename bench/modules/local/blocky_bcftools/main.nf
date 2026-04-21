process BLOCKY_BCFTOOLS {
    label 'process_bcftools'
    cpus { ncpus }
    stageInMode 'copy'
    tag "$test:$format:$ncpus:$nthreads:$rep"


    input:
    tuple val(test), val(format), val(rep), val(ncpus), val(nthreads), path(input), path(index)

    output:
    path("result.tsv"), emit: results

    script:
    def nworkers    = ncpus.intdiv(nthreads)
    def mode        = nthreads == 1 ? 'blocky' : "blocky_t${nthreads}"
    def bcf_threads = nthreads == 1 ? 0 : nthreads
    def out_ext     = format == 'bcf' ? 'bcf' : 'vcf.gz'
    def out_flag    = format == 'bcf' ? '-Ob' : '-Oz'
    def out_file    = "output.${out_ext}"
    def cmd = ''
    if (test == 'norm') {
        cmd = "blocky run -n ${nworkers} -o ${out_file} ${input} ::: bcftools norm -m-any --threads ${bcf_threads} $out_flag"
    } else if (test == 'fill_tags') {
        cmd = "blocky run -n ${nworkers} -o ${out_file} ${input} ::: bcftools +fill-tags --threads ${bcf_threads} $out_flag -- -t AC,AF,AN"
    } else if (test == 'norm_fill_tags') {
        cmd = "blocky run -n ${nworkers} -o ${out_file} ${input} ::: bcftools norm -m-any --threads ${bcf_threads} -Ou ::: bcftools +fill-tags --threads ${bcf_threads} $out_flag -- -t AC,AF,AN"
    }
    """
    /usr/bin/time -v -o timing.txt bash -c '${cmd}'

    # Parse timing and combine with metadata into single result
    parse_time.awk timing.txt > timing.tsv
    printf 'test\\tmode\\tncpus\\tnthreads\\trep\\tformat\\n' > meta.tsv
    printf '%s\\t%s\\t%d\\t%d\\t%d\\t%s\\n' '${test}' '${mode}' ${ncpus} ${nthreads} ${rep} '${format}' >> meta.tsv
    paste meta.tsv timing.tsv > result.tsv

    rm $input $index
    """
}
