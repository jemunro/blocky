include { PREPARE_INPUT }      from '../subworkflows/local/prepare_input'
include { BENCHMARK_BCFTOOLS } from '../subworkflows/local/benchmark_bcftools'
include { BENCHMARK_VCFANNO }  from '../subworkflows/local/benchmark_vcfanno'
include { REPORT }             from '../modules/local/report'

workflow BENCHMARK {
    // Input channels
    ch_test_vcf    = Channel.fromPath(params.test_vcf, checkIfExists: true)
    ch_clinvar_vcf = Channel.fromPath(params.clinvar_vcf, checkIfExists: true)

    // Prepare indices and vcfanno.conf
    PREPARE_INPUT(ch_test_vcf, ch_clinvar_vcf)

    ch_results = channel.empty()

    if (params.run_bcftools) {
        BENCHMARK_BCFTOOLS(PREPARE_INPUT.out.indexed)
        ch_results = ch_results.mix(BENCHMARK_BCFTOOLS.out.results)
    }

    if (params.run_vcfanno) {
        ch_vcf_with_csi = PREPARE_INPUT.out.indexed
            .map { vcf, _tbi, csi, _gzi, _bcf, _bcf_csi -> tuple(vcf, csi) }
        BENCHMARK_VCFANNO(ch_vcf_with_csi, PREPARE_INPUT.out.vcfanno_bundle)
        ch_results = ch_results.mix(BENCHMARK_VCFANNO.out.results)
    }

    // Collect all results into single TSV
    ch_data = ch_results
        .collectFile(name: 'benchmark_data.tsv', storeDir: params.outdir, keepHeader: true)

    // Generate report
    ch_rmd = channel.fromPath("${projectDir}/assets/report.Rmd", checkIfExists: true)
    REPORT(ch_data, ch_rmd)
}
