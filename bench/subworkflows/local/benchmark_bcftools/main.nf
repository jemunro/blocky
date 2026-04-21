include { BCFTOOLS_BASELINE } from '../../../modules/local/bcftools_baseline/main.nf'
include { BLOCKY_BCFTOOLS }  from '../../../modules/local/blocky_bcftools/main.nf'
include { BLOCKY_SCATTER }   from '../../../modules/local/blocky_scatter/main.nf'

workflow BENCHMARK_BCFTOOLS {
    take:
    indexed  // tuple(vcf, tbi, csi, gzi, bcf, bcf_csi)

    main:
    def tests = ['norm', 'fill_tags', 'norm_fill_tags']

    // Extract input channels — all use CSI index
    ch_vcf = indexed.map { vcf, _tbi, csi, _gzi, _bcf, _bcf_csi -> tuple('vcf', vcf, csi) }
    ch_bcf = indexed.map { _vcf, _tbi, _csi, _gzi, bcf, bcf_csi -> tuple('bcf', bcf, bcf_csi) }
    ch_inputs = ch_vcf.mix(ch_bcf)

    // Cross product: test x rep
    ch_design = channel.of(tests).flatten()
        .combine(channel.of(1..params.nreps).flatten())

    // --- Native ---
    ch_native = ch_design
        .combine(ch_inputs)
        .combine(channel.of(params.ncpus).flatten())
        .map { test, rep, fmt, input, idx, ncpus ->
            tuple(test, fmt, rep, ncpus, input, idx)
        }

    BCFTOOLS_BASELINE(ch_native)

    // --- Blocky ---
    ch_blocky = ch_design
        .combine(ch_inputs)
        .combine(channel.of(params.ncpus).flatten())
        .map { test, rep, fmt, input, idx, ncpus ->
            tuple(test, fmt, rep, ncpus, 1, input, idx)
        }

    BLOCKY_BCFTOOLS(ch_blocky)

    // --- Scatter: VCF only, always n=4, compare index types ---
    ch_scatter = channel.of('csi', 'tbi', 'gzi', 'none')
        .combine(channel.of(1..params.nreps).flatten())
        .combine(indexed.map { vcf, tbi, csi, gzi, _bcf, _bcf_csi ->
            tuple(vcf, tbi, csi, gzi)
        })

    BLOCKY_SCATTER(ch_scatter)

    // Collect all results
    ch_results = BCFTOOLS_BASELINE.out.results
        .mix(BLOCKY_BCFTOOLS.out.results)
        .mix(BLOCKY_SCATTER.out.results)

    emit:
    results = ch_results
}
