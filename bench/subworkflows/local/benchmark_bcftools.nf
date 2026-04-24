include { BASELINE }       from '../../modules/local/baseline'
include { BLOCKY_RUN }    from '../../modules/local/blocky_run'
include { SCATTER_GATHER } from '../../modules/local/scatter_gather'
include { BLOCKY_SCATTER } from '../../modules/local/blocky_scatter'

def buildBcftoolsCmds(test, out_flag, threads, out_ext, input) {
    def baseline
    def blocky
    def shard

    if (test == 'norm') {
        baseline = "bcftools norm -m-any --threads ${threads} ${out_flag} -o output.${out_ext} ${input}"
        blocky   = "bcftools norm -m-any --threads 0 ${out_flag}"
        shard    = "bcftools norm -m-any --threads 0 ${out_flag} -o proc_\$shard \$shard"
    } else if (test == 'fill_tags') {
        baseline = "bcftools +fill-tags --threads ${threads} ${out_flag} -o output.${out_ext} ${input} -- -t AC,AF,AN"
        blocky   = "bcftools +fill-tags --threads 0 ${out_flag} -- -t AC,AF,AN"
        shard    = "bcftools +fill-tags --threads 0 ${out_flag} -o proc_\$shard \$shard -- -t AC,AF,AN"
    } else if (test == 'norm_fill_tags') {
        baseline = "bcftools norm -m-any --threads ${threads} -Ou ${input} | bcftools +fill-tags --threads ${threads} ${out_flag} -o output.${out_ext} -- -t AC,AF,AN"
        blocky   = "bcftools norm -m-any --threads 0 -Ou ::: bcftools +fill-tags --threads 0 ${out_flag} -- -t AC,AF,AN"
        shard    = "bcftools norm -m-any --threads 0 -Ou \$shard | bcftools +fill-tags --threads 0 ${out_flag} -o proc_\$shard -- -t AC,AF,AN"
    }

    return [baseline: baseline, blocky: blocky, shard: shard]
}

workflow BENCHMARK_BCFTOOLS {
    take:
    indexed  // tuple(vcf, tbi, csi, gzi, bcf, bcf_csi)

    main:
    def tests = ['norm', 'fill_tags', 'norm_fill_tags']

    // Extract input channels — all use CSI index
    ch_vcf = indexed.map { vcf, _tbi, csi, _gzi, _bcf, _bcf_csi -> tuple('vcf', vcf, csi) }
    ch_bcf = indexed.map { _vcf, _tbi, _csi, _gzi, bcf, bcf_csi -> tuple('bcf', bcf, bcf_csi) }
    ch_inputs = ch_vcf.mix(ch_bcf)

    // Cross product: test x rep x format x ncpus
    ch_base = channel.of(tests).flatten()
        .combine(channel.of(1..params.nreps).flatten())
        .combine(ch_inputs)
        .combine(channel.of(params.ncpus).flatten())
        // -> [test, rep, fmt, input, idx, ncpus]

    // --- Baseline ---
    ch_baseline = ch_base.map { test, rep, fmt, input, idx, ncpus ->
        def out_flag = fmt == 'bcf' ? '-Ob' : '-Oz'
        def threads  = ncpus == 1 ? 0 : ncpus
        def out_ext  = fmt == 'bcf' ? 'bcf' : 'vcf.gz'
        def cmds = buildBcftoolsCmds(test, out_flag, threads, out_ext, input)
        def meta = [test: test, mode: 'native', format: fmt, rep: rep, ncpus: ncpus, nthreads: threads]
        tuple(meta, cmds.baseline, input, idx, [])
    }

    BASELINE(ch_baseline)

    // --- Blocky run ---
    ch_blocky = ch_base.map { test, rep, fmt, input, idx, ncpus ->
        def out_flag = fmt == 'bcf' ? '-Ob' : '-Oz'
        def out_ext  = fmt == 'bcf' ? 'bcf' : 'vcf.gz'
        def cmds = buildBcftoolsCmds(test, out_flag, 0, out_ext, input)
        def meta = [test: test, mode: 'blocky', format: fmt, rep: rep, ncpus: ncpus, nthreads: 1]
        tuple(meta, cmds.blocky, input, idx, [])
    }

    BLOCKY_RUN(ch_blocky)

    // --- Scatter/gather ---
    ch_results = BASELINE.out.results
        .mix(BLOCKY_RUN.out.results)

    if (params.run_scatter_gather) {
        ch_sg = ch_base.map { test, rep, fmt, input, idx, ncpus ->
            def out_flag = fmt == 'bcf' ? '-Ob' : '-Oz'
            def out_ext  = fmt == 'bcf' ? 'bcf' : 'vcf.gz'
            def cmds = buildBcftoolsCmds(test, out_flag, 0, out_ext, input)
            def meta = [test: test, mode: 'scatter_gather', format: fmt, rep: rep, ncpus: ncpus, nthreads: 1]
            tuple(meta, cmds.shard, input, idx, [])
        }

        SCATTER_GATHER(ch_sg)
        ch_results = ch_results.mix(SCATTER_GATHER.out.results)
    }

    // --- Scatter index comparison: VCF only, always n=4 ---
    ch_scatter = channel.of('csi', 'tbi', 'gzi', 'none')
        .combine(channel.of(1..params.nreps).flatten())
        .combine(indexed.map { vcf, tbi, csi, gzi, _bcf, _bcf_csi ->
            tuple(vcf, tbi, csi, gzi)
        })

    BLOCKY_SCATTER(ch_scatter)

    ch_results = ch_results.mix(BLOCKY_SCATTER.out.results)

    emit:
    results = ch_results
}
