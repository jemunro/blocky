include { BASELINE as BASELINE }             from '../../modules/local/baseline'
include { BLOCKY_RUN as BLOCKY_RUN }         from '../../modules/local/blocky_run'
include { SCATTER_GATHER as SCATTER_GATHER } from '../../modules/local/scatter_gather'

workflow BENCHMARK_VCFANNO {
    take:
    vcf_with_index   // tuple(vcf, csi)
    bundle           // tuple(conf, [clinvar_vcf, clinvar_csi])

    main:
    // Flatten bundle for combine (combine flattens nested lists)
    ch_bundle = bundle.first()

    // Cross product: rep x input x ncpus x bundle
    ch_base = channel.of(1..params.nreps).flatten()
        .combine(vcf_with_index)
        .combine(channel.of(params.ncpus).flatten())
        .combine(ch_bundle)
        // -> [rep, vcf, idx, ncpus, conf, [clinvar_vcf, clinvar_csi]]

    // --- Baseline ---
    ch_baseline = ch_base.map { rep, vcf, idx, ncpus, conf, sources ->
        def meta = [test: 'vcfanno', mode: 'native', format: 'vcf', rep: rep, ncpus: ncpus, nthreads: ncpus]
        def cmd = "vcfanno -p ${ncpus} vcfanno.conf ${vcf} | bgzip > output.vcf.gz"
        tuple(meta, cmd, vcf, idx, [conf] + sources)
    }

    BASELINE(ch_baseline)

    // --- Blocky run ---
    ch_blocky = ch_base.map { rep, vcf, idx, ncpus, conf, sources ->
        def meta = [test: 'vcfanno', mode: 'blocky', format: 'vcf', rep: rep, ncpus: ncpus, nthreads: 1]
        def cmd = "vcfanno -p 1 vcfanno.conf /dev/stdin"
        tuple(meta, cmd, vcf, idx, [conf] + sources)
    }

    BLOCKY_RUN(ch_blocky)

    // --- Scatter/gather ---
    ch_results = BASELINE.out.results
        .mix(BLOCKY_RUN.out.results)

    if (params.run_scatter_gather) {
        ch_sg = ch_base.map { rep, vcf, idx, ncpus, conf, sources ->
            def meta = [test: 'vcfanno', mode: 'scatter_gather', format: 'vcf', rep: rep, ncpus: ncpus, nthreads: 1]
            def cmd = "vcfanno -p 1 vcfanno.conf \$shard | bgzip > proc_\$shard"
            tuple(meta, cmd, vcf, idx, [conf] + sources)
        }

        SCATTER_GATHER(ch_sg)
        ch_results = ch_results.mix(SCATTER_GATHER.out.results)
    }

    emit:
    results = ch_results
}
