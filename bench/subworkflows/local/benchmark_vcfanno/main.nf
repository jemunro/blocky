include { VCFANNO_BASELINE } from '../../../modules/local/vcfanno_baseline/main.nf'
include { BLOCKY_VCFANNO }  from '../../../modules/local/blocky_vcfanno/main.nf'

workflow BENCHMARK_VCFANNO {
    take:
    vcf_with_index   // tuple(vcf, tbi)
    vcfanno_conf     // path(vcfanno.conf) — contains absolute path to clinvar

    main:
    // Build base channel: rep x input x conf
    ch_base = channel.of(1..params.nreps).flatten()
        .combine(vcf_with_index)
        .combine(vcfanno_conf)
        // -> [rep, vcf, tbi, conf]

    // --- Native: all ncpus (ncpus=1 serves as baseline) ---
    ch_native = ch_base
        .combine(channel.of(params.ncpus).flatten())
        .map { rep, vcf, tbi, conf, ncpus ->
            tuple(rep, ncpus, vcf, tbi, conf)
        }

    VCFANNO_BASELINE(ch_native)

    // --- Blocky: nthreads=1 for all ncpus, plus nthreads=2 for ncpus >= 4 ---
    ch_blocky_t1 = ch_base
        .combine(channel.of(params.ncpus).flatten())
        .map { rep, vcf, tbi, conf, ncpus ->
            tuple(rep, ncpus, 1, vcf, tbi, conf)
        }

    ch_blocky_t2 = ch_base
        .combine(channel.of(params.ncpus).flatten().filter { it >= 4 })
        .map { rep, vcf, tbi, conf, ncpus ->
            tuple(rep, ncpus, 2, vcf, tbi, conf)
        }

    BLOCKY_VCFANNO(ch_blocky_t1.mix(ch_blocky_t2))

    emit:
    results = VCFANNO_BASELINE.out.results.mix(BLOCKY_VCFANNO.out.results)
}
