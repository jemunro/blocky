include { PREPARE }       from '../../modules/local/prepare'
include { INDEX_CLINVAR } from '../../modules/local/index_clinvar'

workflow PREPARE_INPUT {
    take:
    test_vcf     // path: bgzipped VCF
    clinvar_vcf  // path: bgzipped ClinVar VCF

    main:
    PREPARE(test_vcf)
    INDEX_CLINVAR(clinvar_vcf)

    // Generate vcfanno.conf with basename (source files staged alongside)
    vcfanno_conf = clinvar_vcf.map { vcf ->
        def content = """\
[[annotation]]
file = "${vcf.name}"
fields = ["CLNSIG", "GENEINFO", "ID"]
names  = ["CLNSIG", "CLNGENE", "CLNVID"]
ops    = ["self", "self", "self"]
"""
        content
    }.collectFile(name: 'vcfanno.conf', newLine: false)

    // Bundle conf with source files for staging
    vcfanno_bundle = vcfanno_conf
        .combine(INDEX_CLINVAR.out.indexed)
        .map { conf, vcf, csi -> tuple(conf, [vcf, csi]) }

    emit:
    indexed        = PREPARE.out.indexed    // tuple(vcf, tbi, csi, gzi, bcf, bcf_csi)
    vcfanno_bundle = vcfanno_bundle         // tuple(conf, [clinvar_vcf, clinvar_csi])
}
