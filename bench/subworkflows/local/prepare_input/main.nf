include { PREPARE } from '../../../modules/local/prepare/main.nf'

workflow PREPARE_INPUT {
    take:
    test_vcf     // path: bgzipped VCF
    clinvar_vcf  // path: bgzipped ClinVar VCF
    clinvar_tbi  // path: ClinVar TBI index

    main:
    PREPARE(test_vcf)

    // Generate vcfanno.conf inline via collectFile
    vcfanno_conf = clinvar_vcf.map { vcf ->
        def abs_path = vcf.toRealPath()
        def content = """\
[[annotation]]
file = "${abs_path}"
fields = ["CLNSIG", "GENEINFO", "ID"]
names  = ["CLNSIG", "CLNGENE", "CLNVID"]
ops    = ["self", "self", "self"]
"""
        content
    }.collectFile(name: 'vcfanno.conf', newLine: false)

    emit:
    indexed      = PREPARE.out.indexed   // tuple(vcf, tbi, csi, gzi, bcf, bcf_csi)
    vcfanno_conf = vcfanno_conf          // path(vcfanno.conf)
}
