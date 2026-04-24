process INDEX_CLINVAR {
    label 'process_bcftools'
    cpus 1
    tag "index_clinvar"

    input:
    path(vcf)

    output:
    tuple path(vcf), path("*.csi"), emit: indexed

    script:
    """
    bcftools index --csi ${vcf}
    """
}