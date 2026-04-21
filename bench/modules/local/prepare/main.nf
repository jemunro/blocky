process PREPARE {
    label 'process_bcftools'
    cpus 2

    input:
    path(vcf)

    output:
    tuple path("prepared.vcf.gz"), path("prepared.vcf.gz.tbi"), path("prepared.vcf.gz.csi"), path("prepared.vcf.gz.gzi"), path("prepared.bcf"), path("prepared.bcf.csi"), emit: indexed

    script:
    def n_samples = params.n_samples
    """
    # Subset to first N samples (if input has more)
    N_TOTAL=\$(bcftools query -l ${vcf} | wc -l)
    if [ "\$N_TOTAL" -gt "${n_samples}" ]; then
        bcftools query -l ${vcf} | head -n ${n_samples} > samples.txt
        bcftools view -S samples.txt --threads ${task.cpus} -Oz -o prepared.vcf.gz ${vcf}
        rm samples.txt
    else
        bcftools view --threads ${task.cpus} -Oz -o prepared.vcf.gz ${vcf}
    fi

    # Create all indices
    tabix -p vcf prepared.vcf.gz
    bcftools index --csi prepared.vcf.gz
    bgzip --reindex prepared.vcf.gz

    # BCF conversion + CSI index
    bcftools view -Ob -o prepared.bcf --threads ${task.cpus} prepared.vcf.gz
    bcftools index --csi prepared.bcf
    """
}
