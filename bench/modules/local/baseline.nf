process BASELINE {
    label 'process_bcftools'
    cpus { meta.ncpus }
    stageInMode 'copy'
    tag "${meta.test}:${meta.mode}:${meta.format}:${meta.ncpus}:${meta.rep}"

    input:
    tuple val(meta), val(cmd), path(input), path(index), path(extra)

    output:
    path("result.tsv"), emit: results

    script:
    """
    bench_run.sh '${meta.test}' '${meta.mode}' ${meta.ncpus} ${meta.nthreads} ${meta.rep} '${meta.format}' -- \
        bash -c '${cmd}'
    """
}
