process REPORT {
    label 'process_r'
    cpus 1
    tag "report"

    publishDir params.outdir, mode: 'copy'

    input:
    path(data_file)
    path(rmd_template)

    output:
    path("report.md"),          emit: markdown
    path("report.html"),        emit: html,    optional: true
    path("report_files/**"),    emit: figures, optional: true

    script:
    """
    cp $rmd_template tmp.rmd && rm $rmd_template && mv tmp.rmd report.Rmd
    Rscript -e 'rmarkdown::render("report.Rmd", params = list(data_file = "${data_file}"), output_file = "report.md")'
    """
}
