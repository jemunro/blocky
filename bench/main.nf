#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

include { BENCHMARK } from './workflows/benchmark'

workflow {
    BENCHMARK()
}
