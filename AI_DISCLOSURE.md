# AI Disclosure

This project used AI assistance in accordance with the assignment's Fair Use of AI guidelines.

## How AI Was Used

AI assistance was used for:

- Translating and adapting data-processing logic between Apache Spark/PySpark and Ray Data.
- Debugging Python, Spark, and Ray Data code and helping diagnose execution errors.
- Explaining framework concepts such as distributed execution, lazy evaluation, shuffling, `map_batches()`, Spark's JVM/Python boundary, and Ray's Python-native execution model.
- Assisting with documentation, README preparation, and presentation of the implementation and benchmark observations.

## What Was Done Manually

The following parts of the assignment were performed manually:

- Spark and Ray cluster configuration and network setup.
- Starting the Spark Master/Workers and Ray Head/Workers.
- Configuring the clusters with two workers.
- Running the preprocessing pipelines on the actual NYC Yellow Taxi dataset.
- Collecting execution-time and resource-utilization measurements.
- Validating the observed benchmark results and preparing the final comparison.

## Benchmark Integrity

No synthetic or AI-generated benchmark values were used.

All performance figures reported in the project were obtained from actual executions of the Spark and Ray pipelines on the configured cluster.

## Performance Tuning

AI assistance was also used to suggest/debug specific implementation improvements where required, including handling Ray Data input/schema issues and adapting transformations for the available cluster resources. The resulting changes were tested on the actual cluster before being used in the final implementation.

The benchmark report contains the corresponding Performance Tuning Note and explains the relevant optimization decisions and their effect on the pipeline.

## Summary

AI was used as a development, debugging, translation, and documentation aid. The cluster/network setup, actual execution, benchmark collection, and final evaluation were performed by the student, and the reported performance results are based on measured runs rather than generated values.
