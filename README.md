# DA3408 Assignment 3 — Spark vs. Ray

## Repository Structure

```text
Assignment-3/
├── spark_clean.py
├── ray_clean.py
├── report.pdf
├── screenshots/
│   └── evidences/
│       ├── spark_master.png
│       ├── ray_dashboard.png
│       └── ...
├── README.md
└── .gitignore
```

## Contents

- `spark_clean.py` — Apache Spark implementation of the taxi-data preprocessing pipeline.
- `ray_clean.py` — Ray Data implementation of the equivalent preprocessing pipeline.
- `report.pdf` — Benchmark report containing performance results, graphs, observations, and conclusions.
- `screenshots/evidences/` — Evidence screenshots for cluster setup and execution.
- `.gitignore` — Prevents large/local files such as dataset files, generated outputs, and the Python virtual environment from being committed.

## Data and Generated Output

The original taxi dataset and generated Parquet output are kept locally and are **not** included in the repository.

```text
Data/       → local input dataset (not committed)
Output/     → generated cleaned Parquet files (not committed)
.venv/      → local Python virtual environment (not committed)
```

## Pipeline

Both implementations follow the assignment pipeline:

```text
Ingestion
   ↓
Cleansing
   ↓
Timestamp Formatting
   ↓
Location Lookup Join
   ↓
Average-Speed UDF
   ↓
Parquet Export
```

## Cluster Evidence

The `screenshots/evidences/` directory contains the screenshots submitted as evidence for:

- Spark cluster with two workers
- Ray cluster with two workers
- Relevant execution/benchmark evidence

## Benchmark

The report compares Spark and Ray using:

- Total wall-clock execution time
- Peak CPU usage
- Peak memory usage
- Python UDF stage time
- Observations on JVM-based Spark execution versus Python-native Ray execution
