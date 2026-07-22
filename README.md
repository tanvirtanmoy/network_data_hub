# Network Data Hub

Data ingestion pipeline for daily telecom network metrics.

Cell towers upload a raw CSV (`network_metrics_YYYYMMDD.csv`) to an S3 bucket every day.
This project ingests those files and processes them through a medallion architecture
(bronze → silver → gold) with data quality checks, producing hourly and daily
per-region aggregates for the operations team.

## Stack

- **Orchestration:** Apache Airflow
- **Processing:** PySpark
- **Storage:** S3 + Iceberg (simulated locally with the filesystem + Parquet)

## Project layout

```
├── config/     # pipeline configuration
├── dags/       # Airflow DAG definitions
├── src/        # PySpark jobs (ingestion, validation, transformations)
├── scripts/    # local runner
├── sql/        # mock table DDL for bronze/silver/gold layers
├── tests/      # unit tests
└── data/raw/   # simulated S3 landing bucket with the sample file
```

Detailed architecture notes, data quality strategy, and monitoring design will be
documented here as the pipeline is built.
