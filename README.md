# NYC Taxi Medallion Pipeline (PySpark · Databricks)

> 🚧 **Work in progress.** Bronze, silver and gold layers are scaffolded. Orchestration, tests and benchmarks are next.

![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD4?style=for-the-badge)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

A batch ELT pipeline that ingests public **NYC TLC Yellow Taxi** trip data. It processes the data through a **medallion architecture** (bronze → silver → gold) using PySpark and Delta Lake, and produces analytics-ready tables.

## 🏗️ Architecture
```
 NYC TLC Parquet files
          │
          ▼
 ┌─────────────────┐   raw data + ingestion metadata
 │     BRONZE      │   (append-only Delta table)
 └────────┬────────┘
          ▼
 ┌─────────────────┐   typed, deduplicated, validated
 │     SILVER      │   (data quality rules applied)
 └────────┬────────┘
          ▼
 ┌─────────────────┐   business aggregates:
 │      GOLD       │   daily revenue, zone demand, trip KPIs
 └─────────────────┘
```

## 📁 Project structure
```
├── src/
│   ├── bronze_ingest.py      # Raw ingestion with load metadata
│   ├── silver_transform.py   # Cleaning, typing, deduplication
│   ├── gold_aggregate.py     # Business-level aggregate tables
│   └── quality_checks.py     # Reusable data quality rules
└── README.md
```

## ▶️ Running locally
```bash
pip install pyspark==3.5.1 delta-spark==3.2.0
python src/bronze_ingest.py --source data/raw --target data/bronze
python src/silver_transform.py --source data/bronze --target data/silver
python src/gold_aggregate.py --source data/silver --target data/gold
```
Data comes from the NYC TLC Trip Record Data page (Yellow Taxi monthly Parquet files).

## 🗺️ Roadmap
- [x] Project structure and medallion layer scaffolding
- [x] Data quality rule helpers
- [ ] Incremental (watermark-based) loads into silver
- [ ] Databricks Workflows / Airflow orchestration
- [ ] Unit tests with `pytest` and a local Spark session
- [ ] Partitioning and Z-ordering benchmarks (runtime before vs after)
- [ ] Deploy on Azure Databricks with ADLS Gen2 storage
