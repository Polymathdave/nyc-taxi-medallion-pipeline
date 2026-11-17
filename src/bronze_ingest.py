"""Bronze layer: land raw NYC taxi Parquet files with ingestion metadata."""
import argparse
from pyspark.sql import SparkSession, functions as F


def build_spark(app: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main(source: str, target: str) -> None:
    spark = build_spark("bronze_ingest")
    raw = (
        spark.read.parquet(source)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    raw.write.format("delta").mode("append").save(target)
    print(f"[bronze] wrote {raw.count():,} rows to {target}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--target", required=True)
    a = p.parse_args()
    main(a.source, a.target)
