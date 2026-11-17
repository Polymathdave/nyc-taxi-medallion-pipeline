"""Silver layer: clean, type, validate and deduplicate trip records."""
import argparse
from pyspark.sql import functions as F
from bronze_ingest import build_spark
import quality_checks as qc

KEY_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"]


def main(source: str, target: str) -> None:
    spark = build_spark("silver_transform")
    bronze = spark.read.format("delta").load(source)

    typed = (
        bronze.withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("trip_minutes",
                    (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60)
    )

    clean = qc.not_null(typed, KEY_COLUMNS)
    qc.report(typed, clean, "not_null keys")

    valid = qc.in_range(clean, "trip_distance", 0.1, 200)
    valid = qc.in_range(valid, "total_amount", 0, 1000)
    valid = qc.in_range(valid, "trip_minutes", 1, 300)
    qc.report(clean, valid, "range checks")

    silver = valid.dropDuplicates(KEY_COLUMNS).withColumn("pickup_date", F.to_date("pickup_ts"))
    silver.write.format("delta").mode("overwrite").partitionBy("pickup_date").save(target)
    print(f"[silver] wrote {silver.count():,} rows to {target}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--target", required=True)
    a = p.parse_args()
    main(a.source, a.target)
