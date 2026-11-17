"""Gold layer: business-ready aggregates built from silver trips."""
import argparse
from pyspark.sql import functions as F
from bronze_ingest import build_spark


def main(source: str, target: str) -> None:
    spark = build_spark("gold_aggregate")
    trips = spark.read.format("delta").load(source)

    daily = trips.groupBy("pickup_date").agg(
        F.count("*").alias("trips"),
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("trip_minutes"), 1).alias("avg_minutes"),
    )
    zones = trips.groupBy("PULocationID").agg(
        F.count("*").alias("pickups"),
        F.round(F.avg("total_amount"), 2).alias("avg_fare"),
    )

    daily.write.format("delta").mode("overwrite").save(f"{target}/daily_trip_kpis")
    zones.write.format("delta").mode("overwrite").save(f"{target}/zone_demand")
    print("[gold] wrote daily_trip_kpis and zone_demand")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--target", required=True)
    a = p.parse_args()
    main(a.source, a.target)
