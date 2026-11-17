"""Reusable data quality rules for the silver layer."""
from pyspark.sql import DataFrame, functions as F


def not_null(df: DataFrame, columns: list[str]) -> DataFrame:
    """Keep only rows where every listed column is populated."""
    condition = F.lit(True)
    for col in columns:
        condition = condition & F.col(col).isNotNull()
    return df.filter(condition)


def in_range(df: DataFrame, column: str, low: float, high: float) -> DataFrame:
    """Keep only rows where `column` falls within [low, high]."""
    return df.filter(F.col(column).between(low, high))


def report(before: DataFrame, after: DataFrame, rule: str) -> None:
    """Print how many rows a rule removed."""
    dropped = before.count() - after.count()
    print(f"[quality] {rule}: dropped {dropped:,} rows")
