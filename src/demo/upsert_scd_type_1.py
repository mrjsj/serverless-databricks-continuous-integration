from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
from demo.utils.spark import get_spark_session

def upsert_scd_type_1(
        source_df: DataFrame,
        target_table_name: str,
        primary_key: str | list[str],
    ) -> None:
    """
    Upsert scd type 1 data into a target table.
    
    Args:
        spark (SparkSession): Spark session.
        source_df (DataFrame): Source data.
        target_table (str): Target table name.
        primary_keys (list[str]): Primary key columns.
    """
    if isinstance(primary_key, str):
        primary_key = [primary_key]

    if any(c not in source_df.columns for c in primary_key):
        raise ValueError("Primary key columns not found in source data")

    spark = get_spark_session()
    target_table = DeltaTable.forName(spark, target_table_name)

    if any(c not in target_table.columns for c in primary_key):
        raise ValueError("Primary key columns not found in target table")
    
    merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in primary_key])

    (
        target_table.alias("target").merge(
            source_df.alias("source"),
            condition=merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    