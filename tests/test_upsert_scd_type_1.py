from demo.upsert_scd_type_1 import upsert_scd_type_1
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual


def test_upsert_scd_type_1_with_one_primary_key(spark: SparkSession, test_schema: str, test_table: str):
    
    # Given
    ## An existing table
    existing_df = spark.createDataFrame(
        [
            ("1", "a", "b", "c"),
            ("2", "d", "e", "f"),
        ],
        ["id", "a", "b", "c"],
    )

    # Create existing table
    existing_df.write.format("delta").saveAsTable(f"{test_schema}.{test_table}")

    ## Some new data
    new_df = spark.createDataFrame(
        [
            ("2", "j", "k", "l"),
            ("3", "g", "h", "i"),
        ],
        ["id", "a", "b", "c"],
    )

    ## And a primary key
    primary_key = "id"


    # When
    ## We upsert the new data into the existing table
    upsert_scd_type_1(
        source_df=new_df,
        target_table_name=f"{test_schema}.{test_table}",
        primary_key=primary_key,
    )

    # Then
    ## Not matched by source data should be ignored
    ## Matched data should be updated, 
    ## Not matched by target should be inserted, 
    expected_df = spark.createDataFrame(
        [
            ("1", "a", "b", "c"), # Not matched by source data
            ("2", "j", "k", "l"), # Matched data
            ("3", "g", "h", "i"), # Not matched by target
        ],
        ["id", "a", "b", "c"],
    )

    actual_df = spark.read.table(f"{test_schema}.{test_table}")

    assertDataFrameEqual(expected_df, actual_df)