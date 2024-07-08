import pytest
from pyspark.sql import SparkSession
from demo.utils.spark import get_spark_session
from datetime import datetime, timezone

@pytest.fixture(scope="session")
def spark():
    spark = get_spark_session()
    return spark


@pytest.fixture(scope="session", autouse=True)
def test_catalog(spark: SparkSession):
    
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")    
    unit_test_catalog = f"dbx_unit_tests_{timestamp}"

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {unit_test_catalog}")
    spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {unit_test_catalog} TO developers")
    spark.sql(f"USE CATALOG {unit_test_catalog}")

    yield

    spark.sql(f"DROP CATALOG IF EXISTS {unit_test_catalog} CASCADE")


@pytest.fixture(scope="module")
def test_schema(spark: SparkSession, request):
    if "spark" not in request.fixturenames:
        yield
        return

    test_schema_name = request.module.__name__

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {test_schema_name}")
    spark.sql(f"USE SCHEMA {test_schema_name}")

    yield test_schema_name

    spark.sql(f"DROP SCHEMA IF EXISTS {test_schema_name} CASCADE")

@pytest.fixture(scope="function")
def test_table(request):
    table_name = request.function.__name__

    yield table_name

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
