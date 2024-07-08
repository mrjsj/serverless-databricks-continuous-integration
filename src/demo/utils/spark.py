from pyspark.sql import SparkSession
import sys

def get_spark_session() -> SparkSession:
    if "pytest" not in sys.argv[0]:
        return SparkSession.builder.getOrCreate()
    
    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.serverless().getOrCreate()
