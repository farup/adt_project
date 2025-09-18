# test_citibike_utils.py
import sys
import os
import pytest

# Run the tests from the root directory
sys.path.append(os.getcwd())

@pytest.fixture()
def spark():
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
    except ImportError:
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            print("Using local PySpark session.")
        except ImportError:
            raise ImportError(
                "Neither databricks.connect nor pyspark is installed. Please install one of them to use Spark functionality."
            )   
    return spark