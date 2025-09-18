import datetime

from pyspark.sql import SparkSession

def test_time_duration(spark): 

    data = [
        (datetime.datetime(2025, 4, 10, 10, 0, 0), datetime.datetime(2025, 4, 10, 10, 10, 0)),  # 10 minutes
        (datetime.datetime(2025, 4, 10, 10, 0, 0), datetime.datetime(2025, 4, 10, 10, 30, 0))   # 30 minutes
    ]

    schema = "start_timestamp timestamp, end_timestamp timestamp"
    df = spark.createDataFrame(data, schema=schema)

    result_df = df.withColumn("duration_minutes", 
                                 (df.end_timestamp.cast("long") - df.start_timestamp.cast("long")) / 60)
    

    results = result_df.select("trip_duration_mins").collect()
    
    # Assert that the differences are as expected
    assert results[0]["trip_duration_mins"] == 10
    assert results[1]["trip_duration_mins"] == 30



