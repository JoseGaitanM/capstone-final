import pytest
from pyspark.sql import SparkSession
import os 

@pytest.fixture(scope="session")

def spark_session():
    spark = SparkSession.builder.master("local[1]") \
    .appName("Create snapshot") \
    .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    return spark

def test_transform_parquet(spark_session):
    # read the input parquet file
    # get the content of the current directory
    content = os.listdir()

    # print the content of the directory
    print(content)

    input_df = spark_session.read.parquet("/opt/airflow/data")

    assert input_df.count() == 10
