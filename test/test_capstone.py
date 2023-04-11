import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    # create a SparkSession object
    spark = SparkSession.builder.appName("pytest").getOrCreate()
    yield spark
    spark.stop()

def test_transform_parquet(spark_session):
    # read the input parquet file
    input_df = spark_session.read.parquet("path/to/input/parquet")

    # apply some transformations
    output_df = input_df.filter(input_df["age"] >= 18)

    # read the expected output parquet file
    expected_df = spark_session.read.parquet("path/to/expected/parquet")

    # check that the output dataframe has the expected schema and data
    assert output_df.schema == expected_df.schema
    assert output_df.count() == expected_df.count()
    assert output_df.collect() == expected_df.collect()
