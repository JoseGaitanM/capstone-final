import pytest
from pyspark.sql import SparkSession
import os
import sys

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(script_dir, '..')

sys.path.append(parent_dir)
from dags.spark_jobs.create_snapshoots_job import getSubscriptions

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[1]") \
    .appName("testeando ando") \
    .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    return spark

def test_getSubscriptions(spark_session):
    getSubscriptions

def test_getSubscriptions3(spark_session):
    print(os.listdir())
    pass
