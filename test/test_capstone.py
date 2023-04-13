import pytest
from pyspark.sql import SparkSession
import os
import sys
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, StringType, FloatType, DateType
import tempfile
from datetime import datetime


script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(script_dir, '..')

sys.path.append(parent_dir)
from dags.spark_jobs.create_snapshoots_job import getSubscriptions

def convertRows(rows):
    converted_rows = []

    for row in rows:
        converted_rows.append((
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            datetime.strptime(row[6], '%Y-%m-%d').date(),
            datetime.strptime(row[7], '%Y-%m-%d').date()
        ))

    return converted_rows


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[1]") \
    .appName("testeando ando") \
    .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    return spark

@pytest.fixture(scope="session")
def data_schema_registers():
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("active", BooleanType(), True),
        StructField("subscription", StringType(), True),
        StructField("customer_first_name", StringType(), True),
        StructField("customer_last_name", StringType(), True),
        StructField("cost", IntegerType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True)
    ])

    return schema

@pytest.fixture(scope="session")
def enriched_data_schema_registers():
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("active", BooleanType(), True),
        StructField("subscription", StringType(), True),
        StructField("customer_first_name", StringType(), True),
        StructField("customer_last_name", StringType(), True),
        StructField("cost", IntegerType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("date", DateType(), True)
    ])

    return schema

@pytest.fixture(scope="session")
def test_registers_1(spark_session,data_schema_registers):
    rows = [
        (3, False, 'Family', 'Arthur', 'Hutchinson', 53, '2023-03-25', '2023-06-01'),
        (4, False, 'Family', 'Sara', 'Rodriguez', 89, '2023-04-15', '2023-09-25'),
        (9, False, 'Premium', 'Jill', 'Rivers', 64, '2023-04-07', '2023-09-09'),
        (11, False, 'Basic', 'Chelsea', 'Logan', 67, '2023-03-16', '2023-12-07'),
        (13, False, 'News', 'Danielle', 'Jackson', 66, '2023-04-03', '2024-03-25'),
        (23, False, 'Ultimate', 'Lori', 'Sanchez', 69, '2023-04-20', '2023-05-08'),
        (26, True, 'Family', 'James', 'Hall', 76, '2023-05-07', '2023-10-26'),
        (29, True, 'Movies', 'Kyle', 'Crawford', 51, '2023-03-14', '2023-05-22'),
        (36, True, 'Custom', 'Jacob', 'Carter', 92, '2023-04-25', '2024-01-28'),
        (37, True, 'Premium Plus', 'Jennifer', 'Dudley', 59, '2023-04-28', '2023-09-18')
    ]

    rows = convertRows(rows)
    df = spark_session.createDataFrame(rows,data_schema_registers)

    return df

@pytest.fixture(scope="session")
def test_registers_2(spark_session,data_schema_registers):
    rows = [
        (3, True, "Premium Plus", "Michael", "Carson", 89, "2023-03-31", "2023-09-12"),
        (5, True, "Entertainment", "Travis", "Douglas", 76, "2023-03-29", "2023-10-15"),
        (7, True, "Family", "Crystal", "Howard", 84, "2023-03-30", "2023-12-19"),
        (9, True, "Sports", "Cindy", "Smith", 87, "2023-03-10", "2023-05-31"),
        (10, False, "Custom", "Sabrina", "Welch", 51, "2023-04-25", "2024-01-28"),
        (11, True, "Basic", "Tanya", "Miller", 79, "2023-04-12", "2023-08-13"),
        (14, False, "News", "Yolanda", "Gonzalez", 56, "2023-04-28", "2024-01-08"),
        (15, False, "Entertainment", "Wanda", "Jones", 97, "2023-05-07", "2023-07-30"),
        (16, True, "News", "Jonathan", "Fowler", 81, "2023-03-30", "2023-07-15"),
        (18, True, "Movies", "Michael", "Mueller", 54, "2023-04-09", "2023-06-25")
    ]

    rows = convertRows(rows)
    df = spark_session.createDataFrame(rows,data_schema_registers)

    return df

@pytest.fixture(scope="session")
def test_registers_2(spark_session,data_schema_registers):
    rows = [
        (3, True, "Premium Plus", "Michael", "Carson", 89, "2023-03-31", "2023-09-12"),
        (5, True, "Entertainment", "Travis", "Douglas", 76, "2023-03-29", "2023-10-15"),
        (7, True, "Family", "Crystal", "Howard", 84, "2023-03-30", "2023-12-19"),
        (9, True, "Sports", "Cindy", "Smith", 87, "2023-03-10", "2023-05-31"),
        (10, False, "Custom", "Sabrina", "Welch", 51, "2023-04-25", "2024-01-28"),
        (11, True, "Basic", "Tanya", "Miller", 79, "2023-04-12", "2023-08-13"),
        (14, False, "News", "Yolanda", "Gonzalez", 56, "2023-04-28", "2024-01-08"),
        (15, False, "Entertainment", "Wanda", "Jones", 97, "2023-05-07", "2023-07-30"),
        (16, True, "News", "Jonathan", "Fowler", 81, "2023-03-30", "2023-07-15"),
        (18, True, "Movies", "Michael", "Mueller", 54, "2023-04-09", "2023-06-25")
    ]

    rows = convertRows(rows)
    df = spark_session.createDataFrame(rows,data_schema_registers)

    return df

def test_getSubscriptions(spark_session,test_registers_1):

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir = f"{tmpdir}/subscriptions"
        test_registers_1.show()
        test_registers_1.write.format('parquet').save(temp_dir)

    ruta = f"{tmpdir}/subscriptions"
    subscriptions = getSubscriptions(ruta,spark_session)
    assert subscriptions.count() == 10

def test_getSubscriptions3(spark_session):
    
    print(os.listdir())
    pass

