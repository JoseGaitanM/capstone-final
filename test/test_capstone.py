import pytest
from pyspark.sql import SparkSession
import os
import sys
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, StringType, DateType, LongType, MapType
import tempfile
from datetime import datetime

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(script_dir, '..')
sys.path.append(parent_dir)

from dags.spark_jobs.create_snapshoots_job import getMaxDateSnapshots, readMaxDateSnapshoots, getSubscriptions, getMaxDateRegisters, enrichNewRegisters, joinData, createNewSnapshot, enrichData

def convertRows(rows):
    converted_rows = []

    for row in rows:
        row['start_date'] = datetime.strptime(row['start_date'], '%Y-%m-%d').date()
        row['end_date'] = datetime.strptime(row['end_date'], '%Y-%m-%d').date()
        converted_rows.append(row)

    return converted_rows

##Session
@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[1]") \
    .appName("testeando ando") \
    .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "False")
    return spark

#############################################SCHEMAS
#####Registers
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
def data_schema_retrieve_registers():
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


######Enriched Data
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
        StructField("numberOfChannels", LongType(), False),
        StructField("extras", MapType(StringType(), StringType(), valueContainsNull = True), nullable = True),
        StructField("date", DateType(), True)
    ])

    return schema

#######Subscriptions
@pytest.fixture(scope="session")
def test_subscriptions_schema():
    schema = StructType([
        StructField("subscription", StringType(), True),
        StructField("numberOfChannels", LongType(), True),
        StructField("extras", MapType(StringType(), StringType(), valueContainsNull = True), nullable = True)
    ])

    return schema

#############################################DATA FRAMES
#####################REGISTERS
@pytest.fixture(scope="session")
def test_registers_1(spark_session,data_schema_registers):
    rows = [
        {"id": 1, "active": False, "subscription": "Basic", "customer_first_name": "John", "customer_last_name": "Doe", "cost": 50, "start_date": "2023-04-01", "end_date": "2023-09-30"},
        {"id": 2, "active": False, "subscription": "Premium", "customer_first_name": "Jane", "customer_last_name": "Smith", "cost": 75, "start_date": "2023-03-15", "end_date": "2023-06-30"},
        {"id": 3, "active": False, "subscription": "Family", "customer_first_name": "Arthur", "customer_last_name": "Hutchinson", "cost": 53, "start_date": "2023-03-25", "end_date": "2023-06-01"},
        {"id": 4, "active": False, "subscription": "Family", "customer_first_name": "Sara", "customer_last_name": "Rodriguez", "cost": 89, "start_date": "2023-04-15", "end_date": "2023-09-25"},
        {"id": 9, "active": False, "subscription": "Premium", "customer_first_name": "Jill", "customer_last_name": "Rivers", "cost": 64, "start_date": "2023-04-07", "end_date": "2023-09-09"},
        {"id": 11, "active": False, "subscription": "Basic", "customer_first_name": "Chelsea", "customer_last_name": "Logan", "cost": 67, "start_date": "2023-03-16", "end_date": "2023-12-07"},
        {"id": 13, "active": False, "subscription": "News", "customer_first_name": "Danielle", "customer_last_name": "Jackson", "cost": 66, "start_date": "2023-04-03", "end_date": "2024-03-25"},
        {"id": 23, "active": False, "subscription": "Ultimate", "customer_first_name": "Lori", "customer_last_name": "Sanchez", "cost": 69, "start_date": "2023-04-20", "end_date": "2023-05-08"},
        {"id": 26, "active": True, "subscription": "Family", "customer_first_name": "James", "customer_last_name": "Hall", "cost": 76, "start_date": "2023-05-07", "end_date": "2023-10-26"},
        {"id": 29, "active": True, "subscription": "Movies", "customer_first_name": "Kyle", "customer_last_name": "Crawford", "cost": 51, "start_date": "2023-03-14", "end_date": "2023-05-22"},
        {"id": 36, "active": True, "subscription": "Custom", "customer_first_name": "Jacob", "customer_last_name": "Carter", "cost": 92, "start_date": "2023-04-25", "end_date": "2024-01-28"},
    ]

    rows = convertRows(rows)
    df = spark_session.createDataFrame(rows,data_schema_registers)

    return df

@pytest.fixture(scope="session")
def test_registers_2(spark_session,data_schema_registers):
    rows = [
        {"id": 9, "active": True, "subscription": "Sports", "customer_first_name": "Cindy", "customer_last_name": "Smith", "cost": 87, "start_date": "2023-03-10", "end_date": "2023-05-31"},
        {"id": 10, "active": False, "subscription": "Custom", "customer_first_name": "Sabrina", "customer_last_name": "Welch", "cost": 51, "start_date": "2023-04-25", "end_date": "2024-01-28"},
        {"id": 11, "active": True, "subscription": "Basic", "customer_first_name": "Tanya", "customer_last_name": "Miller", "cost": 79, "start_date": "2023-04-12", "end_date": "2023-08-13"},
        {"id": 14, "active": False, "subscription": "News", "customer_first_name": "Yolanda", "customer_last_name": "Gonzalez", "cost": 56, "start_date": "2023-04-28", "end_date": "2024-01-08"},
        {"id": 15, "active": False, "subscription": "Entertainment", "customer_first_name": "Wanda", "customer_last_name": "Jones", "cost": 97, "start_date": "2023-05-07", "end_date": "2023-07-30"},
        {"id": 16, "active": True, "subscription": "News", "customer_first_name": "Jonathan", "customer_last_name": "Fowler", "cost": 81, "start_date": "2023-03-30", "end_date": "2023-07-15"},
        {"id": 18, "active": True, "subscription": "Movies", "customer_first_name": "Michael", "customer_last_name": "Mueller", "cost": 54, "start_date": "2023-04-09", "end_date": "2023-06-25"},
        {"id": 21, "active": False, "subscription": "Entertainment", "customer_first_name": "Justin", "customer_last_name": "Dixon", "cost": 78, "start_date": "2023-04-14", "end_date": "2023-07-05"},
        {"id": 22, "active": True, "subscription": "Entertainment", "customer_first_name": "Troy", "customer_last_name": "Shields", "cost": 92, "start_date": "2023-04-13", "end_date": "2023-12-05"},
        {"id": 23, "active": False, "subscription": "Family", "customer_first_name": "John", "customer_last_name": "Taylor", "cost": 91, "start_date": "2023-04-19", "end_date": "2023-08-18"}
    ]

    rows = convertRows(rows)
    df = spark_session.createDataFrame(rows,data_schema_registers)

    return df

@pytest.fixture(scope="session")
def test_registers_3(spark_session,data_schema_registers):
    rows = [
        {"id": 3, "active": False, "subscription": "News", "customer_first_name": "Lisa", "customer_last_name": "Robles", "cost": 74, "start_date": "2023-03-22", "end_date": "2024-01-11"},
        {"id": 5, "active": True, "subscription": "News", "customer_first_name": "Kevin", "customer_last_name": "Strickland", "cost": 86, "start_date": "2023-04-04", "end_date": "2024-01-19"},
        {"id": 6, "active": True, "subscription": "Premium Plus", "customer_first_name": "Randall", "customer_last_name": "Ferrell", "cost": 95, "start_date": "2023-05-04", "end_date": "2023-10-03"},
        {"id": 7, "active": True, "subscription": "Sports", "customer_first_name": "David", "customer_last_name": "Bradley", "cost": 77, "start_date": "2023-03-22", "end_date": "2023-05-17"},
        {"id": 10, "active": True, "subscription": "Premium", "customer_first_name": "Pamela", "customer_last_name": "Alvarado", "cost": 78, "start_date": "2023-03-23", "end_date": "2023-05-23"},
        {"id": 14, "active": True, "subscription": "Custom", "customer_first_name": "Laura", "customer_last_name": "Moore", "cost": 89, "start_date": "2023-05-04", "end_date": "2023-10-07"},
        {"id": 15, "active": False, "subscription": "Sports", "customer_first_name": "Timothy", "customer_last_name": "Hughes", "cost": 83, "start_date": "2023-03-27", "end_date": "2024-03-03"},
        {"id": 16, "active": False, "subscription": "Custom", "customer_first_name": "Nicholas", "customer_last_name": "Hess", "cost": 60, "start_date": "2023-05-01", "end_date": "2023-12-25"},
        {"id": 19, "active": True, "subscription": "Premium Plus", "customer_first_name": "Billy", "customer_last_name": "Murillo", "cost": 84, "start_date": "2023-04-11", "end_date": "2023-09-22"},
        {"id": 26, "active": True, "subscription": "Ultimate", "customer_first_name": "Ashley", "customer_last_name": "Hopkins", "cost": 86, "start_date": "2023-04-25", "end_date": "2023-11-08"}
    ]

    rows = convertRows(rows)
    df = spark_session.createDataFrame(rows,data_schema_registers)

    return df

#############################Snapshots
@pytest.fixture(scope="session")
def test_snapshot1(spark_session,data_schema_registers):
    rows = [
        {"id": 1, "active": False, "subscription": "Basic", "customer_first_name": "John", "customer_last_name": "Doe", "cost": 50, "start_date": "2023-04-01", "end_date": "2023-09-30"},
        {"id": 2, "active": False, "subscription": "Premium", "customer_first_name": "Jane", "customer_last_name": "Smith", "cost": 75, "start_date": "2023-03-15", "end_date": "2023-06-30"},
        {"id": 3, "active": False, "subscription": "Family", "customer_first_name": "Arthur", "customer_last_name": "Hutchinson", "cost": 53, "start_date": "2023-03-25", "end_date": "2023-06-01"},
        {"id": 4, "active": False, "subscription": "Family", "customer_first_name": "Sara", "customer_last_name": "Rodriguez", "cost": 89, "start_date": "2023-04-15", "end_date": "2023-09-25"},
        {"id": 9, "active": True, "subscription": "Sports", "customer_first_name": "Cindy", "customer_last_name": "Smith", "cost": 87, "start_date": "2023-03-10", "end_date": "2023-05-31"},
        {"id": 10, "active": False, "subscription": "Custom", "customer_first_name": "Sabrina", "customer_last_name": "Welch", "cost": 51, "start_date": "2023-04-25", "end_date": "2024-01-28"},
        {"id": 11, "active": True, "subscription": "Basic", "customer_first_name": "Tanya", "customer_last_name": "Miller", "cost": 79, "start_date": "2023-04-12", "end_date": "2023-08-13"},
        {"id": 13, "active": False, "subscription": "News", "customer_first_name": "Danielle", "customer_last_name": "Jackson", "cost": 66, "start_date": "2023-04-03", "end_date": "2024-03-25"},
        {"id": 14, "active": False, "subscription": "News", "customer_first_name": "Yolanda", "customer_last_name": "Gonzalez", "cost": 56, "start_date": "2023-04-28", "end_date": "2024-01-08"},
        {"id": 15, "active": False, "subscription": "Entertainment", "customer_first_name": "Wanda", "customer_last_name": "Jones", "cost": 97, "start_date": "2023-05-07", "end_date": "2023-07-30"},
        {"id": 16, "active": True, "subscription": "News", "customer_first_name": "Jonathan", "customer_last_name": "Fowler", "cost": 81, "start_date": "2023-03-30", "end_date": "2023-07-15"},
        {"id": 18, "active": True, "subscription": "Movies", "customer_first_name": "Michael", "customer_last_name": "Mueller", "cost": 54, "start_date": "2023-04-09", "end_date": "2023-06-25"},
        {"id": 21, "active": False, "subscription": "Entertainment", "customer_first_name": "Justin", "customer_last_name": "Dixon", "cost": 78, "start_date": "2023-04-14", "end_date": "2023-07-05"},
        {"id": 22, "active": True, "subscription": "Entertainment", "customer_first_name": "Troy", "customer_last_name": "Shields", "cost": 92, "start_date": "2023-04-13", "end_date": "2023-12-05"},
        {"id": 23, "active": False, "subscription": "Family", "customer_first_name": "John", "customer_last_name": "Taylor", "cost": 91, "start_date": "2023-04-19", "end_date": "2023-08-18"},
        {"id": 26, "active": True, "subscription": "Family", "customer_first_name": "James", "customer_last_name": "Hall", "cost": 76, "start_date": "2023-05-07", "end_date": "2023-10-26"},
        {"id": 29, "active": True, "subscription": "Movies", "customer_first_name": "Kyle", "customer_last_name": "Crawford", "cost": 51, "start_date": "2023-03-14", "end_date": "2023-05-22"},
        {"id": 36, "active": True, "subscription": "Custom", "customer_first_name": "Jacob", "customer_last_name": "Carter", "cost": 92, "start_date": "2023-04-25", "end_date": "2024-01-28"}
    ]

    rows = convertRows(rows)
    df = spark_session.createDataFrame(rows,data_schema_registers)

    return df

@pytest.fixture(scope="session")
def test_snapshot2(spark_session,data_schema_registers):
    rows = [
        {"id": 1, "active": False, "subscription": "Basic", "customer_first_name": "John", "customer_last_name": "Doe", "cost": 50, "start_date": "2023-04-01", "end_date": "2023-09-30"},
        {"id": 2, "active": False, "subscription": "Premium", "customer_first_name": "Jane", "customer_last_name": "Smith", "cost": 75, "start_date": "2023-03-15", "end_date": "2023-06-30"},
        {"id": 3, "active": False, "subscription": "News", "customer_first_name": "Lisa", "customer_last_name": "Robles", "cost": 74, "start_date": "2023-03-22", "end_date": "2024-01-11"},
        {"id": 4, "active": False, "subscription": "Family", "customer_first_name": "Sara", "customer_last_name": "Rodriguez", "cost": 89, "start_date": "2023-04-15", "end_date": "2023-09-25"},
        {"id": 5, "active": True, "subscription": "News", "customer_first_name": "Kevin", "customer_last_name": "Strickland", "cost": 86, "start_date": "2023-04-04", "end_date": "2024-01-19"},
        {"id": 6, "active": True, "subscription": "Premium Plus", "customer_first_name": "Randall", "customer_last_name": "Ferrell", "cost": 95, "start_date": "2023-05-04", "end_date": "2023-10-03"},
        {"id": 7, "active": True, "subscription": "Sports", "customer_first_name": "David", "customer_last_name": "Bradley", "cost": 77, "start_date": "2023-03-22", "end_date": "2023-05-17"},
        {"id": 9, "active": True, "subscription": "Sports", "customer_first_name": "Cindy", "customer_last_name": "Smith", "cost": 87, "start_date": "2023-03-10", "end_date": "2023-05-31"},
        {"id": 10, "active": True, "subscription": "Premium", "customer_first_name": "Pamela", "customer_last_name": "Alvarado", "cost": 78, "start_date": "2023-03-23", "end_date": "2023-05-23"},
        {"id": 11, "active": True, "subscription": "Basic", "customer_first_name": "Tanya", "customer_last_name": "Miller", "cost": 79, "start_date": "2023-04-12", "end_date": "2023-08-13"},
        {"id": 13, "active": False, "subscription": "News", "customer_first_name": "Danielle", "customer_last_name": "Jackson", "cost": 66, "start_date": "2023-04-03", "end_date": "2024-03-25"},
        {"id": 14, "active": True, "subscription": "Custom", "customer_first_name": "Laura", "customer_last_name": "Moore", "cost": 89, "start_date": "2023-05-04", "end_date": "2023-10-07"},
        {"id": 15, "active": False, "subscription": "Sports", "customer_first_name": "Timothy", "customer_last_name": "Hughes", "cost": 83, "start_date": "2023-03-27", "end_date": "2024-03-03"},
        {"id": 16, "active": False, "subscription": "Custom", "customer_first_name": "Nicholas", "customer_last_name": "Hess", "cost": 60, "start_date": "2023-05-01", "end_date": "2023-12-25"},
        {"id": 18, "active": True, "subscription": "Movies", "customer_first_name": "Michael", "customer_last_name": "Mueller", "cost": 54, "start_date": "2023-04-09", "end_date": "2023-06-25"},
        {"id": 19, "active": True, "subscription": "Premium Plus", "customer_first_name": "Billy", "customer_last_name": "Murillo", "cost": 84, "start_date": "2023-04-11", "end_date": "2023-09-22"},
        {"id": 21, "active": False, "subscription": "Entertainment", "customer_first_name": "Justin", "customer_last_name": "Dixon", "cost": 78, "start_date": "2023-04-14", "end_date": "2023-07-05"},
        {"id": 22, "active": True, "subscription": "Entertainment", "customer_first_name": "Troy", "customer_last_name": "Shields", "cost": 92, "start_date": "2023-04-13", "end_date": "2023-12-05"},
        {"id": 23, "active": False, "subscription": "Family", "customer_first_name": "John", "customer_last_name": "Taylor", "cost": 91, "start_date": "2023-04-19", "end_date": "2023-08-18"},
        {"id": 26, "active": True, "subscription": "Ultimate", "customer_first_name": "Ashley", "customer_last_name": "Hopkins", "cost": 86, "start_date": "2023-04-25", "end_date": "2023-11-08"},
        {"id": 29, "active": True, "subscription": "Movies", "customer_first_name": "Kyle", "customer_last_name": "Crawford", "cost": 51, "start_date": "2023-03-14", "end_date": "2023-05-22"},
        {"id": 36, "active": True, "subscription": "Custom", "customer_first_name": "Jacob", "customer_last_name": "Carter", "cost": 92, "start_date": "2023-04-25", "end_date": "2024-01-28"}
    ]

    rows = convertRows(rows)
    df = spark_session.createDataFrame(rows,data_schema_registers)

    return df



#############################ENRICHED DATA
@pytest.fixture(scope="session")
def test_enriched_registers_1(spark_session,data_schema_registers):
    rows = [
        {"subscription":"Basic","id":1,"active":False,"customer_first_name":"John","customer_last_name":"Doe","cost":50,"start_date":"2023-04-01","end_date":"2023-09-30","numberOfChannels":50,"extras":{}},
        {"subscription":"Basic","id":11,"active":False,"customer_first_name":"Chelsea","customer_last_name":"Logan","cost":67,"start_date":"2023-03-16","end_date":"2023-12-07","numberOfChannels":50,"extras":{}},
        {"subscription":"Custom","id":36,"active":True,"customer_first_name":"Jacob","customer_last_name":"Carter","cost":92,"start_date":"2023-04-25","end_date":"2024-01-28","numberOfChannels":150,"extras":{}},
        {"subscription":"Family","id":3,"active":False,"customer_first_name":"Arthur","customer_last_name":"Hutchinson","cost":53,"start_date":"2023-03-25","end_date":"2023-06-01","numberOfChannels":75,"extras":{"DVR":"3","Kids Package":"5"}},
        {"subscription":"Family","id":4,"active":False,"customer_first_name":"Sara","customer_last_name":"Rodriguez","cost":89,"start_date":"2023-04-15","end_date":"2023-09-25","numberOfChannels":75,"extras":{"DVR":"3","Kids Package":"5"}},
        {"subscription":"Family","id":26,"active":True,"customer_first_name":"James","customer_last_name":"Hall","cost":76,"start_date":"2023-05-07","end_date":"2023-10-26","numberOfChannels":75,"extras":{"DVR":"3","Kids Package":"5"}},
        {"subscription":"Movies","id":29,"active":True,"customer_first_name":"Kyle","customer_last_name":"Crawford","cost":51,"start_date":"2023-03-14","end_date":"2023-05-22","numberOfChannels":100,"extras":{"Cinemax":"4","HBO":"5","Showtime":"3"}},
        {"subscription":"News","id":13,"active":False,"customer_first_name":"Danielle","customer_last_name":"Jackson","cost":66,"start_date":"2023-04-03","end_date":"2024-03-25","numberOfChannels":50,"extras":{"CNN":"5","Fox News":"3"}},
        {"subscription":"Premium","id":2,"active":False,"customer_first_name":"Jane","customer_last_name":"Smith","cost":75,"start_date":"2023-03-15","end_date":"2023-06-30","numberOfChannels":100,"extras":{"HBO":"4","Cinemax":"3"}},
        {"subscription":"Premium","id":9,"active":False,"customer_first_name":"Jill","customer_last_name":"Rivers","cost":64,"start_date":"2023-04-07","end_date":"2023-09-09","numberOfChannels":100,"extras":{"HBO":"4","Cinemax":"3"}},
        {"subscription":"Ultimate","id":23,"active":False,"customer_first_name":"Lori","customer_last_name":"Sanchez","cost":69,"start_date":"2023-04-20","end_date":"2023-05-08","numberOfChannels":200,"extras":{"Cinemax":"4","HBO":"5","Showtime":"3","Sports Package":"2"}}
    ]

    rows = convertRows(rows)
    df = spark_session.createDataFrame(rows,data_schema_registers)

    return df


################################SUBSCRIPTIONS
@pytest.fixture(scope="session")
def test_subscriptions(spark_session,test_subscriptions_schema):
    subscriptions= [
        {"subscription": "Basic", "numberOfChannels": 50, "extras": {}},
        {"subscription": "Premium", "numberOfChannels": 100, "extras": {"HBO": "4", "Cinemax": "3"}},
        {"subscription": "Ultimate", "numberOfChannels": 200, "extras": {"HBO": "5", "Cinemax": "4", "Showtime": "3", "Sports Package": "2"}},
        {"subscription": "Sports", "numberOfChannels": 75, "extras": {"Sports Package": "5"}},
        {"subscription": "Entertainment", "numberOfChannels": 75, "extras": {"Showtime": "4", "Kids Package": "3"}},
        {"subscription": "News", "numberOfChannels": 50, "extras": {"CNN": "5", "Fox News": "3"}},
        {"subscription": "Movies", "numberOfChannels": 100, "extras": {"HBO": "5", "Cinemax": "4", "Showtime": "3"}},
        {"subscription": "Family", "numberOfChannels": 75, "extras": {"Kids Package": "5", "DVR": "3"}},
        {"subscription": "Premium Plus", "numberOfChannels": 200, "extras": {"HBO": "5", "Cinemax": "4", "Showtime": "4", "Sports Package": "3", "DVR": "2"}},
        {"subscription": "Custom", "numberOfChannels": 150, "extras": {}}
    ]

    df = spark_session.createDataFrame(subscriptions, test_subscriptions_schema)
    return df 

##############################DIRECTORIES
############BASE DIR
@pytest.fixture(scope="session")
def test_dir():
    tmpdir = tempfile.TemporaryDirectory() 
    return tmpdir

############SUBSCRIPTIONS
@pytest.fixture(scope="session")
def test_subscriptions_dir(test_subscriptions,test_dir):
    tmpdir = test_dir
    temp_dir = f"{tmpdir}/subscriptions"
    test_subscriptions.write.mode('overwrite').format('parquet').save(temp_dir)

    return temp_dir

############REGISTERS
@pytest.fixture(scope="session")
def test_registers_dir(test_dir,test_registers_1,test_registers_2,test_registers_3):
    tmpdir = test_dir
    temp_dir = f"{tmpdir}/registers"

    test_registers_1.coalesce(1).write.mode('overwrite').format('json').save(temp_dir+'/date=2023-04-08')
    test_registers_2.coalesce(1).write.mode('overwrite').format('json').save(temp_dir+'/date=2023-04-09')
    test_registers_3.coalesce(1).write.mode('overwrite').format('json').save(temp_dir+'/date=2023-04-10')

    return temp_dir

############SNAPSHOTS
@pytest.fixture(scope="session")
def test_snapshots_dir(test_dir,test_snapshot1,test_snapshot2):
    tmpdir = test_dir
    temp_dir = f"{tmpdir}/snapshots"

    test_snapshot1.write.mode('overwrite').format('parquet').save(temp_dir+'/date=2023-04-09')
    test_snapshot2.write.mode('overwrite').format('parquet').save(temp_dir+'/date=2023-04-10')

    return temp_dir

def test_getMaxDateSnapshots(spark_session, test_snapshots_dir):
    MaxDateSnapshots = getMaxDateSnapshots(test_snapshots_dir,spark_session)

    assert MaxDateSnapshots == datetime.strptime('2023-04-10', '%Y-%m-%d').date()

def test_readMaxDateSnapshoots(test_snapshots_dir,spark_session,test_snapshot2):
    snapshots = readMaxDateSnapshoots(test_snapshots_dir,datetime.strptime('2023-04-10', '%Y-%m-%d').date() , spark_session)

    assert snapshots.collect() == test_snapshot2.collect()

def test_getSubscriptions(spark_session, test_subscriptions_dir,test_subscriptions_schema):
    subscriptions = getSubscriptions(test_subscriptions_dir,spark_session)
    
    assert subscriptions.count() == 10
    assert subscriptions.schema == test_subscriptions_schema
    
def test_getMaxDateRegisters(spark_session, test_registers_dir, data_schema_retrieve_registers):
    dateData, registers = getMaxDateRegisters(test_registers_dir,spark_session)

    assert dateData == datetime.strptime('2023-04-10', '%Y-%m-%d').date()
    assert registers.schema == data_schema_retrieve_registers
    


def test_getSubscriptions2(spark_session,test_registers_1,test_subscriptions):
    
    joined = test_registers_1.join(test_subscriptions, ['subscription'])
    joined.show(n=1000, truncate=False)

    joined.coalesce(1).write.mode('overwrite').mode('overwrite').format('json').save('data/files/test2')
    
    pass

