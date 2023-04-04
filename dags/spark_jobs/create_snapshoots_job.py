
from pyspark.sql import SparkSession
from datetime import date, timedelta
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType
from pyspark.sql.functions import max, min
import os

schemaSubscriptions = StructType([
  StructField("subscription", StringType(), False),
  StructField("numberOfChannels", LongType(), False),
  StructField("extras", MapType(StringType(), StringType(), valueContainsNull = True), nullable = True)
])

spark = SparkSession.builder.master("local[1]") \
    .appName("Create snapshot") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

##Case snapshoots exists
if os.listdir("/opt/airflow/data/files/snapshoots"):

  dfSnapshoots = spark.read.parquet("/opt/airflow/data/files/snapshoots")
  dfSnapshoots.show(n=1000, truncate=False)

  datesSnapshoots = dfSnapshoots.select("date").distinct()
  datesSnapshoots.show()

  maxDateSnapshoots = datesSnapshoots.agg(max('date')).collect()[0][0]
  print('Max date snapshoots: ',maxDateSnapshoots)

  snapshoot = spark.read.parquet(f'/opt/airflow/data/files/snapshoots/date={maxDateSnapshoots.strftime("%Y-%m-%d")}')
  snapshoot.show(n=1000, truncate=False)

  if os.listdir("/opt/airflow/data/files/registers"):

    df2 = spark.read.parquet("/opt/airflow/data/files/subscriptions")
    df2.show(n=1000, truncate=False)

    df1 = spark.read.option("multiline","true").json("/opt/airflow/data/files/registers")
    df1.show(n=1000, truncate=False)

    distinct_dates = df1.select("date").distinct()
    distinct_dates.show()

    DataData = maxDateSnapshoots + timedelta(days=1)
    print('Date for data json: ', DataData)

    if (os.path.isdir(f'/opt/airflow/data/files/registers/date={DataData.strftime("%Y-%m-%d")}') and DataData > maxDateSnapshoots):

      data = spark.read.option("multiline","true").json(f'/opt/airflow/data/files/registers/date={DataData.strftime("%Y-%m-%d")}')

      joined = data.join(df2, ['subscription'])
      joined.show(n=1000, truncate=False)

      union = joined.unionByName(snapshoot).dropDuplicates(['id']).orderBy('id')
      union.show(n=1000, truncate=False) 
      
      union.write.mode("overwrite").parquet(f'/opt/airflow/data/files/snapshoots/date={DataData.strftime("%Y-%m-%d")}')

    else:
      print('Snapshoots Up to date')

  else:
    print('ERROR: No data .json')

else:

   if os.listdir("/opt/airflow/data/files/registers"):

    df2 = spark.read.parquet("/opt/airflow/data/files/subscriptions")
    df2.show(n=1000, truncate=False)

    df1 = spark.read.option("multiline","true").json("/opt/airflow/data/files/registers")
    df1.show(n=1000, truncate=False)

    distinct_dates = df1.select("date").distinct()
    distinct_dates.show()

    minDateData = distinct_dates.agg(min('date')).collect()[0][0]
    print('Min date data json: ', minDateData)

    data = spark.read.option("multiline","true").json(f'/opt/airflow/data/files/registers/date={minDateData.strftime("%Y-%m-%d")}')
    data.show(n=1000, truncate=False)

    joined = data.join(df2, ['subscription'])
    joined.show(n=1000, truncate=False)
    joined.write.mode("overwrite").parquet(f'/opt/airflow/data/files/snapshoots/date={minDateData.strftime("%Y-%m-%d")}')