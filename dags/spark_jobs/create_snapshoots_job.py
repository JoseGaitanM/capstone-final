
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

##Case snapshots exists
if os.listdir("/opt/airflow/data/files/snapshots"):

  dfsnapshots = spark.read.parquet("/opt/airflow/data/files/snapshots")
  dfsnapshots.show(n=1000, truncate=False)

  datessnapshots = dfsnapshots.select("date").distinct()
  datessnapshots.show()

  maxDatesnapshots = datessnapshots.agg(max('date')).collect()[0][0]
  print('Max date snapshots: ',maxDatesnapshots)

  snapshoot = spark.read.parquet(f'/opt/airflow/data/files/snapshots/date={maxDatesnapshots.strftime("%Y-%m-%d")}')
  snapshoot.show(n=1000, truncate=False)

  if os.listdir("/opt/airflow/data/files/registers"):

    df2 = spark.read.parquet("/opt/airflow/data/files/subscriptions")
    df2.show(n=1000, truncate=False)

    df1 = spark.read.option("multiline","true").json("/opt/airflow/data/files/registers")
    df1.show(n=1000, truncate=False)

    distinct_dates = df1.select("date").distinct()
    distinct_dates.show()

    DataData = maxDatesnapshots + timedelta(days=1)
    print('Date for data json: ', DataData)

    if (os.path.isdir(f'/opt/airflow/data/files/registers/date={DataData.strftime("%Y-%m-%d")}') and DataData > maxDatesnapshots):

      data = spark.read.option("multiline","true").json(f'/opt/airflow/data/files/registers/date={DataData.strftime("%Y-%m-%d")}')

      joined = data.join(df2, ['subscription'])
      joined.show(n=1000, truncate=False)

      union = joined.unionByName(snapshoot).dropDuplicates(['id']).orderBy('id')
      union.show(n=1000, truncate=False) 
      
      union.write.mode("overwrite").parquet(f'/opt/airflow/data/files/snapshots/date={DataData.strftime("%Y-%m-%d")}')

    else:
      print('snapshots Up to date')

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
    joined.write.mode("overwrite").parquet(f'/opt/airflow/data/files/snapshots/date={minDateData.strftime("%Y-%m-%d")}')