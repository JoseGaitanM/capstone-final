from pyspark.sql import SparkSession
from datetime import date, timedelta
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType
from pyspark.sql.functions import max, min, col, lit, row_number
import os
from pyspark.sql.window import Window
from pyspark.sql.functions import input_file_name, regexp_extract

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
  snapshoot.drop('date')

  if os.listdir("/opt/airflow/data/files/registers"):

    df2 = spark.read.parquet("/opt/airflow/data/files/subscriptions")
    df2.show(n=1000, truncate=False)

    df1 = spark.read.option("multiline","true").json("/opt/airflow/data/files/registers")
    df1.show(n=1000, truncate=False)

    distinct_dates = df1.select("date").distinct()
    distinct_dates.show()

    dateData = distinct_dates.agg(max('date')).collect()[0][0]
    print('Date data json: ', dateData)

    if (dateData > maxDatesnapshots):

      data = spark.read.option("multiline","true").json(f'/opt/airflow/data/files/registers')
      data[data['date'] > maxDatesnapshots]
      data.show(n=1000, truncate=False)

      joined = data.join(df2, ['subscription'])
      joined.show(n=1000, truncate=False)

      joined.printSchema()
      snapshoot.printSchema()

      union = joined.union(snapshoot)
      union.show(n=1000, truncate=False)

      latestData = (joined
                     .withColumn("rowNumber", row_number().over(Window.partitionBy(col("id")).orderBy(col("date").desc())))
                     .where(col("rowNumber") == lit(1))
                     .drop("rowNumber"))
      
      latestData.show(n=1000, truncate=False)
      latestData.write.mode("overwrite").parquet(f'/opt/airflow/data/files/snapshots/date={dateData.strftime("%Y-%m-%d")}')

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

    dateData = distinct_dates.agg(max('date')).collect()[0][0]
    print('Date data json: ', dateData)

    joined = df1.join(df2, ['subscription'])
    joined.show(n=1000, truncate=False)

    latestData = (joined
                     .withColumn("rowNumber", row_number().over(Window.partitionBy(col("id")).orderBy(col("date").desc())))
                     .where(col("rowNumber") == lit(1))
                     .drop("rowNumber"))
    
    latestData.show(n=1000, truncate=False)
    latestData.write.mode("overwrite").parquet(f'/opt/airflow/data/files/snapshots/date={dateData.strftime("%Y-%m-%d")}/')