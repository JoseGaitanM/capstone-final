from pyspark.sql import SparkSession
from datetime import date, timedelta
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType

schemaSubscriptions = StructType([
  StructField("subscription", StringType(), False),
  StructField("numberOfChannels", LongType(), False),
  StructField("extras", MapType(StringType(), StringType(), valueContainsNull = True), nullable = True)
])

spark = SparkSession.builder.master("local[1]") \
    .appName("Create snapshot") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

df2 = spark.read.parquet("/opt/airflow/data/files/subscriptions")
df2.show(n=1000, truncate=False)

df1 = spark.read.option("multiline","true").json("/opt/airflow/data/files/registers")
df1.show(n=1000, truncate=False)

distinct_dates = df1.select("date").distinct()
distinct_dates.show()

joined = df1.join(df2, ['subscription'])
joined.show(n=1000, truncate=False)
joined.write.mode("overwrite").parquet(f'/opt/airflow/data/files/snapshoots/date={date.today().strftime("%Y-%m-%d")}')

df3 = spark.read.parquet(f'/opt/airflow/data/files/snapshoots/date={date.today().strftime("%Y-%m-%d")}')
df3.show(n=1000, truncate=False)