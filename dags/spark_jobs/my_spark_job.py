
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Create snapshot") \
    .getOrCreate()

# Read Parquet file into a DataFrame
df2 = spark.read.parquet("/opt/airflow/data/files/subscription/subscriptions.parquet")
df2.show()

# Read JSON file into a DataFrame
df1 = spark.read.option("multiline","true").json("/opt/airflow/data/files/data.json")
df1.show()

joined = df1.join(df2, ['subscription'])

# Display the result
joined.show()
