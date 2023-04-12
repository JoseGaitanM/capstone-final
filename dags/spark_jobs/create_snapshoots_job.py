from pyspark.sql import SparkSession
from datetime import date, timedelta
from pyspark.sql.functions import max, col, lit, row_number
import os
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local[1]") \
    .appName("Create snapshot") \
    .getOrCreate()
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

def getMaxDateSnapshots(path):
  dfsnapshots = spark.read.parquet(path)
  dfsnapshots.show(n=1000, truncate=False)

  datessnapshots = dfsnapshots.select("date").distinct()
  datessnapshots.show()

  maxDatesnapshots = datessnapshots.agg(max('date')).collect()[0][0]
  print('Max date snapshots: ',maxDatesnapshots)

  return maxDatesnapshots

def readMaxDateSnapshoots(path,maxDatesnapshots):
  snapshoot = spark.read.parquet(f'{path}/date={maxDatesnapshots.strftime("%Y-%m-%d")}')
  snapshoot.show(n=1000, truncate=False)
  snapshoot.drop('date')

  return snapshoot

def getSubscriptions(path):
  subscriptions = spark.read.parquet(path)
  subscriptions.show(n=1000, truncate=False)

  return subscriptions


def getMaxDateRegisters(path):
  registers = spark.read.option("multiline","true").json(path)
  registers.show(n=1000, truncate=False)

  distinct_dates = registers.select("date").distinct()
  distinct_dates.show()

  dateData = distinct_dates.agg(max('date')).collect()[0][0]
  print('Date data json: ', dateData)

  return (dateData, registers)

def enrichNewRegisters(subscriptions):
  registers = spark.read.option("multiline","true").json(f'/opt/airflow/data/files/registers')
  registers[registers['date'] > maxDatesnapshots]
  registers.show(n=1000, truncate=False)

  joined = registers.join(subscriptions, ['subscription'])
  joined.show(n=1000, truncate=False)

  joined.printSchema()
  snapshoot.printSchema()

  return joined


def createNewSnapshot(joined,dateData):
  latestData = (joined
                .withColumn("rowNumber", row_number().over(Window.partitionBy(col("id")).orderBy(col("date").desc())))
                .where(col("rowNumber") == lit(1))
                .drop("rowNumber"))
  
  latestData.show(n=1000, truncate=False)
  latestData.write.mode("overwrite").parquet(f'/opt/airflow/data/files/snapshots/date={dateData.strftime("%Y-%m-%d")}')

  return latestData

def enrichData(registers,subscription):

  joined = registers.join(subscription, ['subscription'])
  joined.show(n=1000, truncate=False)

  return joined

snapshotsPath = "/opt/airflow/data/files/snapshots"
subscriptionsPath = "/opt/airflow/data/files/subscriptions"
registersPath = "/opt/airflow/data/files/registers"


if os.listdir("/opt/airflow/data/files/snapshots"):

  ###get max date snapshots
  maxDatesnapshots = getMaxDateSnapshots(snapshotsPath)

  ###read max snapshots
  snapshoot = readMaxDateSnapshoots(snapshotsPath,maxDatesnapshots)

  if os.listdir("/opt/airflow/data/files/registers"):

    #####get subscriptions
    subscriptions = getSubscriptions(subscriptionsPath)

    ###get max date registers
    dateData,_ = getMaxDateRegisters(registersPath)

    if (dateData > maxDatesnapshots):
      ###enrich new registers
      joined = enrichNewRegisters(subscriptions)

      ###create new snapshoot
      latestData = createNewSnapshot(joined,dateData)
    else:
      print('snapshots Up to date')

  else:
    print('ERROR: No data .json')

else:
   
   if os.listdir("/opt/airflow/data/files/registers"):

    #####get subscriptions
    subscriptions = getSubscriptions(subscriptionsPath)

    ###get max date registers
    dateData,registers = getMaxDateRegisters(registersPath)

    ##enrich data
    joined = enrichData(registers,subscriptions)

    ###create new snapshoot
    latestData = createNewSnapshot(joined,dateData)