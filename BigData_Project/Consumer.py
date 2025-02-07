import threading
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, MapType

stop_event=threading.Event()

def kafkaConsumer():
    spark = SparkSession.builder \
            .appName('kafkaconsumer')\
            .getOrCreate()

    topic='StockData'
    data=spark.readStream\
            .format('kafka')\
            .option('kafka.bootstrap.servers','localhost:9092')\
            .option('subscribe',topic)\
            .option('failOnDataLoss','false')\
            .option('startingOffsets','latest')\
            .load()
    schema = StructType([
        StructField("Meta Data", StructType([
            StructField("1. Information", StringType(), True),
            StructField("2. Symbol", StringType(), True),
            StructField("3. Last Refreshed", StringType(), True),
            StructField("4. Output Size", StringType(), True),
            StructField("5. Time Zone", StringType(), True)
        ]), True),
        StructField("Time Series (Daily)", MapType(
            StringType(),  # Date
            StructType([
                StructField("1. open", StringType(), True),
                StructField("2. high", StringType(), True),
                StructField("3. low", StringType(), True),
                StructField("4. close", StringType(), True),
                StructField("5. volume", StringType(), True)
            ]), True
        ), True)
    ])

    json_data=data.selectExpr("CAST(value AS STRING) AS json")
    data1=json_data.select(from_json(col('json'),schema).alias('parsed')).select('parsed')
    data1_coal=data1.coalesce(1)

    query = data1_coal.writeStream\
            .outputMode("append")\
            .format("json")\
            .option('path','/home/sunbeam/Desktop/BigData_Project/Extra')\
            .option('checkpointLocation','/home/sunbeam/Desktop/BigData_Project/Extra')\
            .trigger(processingTime='5 seconds')\
            .start()

    print(query)

    try:
        query.awaitTermination(timeout=300)
    except Exception as e:
        print(e)
    print("Consumer stopped")
kafkaConsumer()
