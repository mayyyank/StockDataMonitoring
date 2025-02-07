from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import json
import os


def sparkHive():

    spark = SparkSession.builder.appName('SparkToHive')\
            .config('spark.sql.catalogImplementation','hive')\
            .config('hive.metastore.uris','thrift://0.0.0.0:9083') \
            .enableHiveSupport() \
            .getOrCreate()
    csv_path="/home/sunbeam/Desktop/BigData_Project/Extra/DataFrame/stock.csv"

    if os.path.exists(csv_path):
        df1=spark.read.option('inferSchema','True').option('header','True').csv(csv_path)
    else:
        schema=StructType([
            StructField('Company',StringType(),True),
            StructField('Date',StringType(),True),
            StructField('Open',FloatType(),True),
            StructField('High',FloatType(),True),
            StructField('Low',FloatType(),True),
            StructField('Close',FloatType(),True),
            StructField('Volume',IntegerType(),True)
        ])
        df1=spark.createDataFrame([],schema)

    json_path='/home/sunbeam/Desktop/BigData_Project/Extra/MergedStockData.json'
    with open(json_path,'r') as file:
        for line in file:
            data=json.loads(line)
            company_name=data['parsed']['Meta Data']['2. Symbol']
            time_series=data['parsed']['Time Series (Daily)']
            rows=[]
            for date,values in time_series.items():
                row = {
                    'Company': company_name,
                    'Date': date,
                    'Open': float(values["1. open"]),
                    'High': float(values["2. high"]),
                    'Low': float(values["3. low"]),
                    'Close': float(values["4. close"]),
                    'Volume': int(values["5. volume"])
                }
                rows.append(row)
            df=spark.createDataFrame(rows)
            df=df.select('Company','Date','Open','High','Low','Close','Volume')
            df1=df1.union(df).dropDuplicates(['Company','Date'])


    df2=df1.where("Date LIKE '____-__-__'")
    df2=df2.sort('Company','Date')
    df2.show(df2.count(),truncate=False)

    df2.write.option('header','true')\
        .mode('append')\
        .csv(csv_path)

    spark.sql('use stockdata')

    df2.write.mode('append').saveAsTable('stockdata.StockDetails')
    print("Saved to Hive")

    spark.stop()

sparkHive()