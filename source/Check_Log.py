# Lib for read Argument from execute pipeline
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("Mysql - to -  HDFS") \
                            .config("spark.driver.memory", "40g") \
                            .config("spark.executor.memory", "40g") \
                            .config("spark.sql.parquet.vorder.enabled", "true") \
                            .getOrCreate()

# # Define bath for saving data to hdfs
# log_path = "hdfs://localhost:9000/log/Global_Electronics_Retailer/2024-06-03/batch_2"

# df = spark.read.format("parquet").load(f"{log_path}/*.parquet")
# df.show()

# error = df.select("Error").collect()[0][0]

import os
import re

project = "Global_Electronics_Retailer"
tblName = "customers"
year = "2024"
month = "05"
day = "26"
