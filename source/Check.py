from pyspark.sql import SparkSession
import great_expectations as gx

from modules.HDFSUtils import *
hdfsUtils = HDFSUtils()

# Create SparkSession
spark = SparkSession.builder.appName("Mysql - to -  HDFS") \
                            .config("spark.driver.memory", "4g") \
                            .config("spark.sql.parquet.vorder.enabled", "true") \
                            .config("spark.sql.shuffle.partitions", 100) \
                            .getOrCreate()

path = "hdfs://localhost:9000/log/Global_Electronics_Retailer/2024-06-09/batch_23/*.parquet"

df = spark.read.format("parquet").load(path)
df.select("Error").show(truncate=False)

