# Lib for read Argument from execute pipeline
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName("Load to Hive") \
                            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/warehouse").enableHiveSupport() \
                            .config("hive.exec.dynamic.partition", "true") \
	                        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                            .config("spark.driver.memory", "40g") \
                            .config("spark.executor.memory", "40g") \
                            .config("spark.sql.parquet.vorder.enabled", "true") \
                            .getOrCreate()

dbName = "test"
tablename = "customer"
