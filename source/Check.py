from pyspark.sql import SparkSession


# Create SparkSession
spark = SparkSession.builder.appName("Mysql - to -  HDFS") \
                            .config("spark.driver.memory", "4g") \
                            .config("spark.sql.parquet.vorder.enabled", "true") \
                            .config("spark.sql.shuffle.partitions", 100) \
                            .getOrCreate()


# Define parameter
database = "Global_Electronics_Retailer"
dbName = f"jdbc:mysql://localhost:3306/{database}"
tbName = "customers"
driver = "com.mysql.jdbc.Driver"
username = "root"
password = "password"

df = spark.read.format("jdbc") \
                        .option("driver", driver) \
                        .option("url", dbName) \
                        .option("dbtable", tbName) \
                        .option("user", username) \
                        .option("password", password).load()

df.show()
