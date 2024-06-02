# Lib for read Argument from execute pipeline
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t

import traceback

# Import modules
from modules.Extraction import *
from modules.DataLog import *
from modules.HDFSUtils import *

# Create SparkSession
spark = SparkSession.builder.appName("Mysql - to -  HDFS") \
                            .config("spark.driver.memory", "40g") \
                            .config("spark.executor.memory", "40g") \
                            .config("spark.sql.parquet.vorder.enabled", "true") \
                            .getOrCreate()

# Receive argument
executionDate = sys.argv[1]
# Partition data by Arguments
parse_execution = executionDate.split("-")
year = parse_execution[0]
month = parse_execution[1]
day = parse_execution[2]


# Define bath for saving data to hdfs
log_path = "hdfs://localhost:9000/log/"
hdfs_path = "hdfs://localhost:9000/datalake/"
project = "Global_Electronics_Retailer"
base_path = f"{hdfs_path}/{project}"

# Instance of modules
extraction = Extraction()
hdfsUtils = HDFSUtils()
dataLogger = DataLog()

# Define for log pipeline
batch_run = hdfsUtils.check_batch_run(executionDate)
start_time = ""
end_time = ""
error = ""
status = ""
source_row_read = 0
numInserted = 0
numUpdated = 0
pipeline_job = "source_to_datalake"


# Define parameter
database = "Global_Electronics_Retailer"
dbname = f"jdbc:mysql://localhost:3306/{database}"
driver = "com.mysql.jdbc.Driver"
username = "root"
password = "password"


# Read from source query for checking datasource
check_souce = "Source_Check"
df_check_source = extraction.read_table_mysql(spark, driver, dbname, check_souce, username, password)

# Check read table
# tblNames = "customers"
# df_customer = extraction.read_table_mysql(spark, driver, dbname, tblNames, username, password)
# df_customer.show()

# Check read all table
tblNames = ["customers", "sales", "products", "stores", "exchange_rates"]

# # Read all table
for tblName in tblNames:
    # Start time for check
    start_time = spark.sql(''' SELECT CURRENT_TIMESTAMP() as current_time ''') \
                        .collect()[0]["current_time"].strftime('%Y-%m-%d %H:%M:%S')
    try:
        # Read data
        df = extraction.read_table_mysql(spark, driver, dbname, tblName, username, password)
        

        # Validate data
        source_row_read = df_check_source.filter(f.col("table_name") == f"{tblName}").select("source_row_read").collect()[0][0]
        numInserted = df.count()

        # Create new column for partition
        df = extraction.create_year_month_day(df, executionDate, f)
        
        # Display df
        # df.show()

        # Write data to HDFS
        code = hdfsUtils.check_exist_data(executionDate, project, tblName)
        # Exist file
        if code == 0: # Yes => Append for version data
            df.write.mode("append").format("parquet") \
                    .save(f"{base_path}/{tblName}/year={year}/month={month}/day={day}/{tblName}_{year}_{month}_{day}-version_{batch_run}.parquet")
        else: # No => First run
            df.write.mode("overwrite").format("parquet") \
                    .save(f"{base_path}/{tblName}/year={year}/month={month}/day={day}/{tblName}_{year}_{month}_{day}-version_{batch_run}.parquet")
    
    except:
        error = traceback.format_exc()
        status = "Failed"

    else:
        error = ""
        status = "Success"
    
    # End time for check
    end_time = spark.sql(''' SELECT CURRENT_TIMESTAMP() as current_time ''') \
                        .collect()[0]["current_time"].strftime('%Y-%m-%d %H:%M:%S')

    # Check status
    # print("Tablename: ", tblName, "Error: ", error, "Status: ", status, 
    #       "Source rows: ", source_row_read, "Num of rows Inserted: ", numInserted)


    df_log = dataLogger.log_data(batch_run, pipeline_job, database, tblName,
                 start_time, end_time, source_row_read, numInserted, numUpdated, "", 
                 "", error, status, t, spark)

    df_log.write.mode("append").format("parquet").save(f"{log_path}/{project}/{executionDate}/batch_{batch_run}/")

