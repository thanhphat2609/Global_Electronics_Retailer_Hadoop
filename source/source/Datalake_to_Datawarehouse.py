# Lib for read Argument from execute pipeline
from pyspark.sql import SparkSession

# Import modules
from modules.Extraction import *
from modules.Load import *
from modules.Transformation import *
from modules.DataLog import *
from modules.HDFSUtils import *
from modules.Validate import *

import traceback
import great_expectations as gx
import pyspark.sql.functions as f
import pyspark.sql.types as t


# Create SparkSession
spark = SparkSession.builder.appName("Load to Hive") \
                            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/warehouse").enableHiveSupport() \
                            .config("hive.exec.dynamic.partition", "true") \
	                        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
                            .config("spark.driver.memory", "4g") \
                            .config("spark.sql.parquet.vorder.enabled", "true") \
                            .config("spark.sql.shuffle.partitions", 100) \
                            .config("spark.dynamicAllocation.enabled", "true") \
                            .config("spark.executor.cores", 4) \
                            .config("spark.dynamicAllocation.minExecutors","1") \
                            .config("spark.dynamicAllocation.maxExecutors","5") \
                            .getOrCreate()

# Instance of modules
loadHive = Load()
extraction = Extraction()
transformatiom = Transformation()
hdfsUtils = HDFSUtils()
dataLogger = DataLog()
validateData = ValidateData()

# Instance of great_expectations
context = gx.get_context()


# Receive argument
executionDate = spark.sql("SELECT CURRENT_DATE() as current_date_run").collect()[0][0]
# Partition data by Arguments
parse_execution = executionDate.split("-")
year = parse_execution[0]
month = parse_execution[1]
day = parse_execution[2]


# Define param for warehouse, project
dbHiveName = "wh_global_electronics_retailer"
project = "Global_Electronics_Retailer"


# Define bath for saving data to hdfs
log_path = "hdfs://localhost:9000/log/"

# Define for log pipeline
batch_id = hdfsUtils.check_batch_run(executionDate) - 1
pipeline_job = "datalake_to_datawarehouse"
start_time = ""
end_time = ""
error = ""
status = ""
source_row_read = 0
numInserted = 0
numUpdated = 0
columnNull = ""
columnMissing = ""

# Column check
col_check_match = None
col_check_null = ""


# # Create new database
# loadHive.create_db_hive(spark, dbHiveName)

# Check read all table
tblNames = ["customers", "sales", "products", "stores", "exchange_rates"]

for tblName in tblNames:

    # Start time for check
    start_time = spark.sql(''' SELECT CURRENT_TIMESTAMP() as current_time ''') \
                        .collect()[0]["current_time"].strftime('%Y-%m-%d %H:%M:%S')

    try:

        new_version_path = hdfsUtils.get_new_version(executionDate, project, tblName)
        validator = context.sources.pandas_default.read_parquet(new_version_path)

        # Check task
            # Task = 6, Customer
        if tblName == "customers":
            col_check_match = ["CustomerKey", "Gender", "Name", "City", "State_Code", 
                               "State", "Zip_Code", "Country", "Continent", "Birthday",
                               "year", "month", "day"]
            col_check_null = "CustomerKey"

            columnMissing = validateData.check_schema(validator, col_check_match)
            columnNull = validateData.check_null(validator, col_check_null)
        
            # Task = 7, Stores
        elif tblName == "stores":
            col_check_match = ["StoreKey", "Country", "State", "Square_Meters", "Open_Date",
                               "year", "month", "day"]
            col_check_null = "StoreKey"

            columnMissing = validateData.check_schema(validator, col_check_match)
            columnNull = validateData.check_null(validator, col_check_null)
            
            # Task = 8, Products
        elif tblName == "products":
            col_check_match = ["ProductKey", "Product_Name", "Brand", "Color", 
                               "Unit_Cost_USD", "Unit_Price_USD", \
                               "SubcategoryKey", "Subcategory", "CategoryKey", "Category",
                               "year", "month", "day"]
            col_check_null = "ProductKey"

            columnMissing = validateData.check_schema(validator, col_check_match)
            columnNull = validateData.check_null(validator, col_check_null)

            # Task = 9, Sales
        elif tblName == "sales":
            col_check_match = ["Order_Number", "Line_Item", "Order_Date", "Delivery_Date", 
                               "CustomerKey", "StoreKey",
                               "ProductKey", "Quantity", "Currency_Code",
                               "year", "month", "day"]
            col_check_null = "Order_Number"

            columnMissing = validateData.check_schema(validator, col_check_match)
            columnNull = validateData.check_null(validator, col_check_null)
            
            # Task = 10, Exchange_Rates
        elif tblName == "exchange_rates":
            col_check_match = ["Date", "Currency", "Exchange", 
                               "year", "month", "day"]
            col_check_null = "Currency"

            columnMissing = validateData.check_schema(validator, col_check_match)
            columnNull = validateData.check_null(validator, col_check_null)

        else:
            pass

        # Check error 
        if (columnMissing != "") or (columnNull != ""): 
            error = "Missing column or Have null value in data"
            status = "Failed by Missed column or Have null KEY value"
        else:

            error = ""
            status = "Success"

            # print("Path: ", new_version_path)

            df = spark.read.format("parquet").load(new_version_path)
            # df.show()

            # Set numInserted
            source_row_read = df.count()

            # Check exist table and update or insert or overwrite
            check_table_exists = loadHive.check_exist_table(spark, dbHiveName, tblName)
            if check_table_exists == 0: # Not exist
                if tblName != "sales":
                    dim_table = f"dim_{tblName}"
                    loadHive.save_hive_table(df, dbHiveName, dim_table)                    

                else:
                    fact_table = f"fact_{tblName}"
                    loadHive.save_hive_table(df, dbHiveName, fact_table)
                
                numInserted = df.count()

            # Check upsert data
            else: # Exists
                if tblName != "sales":
                    dim_table = f"dim_{tblName}"
                    transformatiom.merge(spark, df, dbHiveName, dim_table) 
                                     

                else:
                    fact_table = f"fact_{tblName}"
                    transformatiom.merge(spark, df, dbHiveName, fact_table)


                numInserted = transformatiom.get_insert_count()
                numUpdated = transformatiom.get_update_count()  
    
    except:
        error = traceback.format_exc()
        status = "Failed"
        print("Error: ", error)


    # End time for check
    end_time = spark.sql(''' SELECT CURRENT_TIMESTAMP() as current_time ''') \
                        .collect()[0]["current_time"].strftime('%Y-%m-%d %H:%M:%S')
    
    df_log = dataLogger.log_data(batch_id, pipeline_job, dbHiveName, tblName,
                 start_time, end_time, source_row_read, numInserted, numUpdated, columnMissing, 
                 columnNull, error, status, t, spark)

    df_log.write.mode("append").format("parquet").save(f"{log_path}/{project}/{executionDate}/batch_{batch_id}/")

