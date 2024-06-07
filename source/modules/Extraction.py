class Extraction:

    def __init__(self) -> None:
        pass

    # Partition data read
    def create_year_month_day(self, df, executionDate, f):
        """
            Function for create year, month, day

            - Args:
                df: Dataframe
                executionDate: Execution Date
                f: pyspark.sql.functions
            
            - Returns:
                df with year, month, day column
        
        """
        executionDate = executionDate.split("-")

        # Partition data by Arguments
        year = executionDate[0]
        month = executionDate[1]
        day = executionDate[2]

        # Create new column for partition
        df = df.withColumn("year", f.lit(year)) \
               .withColumn("month", f.lit(month)) \
               .withColumn("day", f.lit(day))
        return df

    # Function for read table in MySQL into dataframe
    def read_table_mysql(self, spark, driver, dbName, tbName, username, password):
        """
            Read table from MySQL Database
        
        
            - Args:
                spark: SparkSession.
                driver: Driver connection to MySQL. 
                dbName: Database name from MySQL.
                tbName: Table name from MySQL.
                username: Username of account to login MySQL.
                password: Password.
            
            - Return:
                df: Dataframe
                error: Error when try to connection
        """

        df = spark.read.format("jdbc") \
                        .option("driver", driver) \
                        .option("url", dbName) \
                        .option("dbtable", tbName) \
                        .option("user", username) \
                        .option("password", password).load()
    
        return df


    # Function for read data from mysql by using query
    def read_by_query_mysql(slef, spark, driver, dbname, query, username, password):
        """
            Read table from MySQL Database
        
        
            - Args:
                spark: SparkSession.
                driver: Driver connection to MySQL.
                dbName: Database from MySQL.
                tbName: Table name from MySQL.
                username: Username of account.
                password: Password of account.
            
            - Return:
                df: Dataframe
                error: Error when try to connection
        """

        df = spark.read.format("jdbc") \
                        .option("driver", driver) \
                        .option("url", dbname) \
                        .option("dbtable", query) \
                        .option("user", username) \
                        .option("password", password).load()
    
        return df
        