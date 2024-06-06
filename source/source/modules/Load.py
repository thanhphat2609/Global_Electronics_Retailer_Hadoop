class Load:

    def __init__(self) -> None:
        pass
    

    # Function for check exits table
    def check_exist_table(self, spark, dbName, tablename):
        """
            Function for check exist table
        
            - Args:
                spark: SparkSession
                dbName: Name of database
                tablename: Table for check exist
            
            - Return:
                check_exist
                    0: Not
                    1: Yes
        """
        try:
            df = spark.sql(f""" SELECT COUNT(*) FROM {dbName}.{tablename};""")
            df.show()
        except:
            check_exist = 0 # Not Exist
        else:
            check_exist = 1 # Exist
        
        return check_exist
    

     # Function for create table in Hive Warehouse
    def create_db_hive(self, spark, dbHiveName):
        """
            Function for create database in hive
        
            - Args:
                spark: SparkSession
                dbHiveName: Name of database
            
            - Return:
                None
        """

        spark.sql(f"CREATE DATABASE IF NOT EXISTS {dbHiveName}")  


    # Function for save df to hive table
    def save_hive_table(self, df, db_hive_name, table_name):
        """
            Function for save dataframe as Hive Table (Just for inital)

            - Args:
                df: Dataframe
                table_name: Table name to save as

            - Return:
                None
        
        """
        df.write.partitionBy("year", "month", "day").mode('overwrite') \
                .saveAsTable(f"{db_hive_name}.{table_name}")







