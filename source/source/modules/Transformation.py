class Transformation:

    def __init__(self) -> None:
        self.numInserted = 0
        self.numUpdated = 0
        pass

    # Check data update or insert
    def check_update(self, spark, df_old, df_new):
        
        pass


    def check_insert(self, df_old, df_new, col_key):
        """
            Function for check new records

            - Args:
                df_old: Dataframe old from hive
                df_new: Dataframe new from datalake
                col_key: Key column

            - Return:
                None
        
        """
        df_insert = df_new.join(df_old, on = col_key, how = "left")

        return df_insert

    def merge(self, spark, df_new, db_hive_name, tblName, col_key):
        """
            Function for upsert data

            - Args:
                df_new: Dataframe new from datalake
                db_hive_name: Warehouse
                tbl_Name: table name
                col_key: Key column


            - Return:
                None
        
        """
        # Read
        df_old = spark.sql(f"SELECT * FROM {db_hive_name}.{tblName}")

        # Get df_insert and numInserted
        df_insert = self.check_insert(df_old, df_new, col_key)
        self.numInserted = df_insert.count()
        
        # Insert new data
        df_insert.write.mode("append").saveAsTable(f"{db_hive_name}.{tblName}")

        # Check update

        # Update new datas

    # Getter method for insert_count
    def get_insert_count(self):
        return self.numInserted

    # Getter method for update_count
    def get_update_count(self):
        return self.numUpdated