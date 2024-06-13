class Transformation:

    def __init__(self) -> None:
        self.numInserted = 0
        self.numUpdated = 0
        pass

    # Check data update
    def update(self, spark, db_hive_name, tblName, df_new):
        """
            Function for update new data

            - Args:
                db_hive_name: Warehouse from hive
                tblName: Table from Hive
                df_new: New dataframe

            - Return:
                None
        
        """

        # Construct the UPDATE SQL query
        delete_query = f"DELETE FROM {db_hive_name}.{tblName}"

        spark.sql(delete_query)

        df_new.write.partitionBy("year", "month", "day").mode("overwrite").saveAsTable(f"{db_hive_name}.{tblName}")
        

    # Check insert data
    def check_insert(self, df_old, df_new, col_key, f):
        """
            Function for check new records

            - Args:
                df_old: Dataframe old from hive
                df_new: Dataframe new from datalake
                col_key: Key column

            - Return:
                None
        
        """

        df_insert = df_new.alias("t1").join(df_old.alias("t2"), on = col_key, how = "left") \
                          .filter(f.col(f"t2.{col_key}").isNull()) \
                          .selectExpr("t1.*")

        return df_insert

    # Merge data: Update exist data, insert new data
    def merge(self, spark, df_new, db_hive_name, tblName, col_key, f):
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

        # Order Number for col_key is not accepted
        if col_key == "Order Number":
            col_key = "OrderNumber"

        # Read
        df_old = spark.sql(f"SELECT * FROM {db_hive_name}.{tblName}")

        # Update all
        self.numUpdated = df_old.count()
        self.update(spark, db_hive_name, tblName, df_new)

        # Get df_insert and numInserted
        df_insert = self.check_insert(df_old, df_new, col_key, f)
        self.numInserted = df_insert.count()
        # Check row insert
        if df_insert.count() > 0:
            # Insert new data
            if (tblName != "fact_sales") and (tblName != "dim_exchange_rates"):
                df_insert.write.partitionBy("year", "month", "day").mode("append").saveAsTable(f"{db_hive_name}.{tblName}")
            else:
                df_insert.write.partitionBy("OrderDate").mode("append").saveAsTable(f"{db_hive_name}.{tblName}")
        

    # Getter method for insert_count
    def get_insert_count(self):
        return self.numInserted

    # Getter method for update_count
    def get_update_count(self):
        return self.numUpdated
