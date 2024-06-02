class DataLog:
    def __init__(self) -> None:
        pass

    def log_data(self, batch_id, pipeline_job, dbname, table_name,
                 start_time, end_time, src_rows_read, numInserted, numUpdated, columnMissing, 
                 columnNull, error, status, types, spark):
        """
        Log the pipeline data.

        Args:
        - batchid: The batch of the pipeline.
        - pipelinejob: The task name.
        - dbname: Source Database.
        - table_name: Source table
        - start_time: The start time of the task.
        - end_time: The end time of the task.
        - src_rows_read: Number of source rows read.
        - numInserted: Number of rows inserted.
        - numUpdated: Number of rows updated.
        - columnNull: Columns with null counts.
        - error: Error message if any.
        - status: Status of the task.
        - types: instance of pyspark.sql.types
        - spark: SparkSession

        Returns:
        - DataFrame containing items in the workspace.
        """
        # Define schema
        log_schema = types.StructType() \
                        .add("BatchId", types.IntegerType(), True) \
                        .add("PipelineJob", types.StringType(), True) \
                        .add("SourceDatabase", types.StringType()) \
                        .add("SourceTable", types.StringType(), True) \
                        .add("StartTime", types.StringType(), True) \
                        .add("EndTime", types.StringType(), True) \
                        .add("SourceRowsRead", types.IntegerType(), True) \
                        .add("NumTargetInserted", types.IntegerType(), True) \
                        .add("NumTargetUpdated", types.IntegerType(), True) \
                        .add("Status", types.StringType(), True) \
                        .add("ColumnMissing", types.StringType(), True) \
                        .add("ColumnNull", types.StringType(), True) \
                        .add("Error", types.StringType(), True) \

        # Create new row
        new_log_row = [types.Row(batch_id, pipeline_job, dbname, table_name, start_time, end_time, 
                                src_rows_read, numInserted, numUpdated, status, columnMissing, columnNull, error)]
        
        new_df = spark.createDataFrame(new_log_row, log_schema)

        # Return value
        return new_df
