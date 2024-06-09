from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

class LogUtils:
    def __init__(self) -> None:
        pass

    def log_data(self, batch_id, pipeline_job, dbname, table_name,
                 start_time, end_time, src_rows_read, numInserted, numUpdated, columnMissing, 
                 columnNull, error, status, types, spark):
        """
        Log the pipeline data.

        Args:
        - batch: The batch of the pipeline.
        -
        - pipelinejob: The task name.
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


    def pipeline_log(self, dag_id, task_id, task_status, task_error, start_time, **kwargs):
        """
        Function for logging task status to MySQL.

        Args:
            dag_id: The id of the DAG.
            task_id: The id of the task.
            task_status: The status of the task.
            task_error: Any error associated with the task.
            start_time: The start time of the task.
            execution_date: The execution date of the task.
            **kwargs: Additional keyword arguments.

        Returns:
            None
        """

        execution_date = datetime.now().strftime('%Y-%m-%d')
        end_time = datetime.now().strftime('%H:%M:%S')

        sql_statement = f"""
                        INSERT INTO airflow_log_pipeline_db.pipeline_log 
                        (dag_id, pipeline_id, pipeline_status, pipeline_error, start_time, end_time, executionDate, lastrunDate) 
                        VALUES 
                        ('{dag_id}', '{task_id}', '{task_status}', '{task_error}', '{start_time}', '{end_time}', '{execution_date}', '{execution_date}')
                    """

        mysql_task = MySqlOperator(
            task_id="log_task_status_to_mysql",
            mysql_conn_id="mysql_pipeline_log",
            sql=sql_statement
        )

        mysql_task.execute(context=kwargs)
