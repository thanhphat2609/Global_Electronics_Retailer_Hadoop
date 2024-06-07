from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime

import sys
sys.path.append("/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source")

from modules.HadoopEcosystem import *
hadoop = HadoopEcosystem()

def create_data_pipeline_dag(hadoop):

    # Create a DAG
    dag = DAG(
        'Pipeline_Global_Electronics_Retailer_Main',
        description = 'A DAG run Pipeline',
        schedule = "0 22 * * *", 
        start_date = datetime(2024, 6, 4),
        catchup = False
    )

    # Task to start Hadoop
    start_hadoop_task = PythonOperator(
        task_id= 'start_hadoop_service',
        python_callable = hadoop.start_hadoop,
        dag = dag,
    )

    # Task to run Spark job from source_to_datalake.py
    run_spark_source_to_datalake_task = SparkSubmitOperator(
        task_id = "run_spark_source_to_datalake",
        application = "/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/Source_to_Datalake.py",
        conn_id = "spark_default",
        total_executor_cores = '5',
        executor_cores = '1',
        executor_memory = '4g',
        num_executors = '4',
        driver_memory = '4g',
        jars = '/home/thanhphat/PersonalProject/Global_Electronics_Retailer/driver/mysql-connector-j-8.1.0.jar',
        verbose = False
    )

    # Task to run Spark job from datalake_to_datawarehouse.py
    run_spark_datalake_to_datawarehouse_task = SparkSubmitOperator(
        task_id = 'run_spark_datalake_to_datawarehouse',
        application = "/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/Datalake_to_Datawarehouse.py",
        conn_id = "spark_default",
        total_executor_cores = '5',
        executor_cores = '1',
        executor_memory = '4g',
        num_executors = '4',
        driver_memory = '4g',
        verbose = False
    )

    # Task to start Apache Superset
    start_superset_task = PythonOperator(
        task_id = 'start_superset',
        python_callable = hadoop.start_superset,
        dag = dag,
    )

    # Define dependencies between tasks
    start_hadoop_task >> run_spark_source_to_datalake_task >> run_spark_datalake_to_datawarehouse_task >> start_superset_task

    # Return the created DAG
    return dag

# Create the DAG when the file is imported
data_pipeline_dag = create_data_pipeline_dag(hadoop)