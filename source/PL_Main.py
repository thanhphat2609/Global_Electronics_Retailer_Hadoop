from airflow import DAG
# with operator need to set dag = dag for relationships
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import sys
sys.path.append("/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source")

from modules.HadoopEcosystem import *
hadoop = HadoopEcosystem()

from modules.LogUtils import *
logUtils = LogUtils()

def create_data_pipeline_dag(hadoop, logUtils, dagid):

    # Create a DAG
    dag = DAG(
        f'{dagid}',
        description = 'A DAG run Pipeline Global Electronics Retailer',
        catchup = False
    )

    task_start_time = datetime.now().strftime('%H:%M:%S')

    # Task to start Hadoop
    start_hadoop_task = PythonOperator(
        task_id = 'start_hadoop_service',
        python_callable = hadoop.start_hadoop,
        dag = dag,
    )

    # Log_Task
    log_pipeline_hadoop_task = PythonOperator(
        task_id = "log_task_hadoop",
        python_callable = logUtils.pipeline_log,
        op_kwargs = {
            "dag_id": f"{dagid}",
            "task_id": "start_hadoop_service",
            "task_status": "Success",
            "task_error": "",
            "start_time": task_start_time
        },
        dag = dag,
    )

    task_start_time = datetime.now().strftime('%H:%M:%S')

    # Task to run Spark job from source_to_datalake.py
    run_spark_source_to_datalake_task = SparkSubmitOperator(
        task_id = "run_spark_source_to_datalake",
        application = "/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/Source_to_Datalake.py",
        conn_id = "spark_default",
        total_executor_cores = '5',
        executor_cores = '1',
        executor_memory = '1g',
        num_executors = '4',
        driver_memory = '1g',
        jars = '/home/thanhphat/PersonalProject/Global_Electronics_Retailer/driver/mysql-connector-j-8.1.0.jar',
        verbose = False
    )

    # Log_Task
    log_pipeline_source_to_datalake_task = PythonOperator(
        task_id = "log_task_source_to_datalake",
        python_callable = logUtils.pipeline_log,
        op_kwargs = {
            "dag_id": f"{dagid}",
            "task_id": "run_spark_source_to_datalake",
            "task_status": "Success",  # or "FAILED" based on the actual status
            "task_error": "",  # Include any error message if the task failed
            "start_time": task_start_time
        },
        dag = dag,
    )

    task_start_time = datetime.now().strftime('%H:%M:%S')
    # Task to run Spark job from datalake_to_datawarehouse.py
    run_spark_datalake_to_datawarehouse_task = SparkSubmitOperator(
        task_id = 'run_spark_datalake_to_datawarehouse',
        application = "/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/Datalake_to_Datawarehouse.py",
        conn_id = "spark_default_2",
        total_executor_cores = '5',
        executor_cores = '1',
        executor_memory = '1g',
        num_executors = '4',
        driver_memory = '1g',
        verbose = False
    )

    # Log_Task
    log_pipeline_datalake_to_datawarehouse_task = PythonOperator(
        task_id = "log_task_datalake_to_datawarehouse",
        python_callable = logUtils.pipeline_log,
        op_kwargs = {
            "dag_id": f"{dagid}",
            "task_id": "run_spark_datalake_to_datawarehouse",
            "task_status": "Success",  # or "FAILED" based on the actual status
            "task_error": "",  # Include any error message if the task failed
            "start_time": task_start_time
        },
        dag = dag,
    )

    task_start_time = datetime.now().strftime('%H:%M:%S')
    # Task to start Apache Hive
    start_hive_server2 = PythonOperator(
        task_id = 'start_hive',
        python_callable = hadoop.start_hive,
        dag = dag,
    )

    # Log_Task
    log_start_hive_task = PythonOperator(
        task_id = "log_task_start_hive",
        python_callable = logUtils.pipeline_log,
        op_kwargs = {
            "dag_id": f"{dagid}",
            "task_id": "start_hive",
            "task_status": "Success",  # or "FAILED" based on the actual status
            "task_error": "",  # Include any error message if the task failed
            "start_time": task_start_time
        },
        dag = dag,
    )

    task_start_time = datetime.now().strftime('%H:%M:%S')
    # Task to start Apache Superset
    start_superset_task = PythonOperator(
        task_id = 'start_superset',
        python_callable = hadoop.start_superset,
        dag = dag,
    )

    # Log_Task
    log_start_superset_task = PythonOperator(
        task_id = "log_task_start_superset",
        python_callable = logUtils.pipeline_log,
        op_kwargs = {
            "dag_id": f"{dagid}",
            "task_id": "start_superset",
            "task_status": "Success",  # or "FAILED" based on the actual status
            "task_error": "",  # Include any error message if the task failed
            "start_time": task_start_time
        },
        dag = dag,
    )


    # Define dependencies between tasks
    start_hadoop_task >> log_pipeline_hadoop_task >> \
    run_spark_source_to_datalake_task >>  log_pipeline_source_to_datalake_task >> \
    run_spark_datalake_to_datawarehouse_task >> log_pipeline_datalake_to_datawarehouse_task >> \
    start_hive_server2 >>  log_start_hive_task >> \
    start_superset_task >> log_start_superset_task

    # Return the created DAG
    return dag

# Create the DAG when the file is imported
data_pipeline_dag = create_data_pipeline_dag(hadoop, logUtils, 'PL_Main_Global_Electronics_Retailer')