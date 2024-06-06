from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

import sys
sys.path.append("/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source")

from modules.HadoopEcosystem import *
hadoop = HadoopEcosystem()

def create_data_pipeline_dag(hadoop):

    # Create a DAG
    dag = DAG(
        'Pipeline_Global_Electronics_Retailer',
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

    driver_path = "/home/thanhphat/PersonalProject/Global_Electronics_Retailer/driver/mysql-connector-j-8.1.0.jar"
    spark_app_path = "/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/Source_to_Datalake.py"

    # Task to run Spark job from source_to_datalake.py
    run_spark_source_to_datalake_task = BashOperator(
        task_id = 'run_spark_source_to_datalake',
        bash_command = f"spark-submit --jars {driver_path} {spark_app_path}",
        dag = dag,
    )

    spark_app_path_2 = "/home/thanhphat/PersonalProject/Global_Electronics_Retailer/source/Datalake_to_Datawarehouse.py"

    # Task to run Spark job from datalake_to_datawarehouse.py
    run_spark_datalake_to_datawarehouse_task = BashOperator(
        task_id = 'run_spark_datalake_to_datawarehouse',
        bash_command = f"spark-submit {spark_app_path_2}",
        dag = dag,
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