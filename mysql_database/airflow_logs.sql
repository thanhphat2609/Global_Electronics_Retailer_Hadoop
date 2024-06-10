CREATE DATABASE airflow_log_pipeline_db CHARACTER SET utf8 COLLATE utf8_unicode_ci;
CREATE USER 'airflow_user' IDENTIFIED BY 'airflow_pass';
GRANT ALL PRIVILEGES ON airflow_log_pipeline_db.* TO 'airflow_user';


CREATE TABLE airflow_log_pipeline_db.pipeline_log(
	dag_id varchar(100),
	pipeline_id varchar(100),
    pipeline_status varchar(150),
	pipeline_error varchar(8000),
	start_time varchar(1000),
    end_time varchar(1000),
    executionDate varchar(1000),
    lastrunDate varchar(1000)
)
