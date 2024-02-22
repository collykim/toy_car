from airflow import DAG
from datetime import datetime as dt
from datetime import timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt(2024, 2, 15),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'News_Spark_Submit',
    default_args=default_args,
    description='Car Naver News Collect in Daily',
    schedule_interval="0 9 * * *",
    catchup=False
)

news_spark_task = SparkSubmitOperator(
    task_id='Naver_News_Spark_task',
    application='/home/ubuntu/spark_file/naver_news_spark.py',  # Spark job이 있는 경로
    dag=dag,
    deploy_mode='client',
)


news_spark_task