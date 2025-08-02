from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='glue_process_krishikendra_dag_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Trigger AWS Glue job to process krishikendra data',
) as dag:

    glue_job = GlueJobOperator(
        task_id='test-glue-job',
        job_name='process_test_glue',  # This must match the Glue Job name in AWS
        script_location='s3://vai-airflow/dags/glue_job_script/process_test.py',
        region_name='us-east-1',
        iam_role_name='AWSGlueServiceRole-Krishikendra',
        s3_bucket='krishikendra-data-lake',
        aws_conn_id='aws_default',
        create_job_kwargs={
            'GlueVersion': '4.0',
            'NumberOfWorkers': 2,
            'WorkerType': 'G.1X',
        },
        wait_for_completion=True,
        verbose=True,
    )

    glue_job
