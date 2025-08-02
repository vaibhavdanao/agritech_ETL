from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3
import time

AWS_REGION = "us-east-1"
WORKGROUP_NAME = "krishikendra-rs-serverless"
NAMESPACE_NAME = "krishikendra-ns"
IAM_ROLE_ARN = "arn:aws:iam::339712993834:role/AmazonMWAA-agritech_infra_dag-9Z3Hzg"

def create_redshift_namespace():
    client = boto3.client("redshift-serverless", region_name=AWS_REGION)
    try:
        client.create_namespace(
            namespaceName=NAMESPACE_NAME,
            dbName="default_db"
        )
    except client.exceptions.ConflictException:
        print(f"Namespace {NAMESPACE_NAME} already exists.")

def create_redshift_workgroup():
    client = boto3.client("redshift-serverless", region_name=AWS_REGION)
    try:
        client.create_workgroup(
            workgroupName=WORKGROUP_NAME,
            namespaceName=NAMESPACE_NAME,
            baseCapacity=32,  # 32 RPU, adjust based on use
            publiclyAccessible=True,
            enhancedVpcRouting=False,
            # iamRoles=[IAM_ROLE_ARN]  ← ❌ REMOVE THIS LINE
        )
    except client.exceptions.ConflictException:
        print(f"Workgroup {WORKGROUP_NAME} already exists.")

def wait_until_workgroup_ready():
    client = boto3.client("redshift-serverless", region_name=AWS_REGION)
    while True:
        response = client.get_workgroup(workgroupName=WORKGROUP_NAME)
        status = response["workgroup"]["status"]
        print(f"Workgroup status: {status}")
        if status == "AVAILABLE":
            break
        time.sleep(10)

with DAG(
    dag_id="redshift_serverless_setup",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["redshift", "infra", "iceberg"]
) as dag:

    create_namespace = PythonOperator(
        task_id="create_namespace",
        python_callable=create_redshift_namespace
    )

    create_workgroup = PythonOperator(
        task_id="create_workgroup",
        python_callable=create_redshift_workgroup
    )

    wait_for_ready = PythonOperator(
        task_id="wait_for_workgroup_ready",
        python_callable=wait_until_workgroup_ready
    )

    create_namespace >> create_workgroup >> wait_for_ready
