import os
import json

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta


IMAGE = Variable.get("dbt_test_image")


default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'dbt_dag',
    default_args=default_args,
    description='A dbt wrapper for airflow',
    schedule_interval=None,
)

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

def load_manifest():
    local_filepath = os.environ.get('MANIFEST_FILE') or f"{ROOT_DIR}/data/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data


def make_dbt_task(node: str, dbt_verb: str, dag: DAG) -> KubernetesPodOperator:
    """Returns an Airflow operator either run and test an individual model"""
    model = node.split(".")[-1]

    return KubernetesPodOperator(
        dag=dag,
        task_id=node,
        name=node,
        is_delete_operator_pod=False,
        image=IMAGE,
        cmds=[
            "python3",
            "execute.py",
            "--verb", dbt_verb,
            "--target", "prod",
            "--models", model
        ],
        resources={
            "request_cpu": 0.5,
            "limit_cpu": 1,
            "request_memory": "64Mi",
            "limit_memory": "512Mi",
        },
    )

data = load_manifest()

dbt_tasks = {}
for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        node_test = node.replace("model", "test")

        dbt_tasks[node] = make_dbt_task(node, "run", dag)
        dbt_tasks[node_test] = make_dbt_task(node_test, "test", dag)

for node in data["nodes"].keys():
    if node.split(".")[0] == "model":

        # Set dependency to run tests on a model after model runs finishes
        node_test = node.replace("model", "test")
        dbt_tasks[node] >> dbt_tasks[node_test]

        # Set all model -> model dependencies
        for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:

            upstream_node_type = upstream_node.split(".")[0]
            if upstream_node_type == "model":
                dbt_tasks[upstream_node] >> dbt_tasks[node]
