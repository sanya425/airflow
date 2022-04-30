from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from airflow.hooks.base import BaseHook
import os
from airflow.models import Variable
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from airflow.providers.hashicorp.secrets.vault import VaultBackend

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}


def task_model():
    return randint(1, 10)


def choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        "task_A",
        "task_B",
        "task_C"
    ])
    best_accurate = max(accuracies)
    if best_accurate > 8:
        return "accurate"
    return "inaccurate"


def sent_message(*args):
    run_id = args[0]
    execution_date = args[1]

    #slack_token = BaseHook.get_connection('slack').password
    slack_token = Variable.get('slack_token')
    msg = f'DAG_id: {run_id}; Execution date: {execution_date}'
    bash_command = "curl -X POST -H 'Content-type: application/json' --data '{\"text\": \"%s\"}'" \
                   " https://hooks.slack.com/services/%s" % (msg, slack_token)
    os.system(bash_command)


with DAG(dag_id='dag1', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    task_A = PythonOperator(
        task_id="task_A",
        python_callable=task_model
    )

    task_B = PythonOperator(
        task_id="task_B",
        python_callable=task_model
    )

    task_C = PythonOperator(
        task_id="task_C",
        python_callable=task_model
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=choose_best_model
    )

    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

    slack_message = PythonOperator(
        task_id='slack_message',
        python_callable=sent_message,
        op_args=[dag.dag_id, datetime.now()]
    )
    slack_message >> [task_A, task_B, task_C] >> choose_best_model >> [accurate, inaccurate]
