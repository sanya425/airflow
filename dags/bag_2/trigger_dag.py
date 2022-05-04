from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
from sensors.smart_file_sensor import SmartFileSensor
from sub_dags.sub_dag import create_sub_dag

PATH = Variable.get('path_to_run')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}


def sent_message(*args) -> None:
    """
    Sent message to the slack chanel
    :param args: DAG_id, execution_date
    :return: None
    """
    run_id = args[0]
    execution_date = args[1]
    slack_token = Variable.get('slack_token')
    msg = f'DAG_id: {run_id}; Execution date: {execution_date}'
    bash_command = "curl -X POST -H 'Content-type: application/json' --data '{\"text\": \"%s\"}'" \
                   " https://hooks.slack.com/services/%s" % (msg, slack_token)
    os.system(bash_command)


with DAG(dag_id='trigger_dag', schedule_interval='*/4 * * * *', default_args=default_args, catchup=False) as dag:
    waiting_for_file = SmartFileSensor(
        task_id='waiting_for_file',
        poke_interval=5,
        timeout=60 * 5,
        filepath='run.txt',
        fs_conn_id='fs_default',

    )

    trigger_target = TriggerDagRunOperator(
        task_id='trigger_target',
        trigger_dag_id='dag_id_2',
        execution_date='{{ds}}',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=15
    )

    sub_dag = SubDagOperator(
        task_id='sub_dag',
        subdag=create_sub_dag("trigger_dag", "sub_dag", default_args)
    )

    slack_message = PythonOperator(
        task_id='slack_message',
        python_callable=sent_message,
        op_args=[dag.dag_id, datetime.now()]
    )

    waiting_for_file >> trigger_target >> sub_dag >> slack_message
