from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator

PATH = Variable.get('path_to_run')


def _print_result(task_instance) -> None:
    """
    Get form Xcom value and log this
    :return: None
    """
    msg = task_instance.xcom_pull(dag_id='dag_id_2', task_ids='end_dag', key='result_of_task')
    print(f"{msg}")


def create_sub_dag(parent_dag_id, parent_task_id, default_args):
    with DAG(f"{parent_dag_id}.{parent_task_id}", default_args=default_args) as dag:
        sensor = ExternalTaskSensor(
            task_id='sensor',
            external_task_id=None,
            external_dag_id='dag_id_2',
            poke_interval=15
        )

        print_result = PythonOperator(
            task_id='print_result',
            python_callable=_print_result,

        )

        remove_file = BashOperator(
            task_id='remove_file',
            bash_command=f"rm {PATH}run.txt"
        )

        time_stamp = BashOperator(
            task_id='time_stamp',
            bash_command="echo {{ ts_nodash }}"
        )
        sensor >> print_result >> remove_file >> time_stamp

    return dag
