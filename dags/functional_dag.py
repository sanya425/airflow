import json
from datetime import datetime
from airflow.models import DAG, Variable
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 25),
    'retries': 1,
}

PATH = Variable.get('path_to_run')

with DAG(dag_id='func_dag', default_args=default_args) as dag:

    @dag.task(multiple_outputs=True)
    def open_data() -> dict:
        """
        Try to open data
        :return: dict with path to file(such as json)
        """
        with open(f"{PATH}/data_crash.csv", "r") as data:
            print(data.readline())
        return {"path": f"{PATH}/data_crash.csv"}


    @dag.task(multiple_outputs=True)
    def count_accident(raw_json: dict) -> dict:
        """
        Count car crash for every year (2003 - 2015)
        :param raw_json: dict with path to file
        :return: json with answer
        """
        file_path = raw_json["path"]
        data = pd.read_csv(file_path, encoding='unicode_escape')
        count = data[['Year', 'Master Record Number']].groupby(['Year']).count()
        return json.loads(count.to_json())


    @dag.task(multiple_outputs=False)
    def print_result(orig_json: dict) -> str:
        """
        Print result
        :param orig_json: json with answer
        :return: 'success'
        """
        for year, cnt in orig_json['Master Record Number'].items():
            print(f'Year: {year}  ---  {cnt} .')
        return 'success'


    path_data = open_data()
    count_crash = count_accident(path_data)
    log_result = print_result(count_crash)

    path_data >> count_crash >> log_result
