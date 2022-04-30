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
        with open(f"{PATH}/data_crash.csv", "r") as data:
            print(data.readline())
        return {"path": f"{PATH}/data_crash.csv"}

    path_data = open_data()

    @dag.task(multiple_outputs=True)
    def count_accident(raw_json: dict) -> dict:

        file_path = raw_json["path"]
        data = pd.read_csv(file_path, encoding='unicode_escape')
        count = data[['Year', 'Master Record Number']].groupby(['Year']).count()
        return json.loads(count.to_json())

    count_crash = count_accident(path_data)

    @dag.task(multiple_outputs=False)
    def print_result(orig_json: dict) -> str:
        for (year, cnt) in orig_json['Master Record Number'].items():
            print(f'Year: {year}  ---  {cnt} .')
        return 'success'
    log_result = print_result(count_crash)

    path_data >> count_crash >> log_result
