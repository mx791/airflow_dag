import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task_group
import json

BASE_DIR = "/opt/airflow/dags"
dependancy_graph = json.load(open(BASE_DIR + "/task_1/data.json"))

dag = DAG(
    dag_id="load_json",
    start_date=datetime.datetime(2023,10,18),
    #schedule="@daily",
)

def lauch_etl(etl):
    def main():
        print("je lance l'etl " + etl)
    return main

def lauch_model(model):
    def main():
        print("je lance l'etl " + model)
    return main

def create_etl(name, dag=dag):
    return PythonOperator(task_id=name, dag=dag, python_callable=lauch_etl(name))


def create_model(name, dag=dag):
    return PythonOperator(task_id=name, dag=dag, python_callable=lauch_model(name))


names_to_objects = {}
for modl in dependancy_graph:
    if modl not in names_to_objects:
        names_to_objects[modl] = create_model(modl)
    for etl in dependancy_graph[modl]:
        if etl not in names_to_objects:
            names_to_objects[etl] = create_etl(etl)
        names_to_objects[etl] >> names_to_objects[modl]

    
