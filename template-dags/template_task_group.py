from datetime import datetime

from airflow.decorators import dag, task_group, task
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'tags':["template"], 

}

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(1), catchup=False)
def template_task_group_dag():
    @task_group(group_id="process_data")
    def process_data_tasks():
        @task(task_id="extract_data")
        def extract_data():
            # Your code to extract data
            return "Extracted data"

        @task(task_id="transform_data")
        def transform_data(data):
            # Your code to transform data
            return f"Transformed {data}"

        @task(task_id="load_data")
        def load_data(transformed_data):
            # Your code to load data
            print(f"Loaded {transformed_data} into the database")

        data = extract_data()
        transformed = transform_data(data)
        load_data(transformed)

    process_data_tasks()

template_dag = template_task_group_dag()
