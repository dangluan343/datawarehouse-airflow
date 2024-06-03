from datetime import datetime
from airflow.decorators import task, task_group, dag
from airflow.models.dag import DAG
from airflow.utils.dates import days_ago

inputs = ["a", "b", "c"]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'tags': ["template"], 
}

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(1), catchup=False)
def template_map_branch_dag():
    print(1)
    
    @task_group(group_id="my_task_group")
    def my_task_group(input):
        @task.branch
        def branch(element):
            if "a" in element:
                return "my_task_group.a"
            elif "b" in element:
                return "my_task_group.b"
            else:
                return "my_task_group.c"

        @task
        def a():
            print("a")

        @task
        def b():
            print("b")

        @task
        def c():
            print("c")

        branch_task = branch(input)
        a_task = a()
        b_task = b()
        c_task = c()

        branch_task >> [a_task, b_task, c_task]

    my_task_group.expand(input=inputs)

template_dag = template_map_branch_dag()
