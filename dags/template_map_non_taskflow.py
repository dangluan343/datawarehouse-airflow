

from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG


class AddOneOperator(BaseOperator):
    """A custom operator that adds one to the input."""

    def __init__(self, value, **kwargs):
        super().__init__(**kwargs)
        self.value = value

    def execute(self, context):
        return self.value + 1


class SumItOperator(BaseOperator):
    """A custom operator that sums the input."""

    template_fields = ("values",)

    def __init__(self, values, **kwargs):
        super().__init__(**kwargs)
        self.values = values

    def execute(self, context):
        total = sum(self.values)
        print(f"Total was {total}")
        return total


with DAG(
    dag_id="template_dynamic_task_mapping_with_no_taskflow_operators",
    start_date=datetime(2022, 3, 4),
    catchup=False,
):
    # map the task to a list of values
    add_one_task = AddOneOperator.partial(task_id="add_one").expand(value=[1, 2, 3])

    # aggregate (reduce) the mapped tasks results
    sum_it_task = SumItOperator(task_id="sum_it", values=add_one_task.output)