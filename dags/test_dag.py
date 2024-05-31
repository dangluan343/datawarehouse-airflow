
from datetime import datetime

from airflow.decorators import task, task_group
from airflow.models.dag import DAG
from hooks.mongo_hook import MongoDBHook
from etl.extracting.commons import *
from etl.extracting.era5 import *

def serialize(data):
    """
    Serialize data to a JSON string.
    
    Args:
        data: The data to be serialized.
    
    Returns:
        str: JSON serialized string.
    """
    def default_serializer(obj):
        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode('utf-8')
        raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')
    
    return json.dumps(data, default=default_serializer)


def deserialize(json_str):
    """
    Deserialize JSON string back to data.
    
    Args:
        json_str (str): JSON serialized string.
    
    Returns:
        The deserialized data.
    """
    def default_deserializer(obj):
        if isinstance(obj, str):
            try:
                return base64.b64decode(obj)
            except ValueError:
                return obj
        return obj
    
    return json.loads(json_str, object_hook=default_deserializer)



@task_group
def group_task_init_staging_era5_single():
    @task 
    def read_era5_single():
        files = get_files_in_directory(SOURCES_PATH + 'era5-single')
        coordinates, elements = get_era5_data(files[0])
        return coordinates, elements
    
    @task 
    def split_era5_single(era5_data):
        coordinates = era5_data[0]
        elements = era5_data[1]
        splitted_data = split_era5(coordinates, elements, 20)

        return splitted_data

    @task 
    def load_era5_single(documents):
        connect = MongoDBHook(conn_id='mongodb')
        connect.insert_one(
            database='staging_area', 
            collection='era5_single', 
            document=documents[0]
        )
    
    reading_era5 = read_era5_single()
    splitting_era5 = split_era5_single(reading_era5)
    loading_era5 = load_era5_single(splitting_era5)

    reading_era5 >> splitting_era5 >> loading_era5




with DAG(dag_id="test_dag", start_date=datetime(2022, 4, 2), schedule_interval='@once') as dag:
    task_1 = group_task_init_staging_era5_single();
   