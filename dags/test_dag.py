
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


@task
def extract_staging_era5_single():
    connect = MongoDBHook(conn_id='mongodb')
    documents = list(connect.find(database='staging_area', collection='era5_single'))
    dimension_element = []
    dimension_time = []
    dimension_location = []
    fact_era5 = []

    for document in documents:
        
        for element_data in document['elements']:

            for i in range(len(element_data['values'])):
                element = {'name': element_data['name'], 'short_name': element_data['short_name'], 'unit': element_data['units']}
                time = {'date': element_data['date'], 'hour': element_data['time']}
                location = {'latitude': document['coordinates'][i][0], 'longitude': document['coordinates'][i][1], 'altitude': element_data['pressure_level']}

                dimension_element.append(element)
                dimension_time.append(time)
                dimension_location.append(location)

                value = [element_data['values'][i]]
                fact_to_dimensions = {'value': value, 'element': element, 'time': time, 'location': location}
                fact_era5.append(fact_to_dimensions)

    return  {
                'fact': fact_era5, 
                'dimension_element': dimension_element, 
                'dimension_time': dimension_time,
                'dimension_location': dimension_location
            }

    



with DAG(dag_id="test_dag", start_date=datetime(2022, 4, 2), schedule_interval='@once') as dag:
    task_1 = extract_staging_era5_single();
   