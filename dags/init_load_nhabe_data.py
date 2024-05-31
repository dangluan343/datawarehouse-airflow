from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.dates import days_ago
from hooks.mongo_hook import MongoDBHook

from helpers.radar_helper import read_radar_file_path
from helpers.radar_extractor import RadarReader
from helpers.radar_staging_loader import RadarStagingLoader
from helpers.utils import *

from decorators.audit_log_file import * 

from datetime import datetime
import pyart
import logging
import json
import base64

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



default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

with DAG('initial_load_radar', default_args=default_args, schedule_interval='@once') as dag:

    @task_group
    def group_task_init_load_radar_into_staging():
        
        @task
        def task_get_absolute_file_paths():

            connect = MongoDBHook(conn_id='mongodb')
            source_info = {
                'source': 'NHABE',
                'source_type': 'static_file'
            }

            @write_audit_log_file
            def excecute(connect: MongoDBHook, source_info: dict):
                file_paths = read_radar_file_path()
                return file_paths

            return excecute(connect=connect, source_info=source_info)

        @task_group(group_id='group_task_etl_on_each_file')
        def group_task_etl_on_each_file(file_path):

            @task 
            def task_read_radar_info(file_path):
                radar = pyart.io.read_sigmet(file_path)
                reader = RadarReader(radar)
                radar_info = reader.get_radar_info()
                return serialize(radar_info)
            
            @task 
            def task_read_radar_sweep(file_path):
                radar = pyart.io.read_sigmet(file_path)
                reader = RadarReader(radar)
                radar_sweep = reader.get_radar_sweep()
                return serialize(radar_sweep)
            
            @task 
            def task_read_radar_data(file_path):
                radar = pyart.io.read_sigmet(file_path)
                reader = RadarReader(radar)
                radar_data = reader.get_radar_data()
                return serialize(radar_data)
            
            @task
            def task_load_radar_location(radar_info):
                connect = MongoDBHook(conn_id='mongodb')
                loader = RadarStagingLoader(connect)
                logging.info(f"Loaded radar info: {radar_info}")
                loader.load_radar_location(deserialize(radar_info))

            @task
            def task_load_radar_sweep(radar_sweep):
                connect = MongoDBHook(conn_id='mongodb')
                loader = RadarStagingLoader(connect)
                loader.load_radar_sweep(deserialize(radar_sweep))
                logging.info(f"Loaded radar sweep info: {radar_sweep}")

            @task
            def task_load_radar_data(radar_data):
                connect = MongoDBHook(conn_id='mongodb')
                loader = RadarStagingLoader(connect)
                loader.load_radar_data(deserialize(radar_data))
                logging.info(f"Loaded radar data: {radar_data}")
            
            @task
            def task_write_audit_log_load(file_path):
                file_name = get_file_name_from_absolute_path(file_path)
                document = {
                    '_id': file_name,
                    'file_name': file_name,
                    'from': 'disk',
                    'to': 'staging_area',
                    'status': 'success',
                    'completed_at': datetime.now(),
                }
                connect = MongoDBHook(conn_id='mongodb')
                connect.insert_one(
                    database='audit_log', 
                    collection='audit_log_load', 
                    document=document
                )

                logging.info(f"audit log load: {file_name}")

            reading_radar_info = task_read_radar_info(file_path)
            loading_radar_location = task_load_radar_location(reading_radar_info)
            reading_radar_sweep = task_read_radar_sweep(file_path)
            loading_radar_sweep = task_load_radar_sweep(reading_radar_sweep)
            reading_radar_data = task_read_radar_data(file_path)
            loading_radar_data = task_load_radar_data(reading_radar_data) 
            writing_adit_log_load = task_write_audit_log_load(file_path)
            
            return [loading_radar_location, loading_radar_sweep, loading_radar_data] >> writing_adit_log_load

        file_paths = task_get_absolute_file_paths()
        group_task_etl_on_each_file.expand(file_path=file_paths)

    @task_group
    def group_task_init_load_radar_into_core():
        
        @task 
        def get_time_location_key(): #==> [{}, {}]
            connect = MongoDBHook(conn_id='mongodb')
            list_time_volume_start = list(connect.find(
                database='staging_area',
                collection='radar_sweep',
                projection={'_id': False, 'time_volume_start': True, 'radar_location_id': True}  
            ))
            return (list_time_volume_start)
        
        @task_group
        def group_task_process_batch_file(key):
     
            #extract staging radar_sweep
            @task
            def task_extract_staging_radar_sweep(key):
                connect = MongoDBHook(conn_id='mongodb')
                time_volume_start = key['time_volume_start']
                radar_location_id = key['radar_location_id']
                radar_sweep_data = list(connect.find(
                    database='staging_area', 
                    collection='radar_sweep', 
                    query={
                        'time_volume_start': time_volume_start, 
                        'radar_location_id': radar_location_id
                    },
                    projection={
                        '_id': False, 
                        'time_volume_start': False
                    }
                ))
                return serialize(radar_sweep_data)

            #extract staging radar_data
            @task 
            def task_extract_staging_radar_data(key):
                connect = MongoDBHook(conn_id='mongodb')
                time_volume_start = key['time_volume_start']
                radar_location_id = key['radar_location_id']
                radar_data = list(connect.find(
                    database='staging_area', 
                    collection='radar_data', 
                    query={
                        'time_volume_start': time_volume_start, 
                        'radar_location_id': radar_location_id
                    },
                    projection={
                        '_id': False, 
                        'time_volume_start': False
                    }
                )) 
                return serialize(radar_data)

            
            #transform radar_sweep_to_dim_element
            @task
            def task_transform_dim_element_key_dict(radar_sweep_data):
                element_surrogate_key_dict = {}
                for item in deserialize(radar_sweep_data): 
                    element_metadata = item['element_metadata']
                    for key, value in element_metadata.items():
                        element_surrogate_key = create_element_surrogate_key(key)
                        element_surrogate_key_dict[key] = element_surrogate_key

                return serialize(element_surrogate_key_dict)

            @task
            def task_transform_fact_radar_sweep(radar_sweep_data, time_surrogate_key):
                transformed_data = deserialize(radar_sweep_data)
                for item in transformed_data: 
                    if 'element_metadata' in item:
                        del item['element_metadata']
                assign_keys(transformed_data, ['time_id'], [time_surrogate_key])
                return serialize(transformed_data)
            
            #transform radar_data_to_fact_radar_data
            @task
            def task_transform_fact_radar_documents(
                radar_data, 
                time_surrogate_key, 
                transformed_element_key_dict
            ):
                element_key_dict = deserialize(transformed_element_key_dict)
                fact_radar_documents = []

                for item in deserialize(radar_data):
                    n = len(item['reflectivity'])
                    for i in range(n):
                        mapped_fact_radar_obj = {}
                        if(not item['reflectivity'][i] and not item['velocity'][i] and not item['spectrum_width'][i] ):
                            continue
                        if(item['reflectivity'][i]):
                            fact_reflectivity = {
                                'time_id': time_surrogate_key,
                                'second_duration': item['time'],
                                'radar_location_id': item['radar_location_id'],
                                'element_id': element_key_dict['reflectivity'],
                                'value': item['reflectivity'][i],
                                'ray_index': item['ray_index'],
                                'bin_index': i,
                            }

                            fact_radar_documents.append(fact_reflectivity)

                        if(item['velocity'][i]):
                            fact_velocity = {
                                'time_id': time_surrogate_key,
                                'second_duration': item['time'],
                                'radar_location_id': item['radar_location_id'],
                                'element_id': element_key_dict['velocity'],
                                'value': item['velocity'][i],
                                'ray_index': item['ray_index'],
                                'bin_index': i,
                            }

                            fact_radar_documents.append(fact_velocity)


                        if(item['spectrum_width'][i]):
                            fact_spectrum_width = {
                                'time_id': time_surrogate_key,
                                'second_duration': item['time'],
                                'radar_location_id': item['radar_location_id'],
                                'element_id': element_key_dict['spectrum_width'],
                                'value': item['spectrum_width'][i],
                                'ray_index': item['ray_index'],
                                'bin_index': i,
                            }
                            
                            fact_radar_documents.append(fact_spectrum_width)
                
                return serialize(fact_radar_documents)

            #load dim_time
            @task
            def task_load_dim_time(key):
                connect = MongoDBHook(conn_id='mongodb')
                time_volume_start = key['time_volume_start']
                date_document = extract_date_time(time_volume_start)
                time_surrogate_key = create_time_surrogate_key(date_document)
                date_document['_id'] = time_surrogate_key

                find_time = connect.find_one(
                    database='core', 
                    collection='dimension_time', 
                    query={'_id': time_surrogate_key}
                )

                if not find_time:
                    connect.insert_one(
                        database='core', 
                        collection='dimension_time', 
                        document=date_document
                    )
                return time_surrogate_key
                # radar_location_id = key['radar_location_id']
            
            #load dim_location
            @task
            def task_load_dim_location(key):
                # parse_location_surrogate_key
                location_surrogate_key = key['radar_location_id']
            
                connect = MongoDBHook(conn_id='mongodb')
                find_location = connect.find_one(
                    database='core', 
                    collection='dimension_location', 
                    query={'_id': location_surrogate_key}
                )
                if not find_location:
                    dim_location_document = parse_location_surrogate_key(location_surrogate_key)
                    dim_location_document['_id'] = location_surrogate_key
                    connect.insert_one(
                        database='core', 
                        collection='dimension_location', 
                        document=dim_location_document
                    )
                return location_surrogate_key
   
            #transform and load radar_sweep_to_dim_element
            @task
            def task_transform_load_dim_element(radar_sweep_data):
                connect = MongoDBHook(conn_id='mongodb')
                dim_element_documents = []
                for item in deserialize(radar_sweep_data): 
                    element_metadata = item['element_metadata']
                    for key, value in element_metadata.items():
                        element_surrogate_key = create_element_surrogate_key(key)
                        
                        metadata = connect.find_one(
                            database='core', 
                            collection='dimension_element', 
                            query={'_id': element_surrogate_key},
                        )

                        if not metadata:
                            value['_id'] = element_surrogate_key
                            connect.insert_one(
                                database='core', 
                                collection='dimension_element', 
                                document=value,
                            )

                
            #load fact_radar_sweep
            @task
            def task_load_fact_radar_sweep(radar_sweep):
                connect = MongoDBHook(conn_id='mongodb')
                connect.insert_many(
                    database='core', 
                    collection='fact_radar_sweep', 
                    documents=deserialize(radar_sweep)
                )
            # load fact_radar
            @task
            def task_load_fact_radar(radar_data):
                connect = MongoDBHook(conn_id='mongodb')
                connect.insert_many(
                    database='core', 
                    collection='fact_radar', 
                    documents=deserialize(radar_data)
                )
            
            @task
            def task_write_audit_log_load(time_key: str, location_key: str):
                document = {
                    'time_id': time_key,
                    'location_id': location_key,
                    'from': 'staging_area',
                    'to': 'core',
                    'status': 'success',
                    'completed_at': datetime.now(),
                }
                connect = MongoDBHook(conn_id='mongodb')
                connect.insert_one(
                    database='audit_log', 
                    collection='audit_log_load', 
                    document=document
                )

            
            # ETRACT
            extracting_staging_radar_sweep = task_extract_staging_radar_sweep(key)
            extracting_staging_radar_data = task_extract_staging_radar_data(key)

            # LOAD
            loading_dim_time = task_load_dim_time(key)
            loading_dim_location  = task_load_dim_location(key)


            # TRANSFORM
            transforming_fact_radar_sweep = task_transform_fact_radar_sweep(extracting_staging_radar_sweep, loading_dim_time) 
            transforming_element_key_dict = task_transform_dim_element_key_dict(extracting_staging_radar_sweep)
            transforming_fact_radar = task_transform_fact_radar_documents(extracting_staging_radar_data, loading_dim_time, transforming_element_key_dict)


            # LOAD
            loading_dim_element = task_transform_load_dim_element(extracting_staging_radar_sweep)
            loading_fact_radar_sweep = task_load_fact_radar_sweep(transforming_fact_radar_sweep)
            loading_fact_radar = task_load_fact_radar(transforming_fact_radar)

            #AUDIT LOG
            writing_audit_log_load = task_write_audit_log_load(time_key=loading_dim_time, location_key=loading_dim_location)

            # task order
            result = [loading_dim_time, loading_dim_location, loading_dim_element, loading_fact_radar_sweep, loading_fact_radar] >> writing_audit_log_load

        @task 
        def task_refresh_staging():
            connect = MongoDBHook(conn_id='mongodb')
            collections = ['radar_data', 'radar_location', 'radar_sweep']
            for col in collections:
                connect.delete_many(database='staging_area', collection=col,filter={})
            

        keys = get_time_location_key()
        refreshing_staging=task_refresh_staging()
        group_task_process_batch_file.expand(key=keys) >> refreshing_staging
         
        



    init_loading_staging = group_task_init_load_radar_into_staging()
    init_loading_core = group_task_init_load_radar_into_core()

    init_loading_staging >> init_loading_core


