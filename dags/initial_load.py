from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.dates import days_ago

from hooks.mongo_hook import MongoDBHook

from etl.extracting.source_api import *
from etl.extracting.era5 import *
from etl.extracting.radar import *
from etl.extracting.utils import *

from etl.cleaning.cleaner import *
from etl.cleaning.merger import *

from etl.delivering.surrogate_key import *
from etl.delivering.radar import *


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

with DAG('initial_load', default_args=default_args, schedule_interval='@once') as dag:

    @task_group
    def group_task_init_staging_radar():
        # pass
        @task
        def get_absolute_file_paths():

            connect = MongoDBHook(conn_id='mongodb')
            source_info = {
                'source': 'NHABE',
                'source_type': 'static_file'
            }

            @write_audit_log_file
            def excecute(connect: MongoDBHook, source_info: dict):
                file_paths = get_absolute_file_path()
                return file_paths

            return excecute(connect=connect, source_info=source_info)

        
        @task_group(group_id='group_task_etl_on_each_file')
        def group_task_etl_on_each_file(file_path):



            
            
            @task 
            def read_radar_sweep(file_path):
                radar = pyart.io.read_sigmet(file_path)
                reader = RadarReader(radar)
                radar_sweep = reader.get_radar_sweep()
                return serialize(radar_sweep)
            
            @task 
            def read_radar_data(file_path):
                radar = pyart.io.read_sigmet(file_path)
                reader = RadarReader(radar)
                radar_data = reader.get_radar_data()
                return serialize(radar_data)
            
            

            @task
            def load_radar_sweep(radar_sweep):
                connect = MongoDBHook(conn_id='mongodb')
                loader = RadarStagingLoader(connect)
                loader.load_radar_sweep(deserialize(radar_sweep))
                logging.info(f"Loaded radar sweep info: {radar_sweep}")

            @task
            def load_radar_data(radar_data):
                connect = MongoDBHook(conn_id='mongodb')
                loader = RadarStagingLoader(connect)
                loader.load_radar_data(deserialize(radar_data))
                logging.info(f"Loaded radar data: {radar_data}")
            
            @task
            def write_audit_log_load(file_path):
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

            reading_radar_sweep = read_radar_sweep(file_path)
            loading_radar_sweep = load_radar_sweep(reading_radar_sweep)
            reading_radar_data = read_radar_data(file_path)
            loading_radar_data = load_radar_data(reading_radar_data) 
            writing_adit_log_load = write_audit_log_load(file_path)
            
            return [ loading_radar_sweep, loading_radar_data] >> writing_adit_log_load

        file_paths = get_absolute_file_paths()
        group_task_etl_on_each_file.expand(file_path=file_paths)

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
            splitted_data = split_era5(coordinates, elements, 10)

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

    @task_group
    def group_task_init_staging_era5_pressure():

        @task 
        def read_era5_pressure():
            files = get_files_in_directory(SOURCES_PATH + 'era5-pressure')
            coordinates, elements = get_era5_data(files[0])
            return coordinates, elements
        
        @task 
        def split_era5_pressure(era5_data):
            coordinates = era5_data[0]
            elements = era5_data[1]
            splitted_data = split_era5(coordinates, elements, 10)

            return splitted_data

        @task 
        def load_era5_pressure(documents):
            connect = MongoDBHook(conn_id='mongodb')
            connect.insert_one(
                database='staging_area', 
                collection='era5_pressure', 
                document=documents[0]
            )
        
        reading_era5 = read_era5_pressure()
        splitting_era5 = split_era5_pressure(reading_era5)
        loading_era5 = load_era5_pressure(splitting_era5)

        reading_era5 >> splitting_era5 >> loading_era5

    @task_group
    def group_task_init_core():
        
        #radar
        @task
        def extract_staging_radar():
            connect = MongoDBHook(conn_id='mongodb')
            radar_sweep_data = list(connect.find(
                database='staging_area', 
                collection='radar_sweep', 
           
                projection={
                    '_id': False
                }
            ))
            
            radar_data = list(connect.find(
                database='staging_area', 
                collection='radar_data', 
                projection={
                    '_id': False
                }
                
            ))

            return extract_staging_radar_info(
                fact_radar_sweep_documents=radar_sweep_data,
                fact_radar_documents=radar_data
            )


          
        
        @task
        def clean_dim_time_radar(extracted_data):
            cleaned_dim_time = deduplicate(extracted_data['dimension_time'])
            

            return cleaned_dim_time
      
        @task
        def clean_dim_loc_radar(extracted_data):
            cleaned_dim_loc = deduplicate(extracted_data['dimension_location'])
            return cleaned_dim_loc
        
        @task
        def clean_dim_ele_radar(extracted_data):
            cleaned_dim_ele = deduplicate(extracted_data['dimension_element'])
            return cleaned_dim_ele

        @task 
        def assign_sur_key_dim_time_radar(dirty_to_clean, dim_type):
            return assign_dimension_key(dirty_to_clean, dim_type)

        @task 
        def assign_sur_key_dim_loc_radar(dirty_to_clean, dim_type):
            return assign_dimension_key(dirty_to_clean, dim_type)

        @task 
        def assign_sur_key_dim_ele_radar(dirty_to_clean, dim_type):
            return assign_dimension_key(dirty_to_clean, dim_type)
            
        #era5_single
        @task
        def extract_staging_era5_single():
            connect = MongoDBHook(conn_id='mongodb')
            documents = list(connect.find(database='staging_area', collection='era5_single'))
            return extract_staging_era5(documents)

        @task
        def clean_dim_time_era5_single(extracted_data):
            dict_dim_key_for_fact = deduplicate(extracted_data['dimension_time'])
            dim_time_values = list(dict_dim_key_for_fact.values())
            clean_dimension_time_era5(dim_time_values)
            return dict_dim_key_for_fact
        
        @task
        def clean_dim_loc_era5_single(extracted_data):
            dict_dim_key_for_fact = deduplicate(extracted_data['dimension_location'])
            dim_loc_values = list(dict_dim_key_for_fact.values())
            clean_dimension_location_era5(dim_loc_values)
            return dict_dim_key_for_fact

        @task
        def clean_dim_ele_era5_single(extracted_data):
            dict_dim_key_for_fact = deduplicate(extracted_data['dimension_element'])
            return dict_dim_key_for_fact

        @task 
        def assign_sur_key_dim_time_era5_single(dirty_to_clean, dim_type):
            # Used for fact link to dimension
            return assign_dimension_key(dirty_to_clean, dim_type)
  
        @task 
        def assign_sur_key_dim_loc_era5_single(dirty_to_clean, dim_type):
            return assign_dimension_key(dirty_to_clean, dim_type)

        @task 
        def assign_sur_key_dim_ele_era5_single(dirty_to_clean, dim_type):
            return assign_dimension_key(dirty_to_clean, dim_type)
            
        #era5_pressure
        @task
        def extract_staging_era5_pressure():
            connect = MongoDBHook(conn_id='mongodb')
            documents = list(connect.find(database='staging_area', collection='era5_pressure'))
            return extract_staging_era5(documents)

        @task
        def clean_dim_time_era5_pressure(extracted_data):
            dict_dim_key_for_fact = deduplicate(extracted_data['dimension_time'])
            dim_time_values = list(dict_dim_key_for_fact.values())
            clean_dimension_time_era5(dim_time_values)
            return dict_dim_key_for_fact
        
        @task
        def clean_dim_loc_era5_pressure(extracted_data):
            dict_dim_key_for_fact = deduplicate(extracted_data['dimension_location'])
            dim_loc_values = list(dict_dim_key_for_fact.values())
            clean_dimension_location_era5(dim_loc_values)
            return dict_dim_key_for_fact

        @task
        def clean_dim_ele_era5_pressure(extracted_data):
            dict_dim_key_for_fact = deduplicate(extracted_data['dimension_element'])
            return dict_dim_key_for_fact

        @task 
        def assign_sur_key_dim_time_era5_pressure(dirty_to_clean, dim_type):
            # Used for fact link to dimension
            return assign_dimension_key(dirty_to_clean, dim_type)
  
        @task 
        def assign_sur_key_dim_loc_era5_pressure(dirty_to_clean, dim_type):
            return assign_dimension_key(dirty_to_clean, dim_type)

        @task 
        def assign_sur_key_dim_ele_era5_pressure(dirty_to_clean, dim_type):
            return assign_dimension_key(dirty_to_clean, dim_type)

        #common
        @task 
        def merge_dim_time(*arg):
            keys = [item['keys'] for item in arg]
            return merge_sorted_dimension_from_all(keys)

        @task 
        def merge_dim_loc(*arg):
            keys = [item['keys'] for item in arg]
            return merge_sorted_dimension_from_all(keys)
        @task 
        def merge_dim_ele(*arg):
            keys = [item['keys'] for item in arg]
            return merge_sorted_dimension_from_all(keys)

        @task 
        def get_loadable_dimension_time_documents(key_to_clean_all, keys):
            key_all = [item['key_to_clean'] for item in key_to_clean_all]
            connect = MongoDBHook(conn_id='mongodb')
            loadable_keys = []
            for key in keys:
                if not connect.find_one(database='core',collection='dimension_time',query={'_id': key}):
                    loadable_keys.append(key)

            documents = []
            for key in loadable_keys:
                for key_to_clean_one in key_all:
                    if key in key_to_clean_one:
                        document = key_to_clean_one[key]
                        document['_id'] = key
                        documents.append(document)
            
            return documents

        @task 
        def get_loadable_dimension_location_documents(key_to_clean_all, keys):
            key_all = [item['key_to_clean'] for item in key_to_clean_all]
            connect = MongoDBHook(conn_id='mongodb')
            loadable_keys = []
            for key in keys:
                if not connect.find_one(database='core',collection='dimension_location',query={'_id': key}):
                    loadable_keys.append(key)

            documents = []
            for key in loadable_keys:
                for key_to_clean_one in key_all:
                    if key in key_to_clean_one:
                        document = key_to_clean_one[key]
                        document['_id'] = key
                        documents.append(document)
            
            return documents

        @task 
        def get_loadable_dimension_element_documents(key_to_clean_all, keys):
            key_all = [item['key_to_clean'] for item in key_to_clean_all]
            connect = MongoDBHook(conn_id='mongodb')
            loadable_keys = []
            for key in keys:
                if not connect.find_one(database='core',collection='dimension_element',query={'_id': key}):
                    loadable_keys.append(key)

            documents = []
            for key in loadable_keys:
                for key_to_clean_one in key_all:
                    if key in key_to_clean_one:
                        document = key_to_clean_one[key]
                        document['_id'] = key
                        documents.append(document)
            return documents

        @task 
        def load_core_dimension(**kwarg):
            connect = MongoDBHook(conn_id='mongodb')
            for collection, documents in kwarg.items():
                if isinstance(documents, list) and len(documents) > 0:
                    connect.insert_many(database='core', collection=collection, documents=documents)
                
        @task 
        def clean_fact_radar(extracted_data):
            return extracted_data['fact_radar']
            
        @task 
        def clean_fact_radar_sweep(extracted_data):
            return extracted_data['fact_radar_sweep']

        @task 
        def clean_fact_era5_single(extracted_data):
            return extracted_data['fact_era5']

        @task 
        def clean_fact_era5_pressure(extracted_data):
            return extracted_data['fact_era5']

        

        @task 
        def lookup_dimension_keys_era5_single(
            fact_era5_single, 
            dirty_to_key_element, 
            dirty_to_key_time, 
            dirty_to_key_location
        ):
            return lookup_dimension_keys(
                fact_era5_single, 
                dirty_to_key_element['dirty_to_key'], 
                dirty_to_key_time['dirty_to_key'], 
                dirty_to_key_location['dirty_to_key']
            )

        @task 
        def lookup_dimension_keys_era5_pressure(
            fact_era5_pressure, 
            dirty_to_key_element, 
            dirty_to_key_time, 
            dirty_to_key_location
        ):
            return lookup_dimension_keys(
                fact_era5_pressure, 
                dirty_to_key_element['dirty_to_key'], 
                dirty_to_key_time['dirty_to_key'], 
                dirty_to_key_location['dirty_to_key']
            )
        
        @task 
        def lookup_dimension_keys_radar(
            fact_radar, 
            fact_radar_sweep,
            dirty_to_key_element, 
            dirty_to_key_time, 
            dirty_to_key_location
        ):
            return {
                'fact_radar': lookup_dimension_keys(
                    fact_radar, 
                    dirty_to_key_element['dirty_to_key'], 
                    dirty_to_key_time['dirty_to_key'], 
                    dirty_to_key_location['dirty_to_key']
                ), 
                'fact_radar_sweep': lookup_dimension_keys_radar_sweep(
                    fact_radar_sweep, 
                    dirty_to_key_time['dirty_to_key'], 
                    dirty_to_key_location['dirty_to_key']
                ),
            }

        @task 
        def merge_fact_era5(*arg):
            fact_era5 = []
            for fact in arg:
                fact_era5 += fact
            return fact_era5
        
        @task 
        def merge_fact_radar(fact_radar):
            return fact_radar
        
        @task 
        def load_core_fact_radar(*arg):
            connect = MongoDBHook(conn_id='mongodb')

            for merged_fact in arg:
                for collection, documents in merged_fact.items():
                    if isinstance(documents, list) and len(documents) > 0:
                        connect.insert_many(database='core', collection=collection, documents=documents)

        @task 
        def load_core_fact_era5( all_fact_era5):
            connect = MongoDBHook(conn_id='mongodb')
            loadable_fact = []
            for document in all_fact_era5:
                
                print('hâh',type(document))

                query = {
                            'element_id': document['element_id'], 
                            'time_id': document['time_id'], 
                            'location_id': document['location_id']
                        }
                
                if not connect.find_one(database='core', collection='fact_era5',query=query):
                    document['value'] = document['value'][0]
                    loadable_fact.append(document)

            if loadable_fact:
                connect.insert_many(database='core', collection='fact_era5',documents=loadable_fact)

    
    
        #radar
        extracting_staging_radar = extract_staging_radar()
        
        cleaning_dim_time_radar = clean_dim_time_radar(extracting_staging_radar)
        cleaning_dim_loc_radar = clean_dim_loc_radar(extracting_staging_radar)
        cleaning_dim_ele_radar = clean_dim_ele_radar(extracting_staging_radar)
        cleaning_fact_radar = clean_fact_radar(extracting_staging_radar)
        cleaning_fact_radar_sweep = clean_fact_radar_sweep(extracting_staging_radar)

        assigning_sur_key_dim_time_radar = assign_sur_key_dim_time_radar(cleaning_dim_time_radar, 'time')
        assigning_sur_key_dim_loc_radar = assign_sur_key_dim_loc_radar(cleaning_dim_loc_radar, 'location')
        assigning_sur_key_dim_ele_radar = assign_sur_key_dim_ele_radar(cleaning_dim_ele_radar, 'element')

        looking_dimension_keys_radar = lookup_dimension_keys_radar(
            cleaning_fact_radar,
            cleaning_fact_radar_sweep, 
            assigning_sur_key_dim_ele_radar, 
            assigning_sur_key_dim_time_radar, 
            assigning_sur_key_dim_loc_radar
        )


        #era5 single
        extracting_staging_era5_single = extract_staging_era5_single()

        cleaning_dim_time_era5_single = clean_dim_time_era5_single(extracting_staging_era5_single)
        cleaning_dim_loc_era5_single = clean_dim_loc_era5_single(extracting_staging_era5_single)
        cleaning_dim_ele_era5_single = clean_dim_ele_era5_single(extracting_staging_era5_single)
        cleaning_fact_era5_single = clean_fact_era5_single(extracting_staging_era5_single)

        assigning_sur_key_dim_time_era5_single = assign_sur_key_dim_time_era5_single(cleaning_dim_time_era5_single, 'time')
        assigning_sur_key_dim_loc_era5_single = assign_sur_key_dim_loc_era5_single(cleaning_dim_loc_era5_single, 'location')
        assigning_sur_key_dim_ele_era5_single = assign_sur_key_dim_ele_era5_single(cleaning_dim_ele_era5_single, 'element')

        looking_dimension_keys_era5_single = lookup_dimension_keys_era5_single(
            cleaning_fact_era5_single, 
            assigning_sur_key_dim_ele_era5_single, 
            assigning_sur_key_dim_time_era5_single, 
            assigning_sur_key_dim_loc_era5_single
        )

        #era5 pressure
        extracting_staging_era5_pressure = extract_staging_era5_pressure()

        cleaning_dim_time_era5_pressure = clean_dim_time_era5_pressure(extracting_staging_era5_pressure)
        cleaning_dim_loc_era5_pressure = clean_dim_loc_era5_pressure(extracting_staging_era5_pressure)
        cleaning_dim_ele_era5_pressure = clean_dim_ele_era5_pressure(extracting_staging_era5_pressure)
        cleaning_fact_era5_pressure = clean_fact_era5_pressure(extracting_staging_era5_pressure)


        assigning_sur_key_dim_time_era5_pressure = assign_sur_key_dim_time_era5_pressure(cleaning_dim_time_era5_pressure, 'time')
        assigning_sur_key_dim_loc_era5_pressure = assign_sur_key_dim_loc_era5_pressure(cleaning_dim_loc_era5_pressure, 'location')
        assigning_sur_key_dim_ele_era5_pressure = assign_sur_key_dim_ele_era5_pressure(cleaning_dim_ele_era5_pressure, 'element')

        looking_dimension_keys_era5_pressure = lookup_dimension_keys_era5_pressure(
            cleaning_fact_era5_pressure, 
            assigning_sur_key_dim_ele_era5_pressure, 
            assigning_sur_key_dim_time_era5_pressure, 
            assigning_sur_key_dim_loc_era5_pressure
        )

        #common
        merging_dim_time = merge_dim_time(
            assigning_sur_key_dim_time_radar, 
            assigning_sur_key_dim_time_era5_single,
            assigning_sur_key_dim_time_era5_pressure,
        )

        merging_dim_loc = merge_dim_loc(
            assigning_sur_key_dim_loc_radar, 
            assigning_sur_key_dim_loc_era5_single,
            assigning_sur_key_dim_loc_era5_pressure,
        )

        merging_dim_ele = merge_dim_ele(
            assigning_sur_key_dim_ele_radar, 
            assigning_sur_key_dim_ele_era5_single,
            assigning_sur_key_dim_ele_era5_pressure,
        )

        merging_fact_radar = merge_fact_radar(looking_dimension_keys_radar)
        merging_fact_era5 = merge_fact_era5(
            looking_dimension_keys_era5_single, 
            looking_dimension_keys_era5_pressure
        )


        getting_new_time_documents = get_loadable_dimension_time_documents(
            [
                assigning_sur_key_dim_time_radar, 
                assigning_sur_key_dim_time_era5_single,
                assigning_sur_key_dim_time_era5_pressure
            ], 
            merging_dim_time
        )

        getting_new_location_documents = get_loadable_dimension_location_documents(
            [
                assigning_sur_key_dim_loc_radar, 
                assigning_sur_key_dim_loc_era5_single,
                assigning_sur_key_dim_loc_era5_pressure

            ], 
            merging_dim_loc
        )
        getting_new_element_documents = get_loadable_dimension_element_documents(
            [
                assigning_sur_key_dim_ele_radar, 
                assigning_sur_key_dim_ele_era5_single,
                assigning_sur_key_dim_ele_era5_pressure

            ], 
            merging_dim_ele
        )

        loading_core_dimension = load_core_dimension(
            dimension_element=getting_new_element_documents,
            dimension_location=getting_new_location_documents,
            dimension_time=getting_new_time_documents,
        )

        loading_core_fact_era5 = load_core_fact_era5(merging_fact_era5)
        loading_core_fact_radar = load_core_fact_radar(merging_fact_radar)

        #radar
  
        extracting_staging_radar >> [cleaning_dim_time_radar, cleaning_dim_loc_radar, cleaning_dim_ele_radar, cleaning_fact_radar, cleaning_fact_radar_sweep]


        cleaning_dim_time_radar >> assigning_sur_key_dim_time_radar
        cleaning_dim_loc_radar >> assigning_sur_key_dim_loc_radar
        cleaning_dim_ele_radar >> assigning_sur_key_dim_ele_radar


        assigning_sur_key_dim_time_radar >> merging_dim_time
        assigning_sur_key_dim_loc_radar >> merging_dim_loc
        assigning_sur_key_dim_ele_radar >> merging_dim_ele

        
        # [cleaning_fact_era5_single, assigning_sur_key_dim_ele_era5_single, assigning_sur_key_dim_time_era5_single, assigning_sur_key_dim_loc_era5_single] >> looking_dimension_keys_era5_single

        #era5_single

        extracting_staging_era5_single >> [cleaning_dim_time_era5_single ,cleaning_dim_loc_era5_single, cleaning_dim_ele_era5_single, cleaning_fact_era5_single]
    

        cleaning_dim_time_era5_single >> assigning_sur_key_dim_time_era5_single
        cleaning_dim_loc_era5_single >> assigning_sur_key_dim_loc_era5_single
        cleaning_dim_ele_era5_single >> assigning_sur_key_dim_ele_era5_single


        assigning_sur_key_dim_time_era5_single >> merging_dim_time
        assigning_sur_key_dim_loc_era5_single >> merging_dim_loc
        assigning_sur_key_dim_ele_era5_single >> merging_dim_ele

        [cleaning_fact_era5_single, assigning_sur_key_dim_ele_era5_single, assigning_sur_key_dim_time_era5_single, assigning_sur_key_dim_loc_era5_single] >> looking_dimension_keys_era5_single

        #era5_pressure

        extracting_staging_era5_pressure >> [cleaning_dim_time_era5_pressure ,cleaning_dim_loc_era5_pressure, cleaning_dim_ele_era5_pressure, cleaning_fact_era5_pressure]
    

        cleaning_dim_time_era5_pressure >> assigning_sur_key_dim_time_era5_pressure
        cleaning_dim_loc_era5_pressure >> assigning_sur_key_dim_loc_era5_pressure
        cleaning_dim_ele_era5_pressure >> assigning_sur_key_dim_ele_era5_pressure


        assigning_sur_key_dim_time_era5_pressure >> merging_dim_time
        assigning_sur_key_dim_loc_era5_pressure >> merging_dim_loc
        assigning_sur_key_dim_ele_era5_pressure >> merging_dim_ele

        [cleaning_fact_era5_pressure, assigning_sur_key_dim_ele_era5_pressure,  assigning_sur_key_dim_time_era5_pressure, assigning_sur_key_dim_loc_era5_pressure] >> looking_dimension_keys_era5_pressure

        #common

        merging_dim_time >> getting_new_time_documents
        merging_dim_loc >> getting_new_location_documents
        merging_dim_ele >> getting_new_element_documents

        getting_new_time_documents >> loading_core_dimension
        getting_new_location_documents >> loading_core_dimension
        getting_new_element_documents >> loading_core_dimension

        loading_core_dimension >> looking_dimension_keys_radar
        loading_core_dimension >> looking_dimension_keys_era5_pressure
        loading_core_dimension >> looking_dimension_keys_era5_single

        looking_dimension_keys_radar >> merging_fact_radar
        [looking_dimension_keys_era5_pressure, looking_dimension_keys_era5_single] >> merging_fact_era5

        merging_fact_era5 >> loading_core_fact_era5
        merging_fact_radar >> loading_core_fact_radar

     
    @task 
    def refresh_staging():
        connect = MongoDBHook(conn_id='mongodb')
        collections = ['radar_data', 'era5_pressure', 'era5_single', 'radar_sweep']
        for col in collections:
            connect.delete_many(database='staging_area', collection=col,filter={})
            
    init_loading_staging_radar = group_task_init_staging_radar()
    init_loading_staing_era5_single = group_task_init_staging_era5_single()
    init_loading_staing_era5_pressure = group_task_init_staging_era5_pressure()
    init_loading_core = group_task_init_core()
    refreshing_staging = refresh_staging()

    [init_loading_staging_radar, init_loading_staing_era5_single, init_loading_staing_era5_pressure,]  >> init_loading_core >> refreshing_staging


