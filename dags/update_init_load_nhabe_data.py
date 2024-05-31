from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.dates import days_ago
from hooks.mongo_hook import MongoDBHook

from helpers.radar_helper import read_radar_file_path
from helpers.radar_extractor import RadarReader
from helpers.radar_staging_loader import RadarStagingLoader
from helpers.utils import *

from etl.extracting.commons import *
from etl.extracting.era5 import *
from etl.cleaning.cleaner import *

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

with DAG('initial_load_radar_v2', default_args=default_args, schedule_interval='@once') as dag:

    @task_group
    def group_task_init_staging_radar():
        @task
        def get_absolute_file_paths():

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
            def read_radar_info(file_path):
                radar = pyart.io.read_sigmet(file_path)
                reader = RadarReader(radar)
                radar_info = reader.get_radar_info()
                return serialize(radar_info)
            
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
            def load_radar_location(radar_info):
                connect = MongoDBHook(conn_id='mongodb')
                loader = RadarStagingLoader(connect)
                logging.info(f"Loaded radar info: {radar_info}")
                loader.load_radar_location(deserialize(radar_info))

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

            reading_radar_info = read_radar_info(file_path)
            loading_radar_location = load_radar_location(reading_radar_info)
            reading_radar_sweep = read_radar_sweep(file_path)
            loading_radar_sweep = load_radar_sweep(reading_radar_sweep)
            reading_radar_data = read_radar_data(file_path)
            loading_radar_data = load_radar_data(reading_radar_data) 
            writing_adit_log_load = write_audit_log_load(file_path)
            
            return [loading_radar_location, loading_radar_sweep, loading_radar_data] >> writing_adit_log_load

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
            splitted_data = split_era5(coordinates, elements, 20)

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
        
        @task
        def extract_staging_radar():
            connect = MongoDBHook(conn_id='mongodb')
            radar_sweep_data = list(connect.find(
                database='staging_area', 
                collection='radar_sweep', 
                # query={
                #     'time_volume_start': {
                #         "$regex": '^' + date
                #     }
                # },
                projection={
                    '_id': False
                }
            ))
            fact_radar_sweep = []
            dim_time = []
            dim_loc = []
            dim_ele = []
            for radar_sweep in radar_sweep_data:

                location = parse_location_surrogate_key(radar_sweep['radar_location_id'])

                time = time_stamp_to_object(radar_sweep['time_volume_start'])

                element = element_obj_to_dim_element(radar_sweep['element_metadata'])

                del radar_sweep['element_metadata']

                fact_radar_sweep.append(radar_sweep)
                dim_time.append(time)
                dim_loc.append(location)
                dim_ele = dim_ele + element
            
            fact_radar_data = list(connect.find(
                database='staging_area', 
                collection='radar_data', 
                # query={
                #     'time_volume_start': {
                #         "$regex": '^' + date
                #     }
                # },
                projection={
                    '_id': False
                }
            ))

            return {
                'fact_radar_data': fact_radar_data, 
                'fact_radar_sweep': fact_radar_sweep, 
                'dim_loc': dim_loc, 
                'dim_ele': dim_ele, 
                'dim_time': dim_time,
            }

        @task
        def transform_fact_radar(extracted_data):
            fact_radar = extracted_data[0]
            for fact in fact_radar: 
                fact['time_id'] = create_time_surrogate_key(time_stamp_to_object(fact['time_volume_start']))
                del fact['time_volume_start']
            return fact_radar
        
        @task
        def transform_fact_radar_sweep(extracted_data):
            fact_radar_sweep = extracted_data[1]
            for fact in fact_radar_sweep:
                fact['time_id'] = create_time_surrogate_key(time_stamp_to_object(fact['time_volume_start']))
                del fact['time_volume_start']
            return fact_radar_sweep
        
        @task
        def clean_dim_time_radar(extracted_data):
            dict_dim_key_for_fact = deduplicate(extracted_data['dim_time'])
            dim_time_values = list(dict_dim_key_for_fact.values())
            clean_dimension_time_era5(dim_time_values)

            return dict_dim_key_for_fact
   
        
        @task
        def clean_dim_loc_radar(extracted_data):
            dim_loc = deduplicate(extracted_data['dim_loc'])
            dict_dim_key_for_fact = {}
            for dim in dim_loc:
                old_key = create_location_surrogate_key(dim['longitude'], dim['latitude'], dim['altitude'])
                #clean...
                new_dim = dim 
                #end clean
                # new_key = create_location_surrogate_key(new_dim['longitude'], new_dim['latitude'], new_dim['altitude'])
                dict_dim_key_for_fact[old_key] = new_dim
            return dim_loc, dict_dim_key_for_fact
        
        @task
        def clean_dim_ele_radar(extracted_data):
            dict_dim_key_for_fact = deduplicate(extracted_data['dim_ele'])

            return dict_dim_key_for_fact

        @task 
        def assign_sur_key_dim_time_radar(cleaned_time):
            cleaned_dim_time = cleaned_time[0]
            dict_dim_key_for_fact = cleaned_time[1]

            for dim in cleaned_dim_time:
                key = create_time_surrogate_key(dim)
                dim['_id'] = key

            for key, value in dict_dim_key_for_fact.items():
                new_key = create_time_surrogate_key(value)
                dict_dim_key_for_fact[key] = new_key
            return cleaned_dim_time, dict_dim_key_for_fact
        
        @task 
        def assign_sur_key_dim_loc_radar(cleaned_loc):
            cleaned_dim_loc = cleaned_loc[0]

            for dim in cleaned_dim_loc:
                key = create_location_surrogate_key(dim['longitude'], dim['latitude'], dim['altitude'])
                dim['_id'] = key

            for key, value in dict_dim_key_for_fact.items():
                new_key = create_location_surrogate_key(value['longitude'], value['latitude'], value['altitude'])
                dict_dim_key_for_fact[key] = new_key
            return cleaned_dim_loc, 

        @task 
        def assign_sur_key_dim_ele_radar(cleaned_ele):
            cleaned_dim_ele = cleaned_ele[0]
            keys = []
            for dim in cleaned_dim_ele:
                key = create_element_surrogate_key(dim['name'])
                dim['_id'] = key
                keys.append(key)
            return cleaned_dim_ele, keys


        @task
        def extract_staging_era5_single():
            connect = MongoDBHook(conn_id='mongodb')
            
            # return fact_radar_data, fact_radar_sweep, dim_time, dim_loc, dim_ele

        @task
        def clean_dim_time_era5_single(extracted_data):
            dict_dim_key_for_fact = deduplicate(extracted_data['dim_time'])
          
            dim_time_values = list(dict_dim_key_for_fact.values())
            clean_dimension_time_era5(dim_time_values)

            return dict_dim_key_for_fact
        
        @task
        def clean_dim_loc_era5_single(extracted_data):
            dict_dim_key_for_fact = deduplicate(extracted_data['dim_loc'])
            dim_loc_values = list(dict_dim_key_for_fact.values())
            clean_dimension_location_era5(dim_loc_values)
            return dict_dim_key_for_fact

        @task
        def clean_dim_ele_era5_single(extracted_data):
            dict_dim_key_for_fact = deduplicate(extracted_data['dim_ele'])

            return dict_dim_key_for_fact


        @task 
        def assign_sur_key_dim_time_era5_single(cleaned_time):
            dict_dim_key_for_fact = cleaned_time
            clean_dimensions = {}
            for key, value in dict_dim_key_for_fact.items():
                new_key = create_time_surrogate_key(value)
                clean_dimensions[new_key] = value
                dict_dim_key_for_fact[key] = new_key
            return dict_dim_key_for_fact, clean_dimensions
        
        @task 
        def assign_sur_key_dim_loc_era5_single(cleaned_loc):
            dict_dim_key_for_fact = cleaned_loc

            clean_dimensions = {}
            for key, value in dict_dim_key_for_fact.items():
                new_key = create_location_surrogate_key(value['longitude'], value['latitude'], value['altitude'])
                clean_dimensions[new_key] = value
                dict_dim_key_for_fact[key] = new_key
            return dict_dim_key_for_fact, clean_dimensions

        @task 
        def assign_sur_key_dim_ele_era5_single(cleaned_ele):
            dict_dim_key_for_fact = cleaned_ele

            clean_dimensions = {}
            for key, value in dict_dim_key_for_fact.items():
                new_key = create_element_surrogate_key(value['name'])
                clean_dimensions[new_key] = value
                dict_dim_key_for_fact[key] = new_key
            return dict_dim_key_for_fact, clean_dimensions, list(clean_dimensions.keys())

        # merge_dim_time(assign_sur_key_dim_ele_era5_single, assign_sur_key_dim_ele_radar)
        @task 
        def merge_dim_time(*arg):
            lists = []
            hash_list = []
            for source in arg:
                clean_dimensions = source[1]
                cleaned_dim_keys = source[2]
                hash_list.append(clean_dimensions)
                lists.append(cleaned_dim_keys.sort())
            return merge_sorted_lists(lists), hash_list

        @task 
        def get_new_dim_time(merged_dim_time):
            merged_list = merged_dim_time[0]
            hash_list = merged_dim_time[1]
            dims = []
            for clean_dimensions in hash_list:
                dim_obj = {}
                for key, value in clean_dimensions:
                    if(key in merged_list):
                        dim_obj['_id'] = key
                        dim_obj.update(value)
                        dims.append(dim_obj)
            return dims


        @task 
        def get_new_dim_loc(merged_dim_loc):
            merged_list = merged_dim_loc[0]
            hash_list = merged_dim_loc[1]
            dims = []
            for clean_dimensions in hash_list:
                dim_obj = {}
                for key, value in clean_dimensions:
                    if(key in merged_list):
                        dim_obj['_id'] = key
                        dim_obj.update(value)
                        dims.append(dim_obj)
            return dims

        @task 
        def get_new_dim_ele(merged_dim_ele):
            merged_list = merged_dim_ele[0]
            hash_list = merged_dim_ele[1]
            dims = []
            for clean_dimensions in hash_list:
                dim_obj = {}
                for key, value in clean_dimensions:
                    if(key in merged_list):
                        dim_obj['_id'] = key
                        dim_obj.update(value)
                        dims.append(dim_obj)
            return dims




            


        @task 
        def merge_dim_loc():
            lists = []
            hash_list = []
            for source in arg:
                clean_dimensions = source[1]
                cleaned_dim_keys = source[2]
                hash_list.append(clean_dimensions)
                lists.append(cleaned_dim_keys.sort())
            return merge_sorted_lists(lists), hash_list

        @task 
        def merge_dim_ele():
            lists = []
            hash_list = []
            for source in arg:
                clean_dimensions = source[1]
                cleaned_dim_keys = source[2]
                hash_list.append(clean_dimensions)
                lists.append(cleaned_dim_keys.sort())
            return merge_sorted_lists(lists), hash_list


        @task
        def load_core(**kwargs):
            for key, value in kwargs.items():
                if(key=='dimension_time'):
                    pass
                    
        

            

        @task 
        def end(data1, data2):
            print('task_end', data1[0], data2[0])
            pass
    
        #radar
        extracting_staging_radar = extract_staging_radar()

        transforming_fact_radar = transform_fact_radar(extracting_staging_radar)
        transforming_fact_radar_sweep = transform_fact_radar_sweep(extracting_staging_radar)
        cleaning_dim_time_radar = clean_dim_time_radar(extracting_staging_radar)
        cleaning_dim_loc_radar = clean_dim_loc_radar(extracting_staging_radar)
        cleaning_dim_ele_radar = clean_dim_ele_radar(extracting_staging_radar)

        assigning_sur_key_dim_time_radar = assign_sur_key_dim_time_radar(cleaning_dim_time_radar)
        assigning_sur_key_dim_loc_radar = assign_sur_key_dim_loc_radar(cleaning_dim_loc_radar)
        assigning_sur_key_dim_ele_radar = assign_sur_key_dim_ele_radar(cleaning_dim_ele_radar)
        #era5 single
        extracting_staging_era5_single = extract_staging_era5_single()
        cleaning_dim_time_era5_single = clean_dim_time_era5_single(extracting_staging_era5_single)
        cleaning_dim_loc_era5_single = clean_dim_loc_era5_single(extracting_staging_era5_single)
        cleaning_dim_ele_era5_single = clean_dim_ele_era5_single(extracting_staging_era5_single)

        assigning_sur_key_dim_time_era5_single = assign_sur_key_dim_time_era5_single(cleaning_dim_time_era5_single)
        assigning_sur_key_dim_loc_era5_single = assign_sur_key_dim_loc_era5_single(cleaning_dim_loc_era5_single)
        assigning_sur_key_dim_ele_era5_single = assign_sur_key_dim_ele_era5_single(cleaning_dim_ele_era5_single)

        merging_dim_time = merge_dim_time(
            assigning_sur_key_dim_time_radar, 
            assigning_sur_key_dim_time_era5_single
        )

        merging_dim_loc = merge_dim_loc(
            assigning_sur_key_dim_loc_radar, 
            assigning_sur_key_dim_loc_era5_single
        )

        merging_dim_ele = merge_dim_ele(
            assigning_sur_key_dim_ele_radar, 
            assigning_sur_key_dim_ele_era5_single
        )
        

        #radar
        extracting_staging_radar >> transforming_fact_radar
        extracting_staging_radar >> transforming_fact_radar_sweep
        extracting_staging_radar >> cleaning_dim_time_radar
        extracting_staging_radar >> cleaning_dim_loc_radar
        extracting_staging_radar >> cleaning_dim_ele_radar

        cleaning_dim_time_radar >> assigning_sur_key_dim_time_radar
        cleaning_dim_loc_radar >> assigning_sur_key_dim_loc_radar
        cleaning_dim_ele_radar >> assigning_sur_key_dim_ele_radar


        assigning_sur_key_dim_time_radar >> merging_dim_time
        assigning_sur_key_dim_loc_radar >> merging_dim_loc
        assigning_sur_key_dim_ele_radar >> merging_dim_ele

        #era5_single

        extracting_staging_era5_single >> cleaning_dim_time_era5_single
        extracting_staging_era5_single >> cleaning_dim_loc_era5_single
        extracting_staging_era5_single >> cleaning_dim_ele_era5_single

        cleaning_dim_time_era5_single >> assigning_sur_key_dim_time_era5_single
        cleaning_dim_loc_era5_single >> assigning_sur_key_dim_loc_era5_single
        cleaning_dim_ele_era5_single >> assigning_sur_key_dim_ele_era5_single


        assigning_sur_key_dim_time_era5_single >> merging_dim_time
        assigning_sur_key_dim_loc_era5_single >> merging_dim_loc
        assigning_sur_key_dim_ele_era5_single >> merging_dim_ele

        end = end(extracting_staging_radar,extracting_staging_era5_single)

        merging_dim_time >> end
        merging_dim_loc >> end
        merging_dim_ele >> end

        transforming_fact_radar >> end
        transforming_fact_radar_sweep >> end





    


        
        
         

    @task 
    def refresh_staging():
        connect = MongoDBHook(conn_id='mongodb')
        # collections = ['radar_data', 'radar_location', 'radar_sweep']
        # for col in collections:
        #     connect.delete_many(database='staging_area', collection=col,filter={})
            


    init_loading_staging_radar = group_task_init_staging_radar()
    init_loading_staing_era5 = group_task_init_staging_era5_single()
    init_loading_core = group_task_init_core()
    refreshing_staging = refresh_staging()


    [init_loading_staging_radar, init_loading_staing_era5]  >> init_loading_core >> refreshing_staging


