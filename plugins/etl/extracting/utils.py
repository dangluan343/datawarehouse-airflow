from datetime import datetime

def assign_keys(data, key_names, key_values):
    """
    Hàm này gán các trường mới vào mỗi đối tượng trong mảng.

    Parameters:
    - data(list): Mảng các đối tượng (dict).
    - key_names(list): Mảng tên các trường mới cần gán.
    - key_values(list): Mảng giá trị của trường mới.

    Returns:
    Không có giá trị trả về. Hàm thay đổi trực tiếp các đối tượng trong mảng data.
    """
    for obj in data:
        for key, value in zip(key_names, key_values):
            obj[key] = value

import json



#%%
#INPUT: time: string like 2020-07-01T00:00:10Z 
def extract_date_time(time):
    # Parse the input time string into a datetime object
    time_obj = datetime.strptime(time.strip(), "%Y-%m-%dT%H:%M:%SZ")
    date_obj = {
        'year': time_obj.year,
        'month': time_obj.month,
        'day': time_obj.day,
        'hour': time_obj.hour,
        'minute': time_obj.minute,
        'second': time_obj.second,
    }
    return date_obj
    
print(extract_date_time('2020-07-01T00:00:10Z '))
#%%

def parse_location_surrogate_key(location_surrogate_key):
    parts = location_surrogate_key.split('_')
    return {
        'latitude': parts[1],
        'longitude': parts[2],
        'altitude': parts[3],
    }

def time_stamp_to_object(date_time):
    """
    Hàm này chuyển đổi một thời gian dạng chuỗi thành một đối tượng chứa các thành phần của thời gian.
    
    Args:
    date_time (str): Chuỗi thời gian, định dạng 'yyyy-mm-ddTHH:mm:ssZ'.
    
    Returns:
    dict: Đối tượng chứa các thành phần của thời gian.
    """
    # Chuyển đổi chuỗi thời gian thành đối tượng datetime
    dt_obj = datetime.strptime(date_time, '%Y-%m-%dT%H:%M:%SZ')
    
    # Trích xuất các thành phần của thời gian
    time_object = {
        "year": dt_obj.year,
        "month": dt_obj.month,
        "day": dt_obj.day,
        "hour": dt_obj.hour,
        "minute": dt_obj.minute,
        "second": dt_obj.second,
    }
    
    return time_object

def element_obj_to_dim_element(ele_obj):
    dim_ele = []
    for key, value in ele_obj.items():
        dim_ele.append(value)
    return dim_ele

# %%
import os

#%%
def get_absolute_file_path(data_dir='/Users/luanluan/Documents/Data/dw_airflow_2/dags/sources/radar'):
    current_files = set()
    for root, _, files in os.walk(data_dir):
        for file in files:
            if(file == '.DS_Store'):
                continue
            current_files.add(os.path.join(root, file))
    
    return list(current_files)

def get_file_name_from_absolute_path(path):
    return path.split('/')[-1]
# %%


