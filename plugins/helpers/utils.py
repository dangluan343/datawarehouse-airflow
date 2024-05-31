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

import hashlib
import json

def calculate_checksum_for_json(data):
    """
    Tính toán checksum của một kiểu dữ liệu bất kỳ bằng thuật toán SHA-256.
    
    Parameters:
    - data(list | dict | any): Dữ liệu cần tính toán checksum. Đây có thể là một dictionary, một list, hoặc bất kỳ kiểu dữ liệu nào khác có thể biểu diễn dưới dạng chuỗi.
    
    Returns:
    - checksum(str): Giá trị checksum của dữ liệu.
    """
    # Chuyển đổi dữ liệu thành chuỗi JSON
    json_data = json.dumps(data, sort_keys=True)
    
    # Tính toán checksum của chuỗi JSON bằng thuật toán SHA-256
    sha256 = hashlib.sha256()
    sha256.update(json_data.encode('utf-8'))
    return sha256.hexdigest()

import hashlib
import pickle

def calculate_checksum_for_obt(data):
    """
    Tính toán checksum của một kiểu dữ liệu bất kỳ bằng thuật toán SHA-256.
    
    Parameters:
    - data: Dữ liệu cần tính toán checksum. Đây có thể là một đối tượng tùy chỉnh hoặc bất kỳ kiểu dữ liệu nào khác không thể biểu diễn dưới dạng JSON.
    
    Returns:
    - checksum(str): Giá trị checksum của dữ liệu.
    """
    # Biểu diễn đối tượng dưới dạng chuỗi bằng cách sử dụng pickle
    serialized_data = pickle.dumps(data)
    
    # Tính toán checksum của chuỗi bằng thuật toán SHA-256
    sha256 = hashlib.sha256()
    sha256.update(serialized_data)
    return sha256.hexdigest()


# # Tính toán checksum cho dữ liệu
# checksum = calculate_checksum(your_data)

# # In giá trị checksum
# print(checksum)

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
def create_time_surrogate_key(date_obj):
    """
    Creates a surrogate key for time based on the provided date object.

    Parameters:
    - date_obj (dict): A dictionary containing year, month, day, hour, minute, and second.

    Returns:
    - str: A surrogate key formatted as 't_year_month_day_hour_minute_second'.
    """
    return f"t_{date_obj['year']}{date_obj['month']}{date_obj['day']}{date_obj['hour']}{date_obj['minute']}{date_obj['second']}"

def create_element_surrogate_key(element_name):
    return f'e_{element_name}'

def create_location_surrogate_key(longitude, latitude, altitude):
    return f'l_{latitude}_{longitude}_{altitude}'

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
import json

def remove_duplicates(dict_list):
    seen = set()
    unique_list = []
    duplicate_list = []
    duplicates_count = 0

    for d in dict_list:
        j = json.dumps(d, sort_keys=True)
        if j not in seen:
            seen.add(j)
            unique_list.append(d)
        else:
            duplicate_list.append(d)
            duplicates_count += 1

    return unique_list, duplicates_count, duplicate_list

# Example usage
# dict_list = [
#     {'a': 1, 'b': 2},
#     {'b': 2, 'a': 1},
#     {'a': 1, 'b': 3},
#     {'a': 1, 'b': 2},
#     {'c': 3, 'd': 4},
#     {'d': 4, 'c': 3}
# ]

# unique_dict_list, duplicates_count, duplicate_dicts = remove_duplicates(dict_list)
# print("Unique List:", unique_dict_list)
# print("Number of Duplicates:", duplicates_count)
# print("Duplicate Objects:", duplicate_dicts)

#%%
import os
from datetime import datetime

def get_files_with_dates(directory):
    files_list = []

    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            date_added = datetime.fromtimestamp(os.path.getctime(file_path)).strftime('%Y-%m-%d %H:%M:%S')
            files_list.append({'file_path': file_path, 'file_name': file, 'date_added': date_added})

    return files_list

# Thay đổi đường dẫn thư mục của bạn tại đây
directory_path = '/Users/luanluan/Documents/Data/dw_airflow_2/data/radar'

# # Lấy danh sách các tệp trong thư mục và các thư mục con kèm theo ngày thêm vào
# files_with_dates = get_files_with_dates(directory_path)

# # In danh sách các tệp kèm theo ngày thêm vào
# for file_info in files_with_dates:
#     print(file_info)


# input: datetime_strings = ['2020-07-01T00:00:10Z', '2020-07-01T00:03:10Z', '2020-06-01T00:00:10Z', '2021-06-01T00:00:10Z']

def extract_unique_dates(datetime_strings):
    """
    Hàm này nhận vào một danh sách các chuỗi datetime và trả về một danh sách các ngày duy nhất trong định dạng 'yyyy-mm-dd'.
    
    Args:
    datetime_strings (list): Danh sách các chuỗi datetime.
    Vi du: ['2020-07-01T00:00:10Z', '2020-07-01T00:03:10Z', '2020-06-01T00:00:10Z', '2021-06-01T00:00:10Z']
    
    Returns:
    list: Danh sách các ngày duy nhất.
    Vi du: ['2021-06-01', '2020-07-01', '2020-06-01']
    """
    unique_dates = set()

    for datetime_str in datetime_strings:
        dt_obj = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
        unique_dates.add(dt_obj.strftime('%Y-%m-%d'))

    unique_dates_list = list(unique_dates)
    return unique_dates_list

# %%




def merge_sorted_lists(lists):
    heap = []
    result = []

    # Add the first element from each list to the min-heap
    for i, lst in enumerate(lists):
        if lst:
            heapq.heappush(heap, (lst[0], i, 0))

    prev_val = None  # Track the previously added value

    while heap:
        val, list_index, element_index = heapq.heappop(heap)

        # Skip duplicate values
        if val == prev_val:
            continue

        result.append(val)
        prev_val = val

        # Move to the next element in the list
        element_index += 1
        if element_index < len(lists[list_index]):
            heapq.heappush(heap, (lists[list_index][element_index], list_index, element_index))

    return result
