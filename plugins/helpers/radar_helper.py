import os
import pyart
import numpy as np
from helpers.radar_extractor import * 

def transform(value): 
    if(value == '--'):
        return None
    else: 
        return value


def read_data_from_file3(file_path): #file_path: absolute file path 
    """
    Mô tả chức năng của hàm:
    Đọc dữ liệu từ đường dẫn tuyệt đối của file và trả về danh sách các đối tượng được biểu diễn dưới dạng từ điển.

    Input:
    - file_path (str): Đường dẫn tuyệt đối đến tệp cần đọc dữ liệu.

    Output:
    - data (list): Danh sách các đối tượng được biểu diễn dưới dạng từ điển. Mỗi đối tượng đại diện cho một bản ghi hoặc một dòng trong tệp.
      Ví dụ:
      [
          {
            "gate_longitude": "", 
            "gate_latitude":  "", 
            "gate_altitude":  "", 
            "reflectivity_data":  "", 
            "total_power_data":  "", 
            "velocity_data":  "",
            "spectrum_width_data":  "", 
            "time":  ""
          },
          ...
      ]
    """
    radar = pyart.io.read_sigmet(file_path)
    try:
        reader = RadarReader(radar)
        radar_data = reader.get_radar_data()
        radar_sweep_info = reader.get_radar_sweep()
        radar_info = reader.get_radar_info()
        return radar_data, radar_sweep_info, radar_info    
    except ZeroDivisionError:
        # Code block to handle ZeroDivisionError
        print("read_data_from_file3 wrong")
    except Exception as e:
        # Code block to handle any other exceptions
        print("An error occurred:", e)

    


def read_radar_file_path():
    """
    Mô tả chức năng của hàm:
    Đọc toàn bộ đường dẫn của các tệp dữ liệu radar và trả về một mảng chứa các đường dẫn tuyệt đối cho từng tệp.

    Output:
    - file_paths (list): Mảng chứa các đường dẫn tuyệt đối đến các tệp dữ liệu radar.
      Ví dụ:
      [
        "/Users/luanluan/Documents/Data/dw_airflow/data/radar/2020/Pro-Raw(1-8)T7-2020/01/NHB200701000010.RAWXPS3",
        "/Users/luanluan/Documents/Data/dw_airflow/data/radar/2020/Pro-Raw(1-8)T7-2020/01/NHB200701000307.RAWXPS6",
        ...
      ]
    """
    file_path_result = []
    # Đường dẫn tuyệt đối dến thư mục dữ liệu radar
    radar_path = '/Users/luanluan/Documents/Data/dw_airflow_2/data/radar'
    
    #lấy toàn bộ file name trong thư mục hiện tại 
    years = os.listdir(radar_path)
    years.sort()
    
    for year in years:
        if(year == '.DS_Store'):
            continue
        # print('year: ', year)

        month_path = radar_path + '/' + year
        months = os.listdir(month_path)
        months.sort()

        for month in months: 
            if(month == '.DS_Store'):
                continue
            # print('month: ', month)

            day_path = radar_path + '/' + year + '/' + month
            days = os.listdir(day_path)
            days.sort()

            for day in days: 
                if(day == '.DS_Store'):
                    continue
                # print('day: ', day)

                file_day_path = radar_path + '/' + year + '/' + month + '/' + day
                files = os.listdir(file_day_path)
                files.sort()
                for file in files:
                    if(file.endswith('.csv') or file == '.DS_Store'):
                        continue
                    file_path = radar_path + '/' + year + '/' + month + '/' + day + '/'+ file
                    file_path_result.append(file_path)

    return file_path_result

#%%
def get_absolute_file_path(data_dir='/Users/luanluan/Documents/Data/dw_airflow_2/data/radar'):
    current_files = set()
    for root, _, files in os.walk(data_dir):
        for file in files:
            if(file == '.DS_Store'):
                continue
            current_files.add(os.path.join(root, file))
    
    return list(current_files)
# print(get_absolute_file_path())
# %%

#'/Users/luanluan/Documents/Data/dw_airflow_2/data/radar/2020/Pro-Raw(1-8)T7-2020/01/NHB200701000307.RAWXPS6'
#input: **/**/file_name
#output: file_name
def get_file_name_from_absolute_path(path):
    return path.split('/')[-1]
# %%
