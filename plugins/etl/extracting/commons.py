import os
import glob

#%%
SOURCES_PATH = '/Users/luanluan/Documents/Data/dw_airflow_2/data/'

def get_files_in_directory(directory):
    file_pattern = os.path.join(directory, '*.grib')
    file_list = glob.glob(file_pattern)
    return file_list

# print(get_files_in_directory(SOURCES_PATH + 'era5-single'))

def get_radar_data(radar_files): pass
# %%
