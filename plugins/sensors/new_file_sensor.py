from airflow.sensors.base import BaseSensorOperator
import os

class TrackNewFileSensor(BaseSensorOperator):
    def __init__(self, directory='/Users/luanluan/Documents/Data/dw_airflow_2/data/radar', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.directory = directory
        self.already_existing_files = set()

    def poke(self, context):
        current_files = set()
        for root, _, files in os.walk(self.directory):
            for file in files:
                if(file == '.DS_Store'):
                    continue
                current_files.add(os.path.join(root, file))
        
        new_files = current_files - self.already_existing_files
        
        if new_files:
            # Cập nhật danh sách các tệp đã biết
            self.already_existing_files = current_files
            # Lưu đường dẫn của tệp mới vào XCom
            context['ti'].xcom_push(key='new_radar_file_paths', value=list(new_files))
            return True
        return False




# #%%
# class TrackNewFileSensor():
#     def __init__(self, directory='/Users/luanluan/Documents/Data/dw_airflow_2/data/radar'):
#         self.directory = directory
#         self.already_existing_files = set()

#     def poke(self):
#         current_files = set()
#         for root, _, files in os.walk(self.directory):
#             for file in files:
#                 print('file', file)
#                 if(file == '.DS_Store'):
#                     continue
#                 current_files.add(os.path.join(root, file))
#         new_files = current_files - self.already_existing_files
#         print(new_files)

        
        
# a = TrackNewFileSensor()
# a.poke()


# %%
