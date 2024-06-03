import numpy as np
from helpers.utils import *


def create_radar_data_frame(
    time, 
    time_volume_start, 
    reflectivity, 
    velocity, 
    spectrum_width, 
    ray_index,
    location
    ):
    data_frame = {
        'ray_index': ray_index,
        'time': str(time),
        'time_volume_start': time_volume_start,
        'reflectivity': reflectivity,
        'velocity': velocity,
        'spectrum_width': spectrum_width,
        'location': location
    }
    return data_frame


class RadarReader:
    def __init__(self, radar):
        self.radar = radar

    def get_by_fields(self, *args):
        result = {}
        for field in args:
            if hasattr(self.radar, field):  # Kiểm tra xem object có thuộc tính này hay không
                result[field] = getattr(self.radar, field)
        return result

    def get_data_by_fields(self, *args):
        result = {}
        for field in args:
            if hasattr(self.radar, field):  # Kiểm tra xem object có thuộc tính này hay không
                field_value = getattr(self.radar, field)
                if(isinstance(field_value,dict) and 'data' in field_value):
                    result[field] = field_value['data']
                else: 
                    result[field] = field_value

        return result
        

    def get_element_metadata(self,element_field_name):
        metadata = {
            'units': self.radar.fields[element_field_name]['units'],
            'name': str(self.radar.fields[element_field_name]['long_name']),
            'short_name': element_field_name,
            'description': str(self.radar.fields[element_field_name]['standard_name']).replace('_', ' '),
        }
        return metadata

    def get_radar_data(self):

        longitude = self.radar.longitude['data'][0]
        latitude = self.radar.latitude['data'][0]
        altitude = self.radar.altitude['data'][0]

        location = {
            'longitude':longitude,
            'latitude':latitude,
            'altitude':altitude
        }

        data_frames = []

        time_volume_start = self.radar.time['units'][14:]  # Extract time volume start
        times = self.radar.time['data']

        ray_len = len(times)
        for ray_index in range(ray_len):
            time = times[ray_index]

            reflectivity = self.radar.fields['reflectivity']['data'][ray_index].tolist()
            velocity = self.radar.fields['velocity']['data'][ray_index].tolist()
            spectrum_width = self.radar.fields['spectrum_width']['data'][ray_index].tolist()

            data_frame = create_radar_data_frame(
                time, 
                time_volume_start, 
                reflectivity, 
                velocity, 
                spectrum_width, 
                ray_index,
                location
            )

            data_frames.append(data_frame)

        return data_frames

    def get_radar_sweep(self):
        sweep_mode = [x.decode('utf-8') for x in self.radar.sweep_mode['data'].tolist()]
        longitude = self.radar.longitude['data'][0]
        latitude = self.radar.latitude['data'][0]
        altitude = self.radar.altitude['data'][0]
        radar_sweep = {
            'range': {
                'step': self.radar.range['meters_between_gates'].tolist()[0],
                'count': self.radar.ngates,
            },
            'scan_type': self.radar.scan_type,
            'element_metadata': {
                'reflectivity': self.get_element_metadata('reflectivity'),
                'velocity': self.get_element_metadata('velocity'),
                'spectrum_width': self.get_element_metadata('spectrum_width'),
            },
            # 'radar_location_id': create_location_surrogate_key(longitude,latitude,altitude),
            'location': {
                'longitude': longitude,
                'latitude': latitude,
                'altitude': altitude
            },
            'sweep_number': self.radar.sweep_number['data'],
            'sweep_mode': sweep_mode,
            'fixed_angle': self.radar.fixed_angle['data'],
            'sweep_start_ray_index': self.radar.sweep_start_ray_index['data'],
            'sweep_end_ray_index': self.radar.sweep_end_ray_index['data'],
            'azimuth': self.radar.azimuth['data'],
            'elevation': self.radar.elevation['data'],
            'time_volume_start': self.radar.time['units'][14:] #seconds since 2020-07-01T00:00:10Z => 2020-07-01T00:00:10Z
        }
        for key, value in radar_sweep.items():
            if isinstance(value, np.ndarray):
                radar_sweep[key] = value.tolist()
        return radar_sweep
    def get_radar_info(self):
        longitude = self.radar.longitude['data'][0]
        latitude = self.radar.latitude['data'][0]
        altitude = self.radar.altitude['data'][0]
        radar_info = {
            '_id': create_location_surrogate_key(longitude,latitude,altitude),
            'name': self.radar.metadata['instrument_name'],
            'longitude': longitude,
            'latitude': latitude,
            'altitude': altitude,

        }
        return radar_info



