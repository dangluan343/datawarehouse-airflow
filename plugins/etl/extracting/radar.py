
from datetime import datetime

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


def extract_staging_radar_info(fact_radar_sweep_documents, fact_radar_documents, ):

    dimension_element = []
    dimension_time = []
    dimension_location = []
    fact_radar = []
    fact_radar_sweep = []

    for radar_sweep in fact_radar_sweep_documents:
        print('hahaha', radar_sweep.keys(), type(radar_sweep))
        
        time = time_stamp_to_object(radar_sweep['time_volume_start'])

        element = element_obj_to_dim_element(radar_sweep['element_metadata'])

        location = radar_sweep['location']

        fact_radar_sweep.append({
            'time_id': time, 
            'location_id': location, 
            'range': radar_sweep['range'], 
            'scan_type': radar_sweep['scan_type'], 
            'sweep_number': radar_sweep['sweep_number'], 
            'sweep_mode': radar_sweep['sweep_mode'], 
            'fixed_angle': radar_sweep['fixed_angle'], 
            'sweep_start_ray_index': radar_sweep['sweep_start_ray_index'], 
            'sweep_end_ray_index': radar_sweep['sweep_end_ray_index'], 
            'azimuth': radar_sweep['azimuth'], 
            'elevation': radar_sweep['elevation'], 
        })

        dimension_time.append(time)
        dimension_location.append(location)
        dimension_element = dimension_element + element

    element_lookup = {dim['short_name'].lower(): dim for dim in dimension_element}
    
    fixed_fields = {'ray_index', 'time', 'time_volume_start', 'location'}

    fact_radar = []

    for radar_data in fact_radar_documents:
        time = time_stamp_to_object(radar_data['time_volume_start'])
        location = radar_data['location']

        for key, value in radar_data.items():
            if key not in fixed_fields:
                element = element_lookup.get(key.lower())
                
                for index, fact in enumerate(value):
                    if(fact is not None):
                        fact_radar.append({
                            'time_id': time, 
                            'location_id': location, 
                            'element_id': element,
                            'value': fact,
                            'second_duration': radar_data['time'], 
                            'ray_index': radar_data['ray_index'], 
                            'bin_index': index,
                        })
    
    return  {
                'fact_radar': fact_radar, 
                'fact_radar_sweep': fact_radar_sweep,
                'dimension_element': dimension_element, 
                'dimension_time': dimension_time,
                'dimension_location': dimension_location
            }

