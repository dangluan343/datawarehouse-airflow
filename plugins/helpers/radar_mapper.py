from itertools import chain


def fact_radar_mapping(radar_obj):
    radar_data_len = len(radar_obj['reflectivity'])
    result = []
    for i in range(radar_data_len):
        reflectivity=radar_obj['reflectivity'][i]
        velocity=radar_obj['velocity'][i]
        spectrum_width=radar_obj['spectrum_width'][i]

        radar_data = {
            'time':radar_obj['time'],
            'ray_index':radar_obj['ray_index'],
            'bin_index': i,
        }
        if(not reflectivity and not velocity and not spectrum_width):
            continue
        if reflectivity:
            radar_data['reflectivity'] = reflectivity
        if velocity:
            radar_data['velocity'] = velocity
        if spectrum_width:
            radar_data['spectrum_width'] = spectrum_width
        
        result.append(radar_data)
    return result



def facts_radar_mapping(radar_data):
    result = []
    for radar_obj in radar_data:
        fact = fact_radar_mapping(radar_obj)
        result.append(fact)
    
    return list(chain.from_iterable(result))
