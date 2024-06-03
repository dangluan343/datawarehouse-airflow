import numpy as np
import json

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


def deduplicate(values):
    """Use set to remove duplicate dictionaries
    list of 1 type of dimension(time, location) -> deduplicated and sorted list of that dimension in all documents
    """
    unique_values = set()
    for value in values:
        # dict is mutable -> convert to tuple (immutable) so as to be hashed
        unique_values.add(tuple(value.items()))

    # revert tuple (unique_values) -> dictionary
    unique_values = [dict(value) for value in unique_values]

    # link dirty dimension/fact value to the clean one

    dirty_to_clean = {}
    for value in unique_values:
        dirty_to_clean[json.dumps(value, sort_keys=False)] = value

    return dirty_to_clean


def clean_dimension_time_era5(dimension_times):
    for index, value in enumerate(dimension_times):
        date = value['date']
        dimension_times[index]['year'] = date // 10000
        dimension_times[index]['month'] = (date // 100) % 100
        dimension_times[index]['day'] = date % 100

        del dimension_times[index]['date']

        hour = value['hour']
        if hour != 0:
            dimension_times[index]['hour'] = hour // 100
        dimension_times[index]['minute'] = 0
        dimension_times[index]['second'] = 0

    return dimension_times



def clean_dimension_location_era5(dimension_locations):
    for index, value in enumerate(dimension_locations):
        hPa = value['altitude']
        dimension_locations[index]['altitude'] = convert_hpa_level_to_m(hPa)

    return dimension_locations


def convert_hpa_level_to_m(hPa):
    if hPa == 0:
        return 0
    
    feet = ((1 - abs(hPa / 1013.25) ** 0.190284) * 145366.45)
    meter = feet * 0.3048
    return meter

# %%
