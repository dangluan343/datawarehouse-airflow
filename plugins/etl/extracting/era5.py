import eccodes

def get_era5_data(era5_file):
    file = open(era5_file, 'rb')

    # Extract coordinate values
    coordinates = []

    grib_id = eccodes.codes_any_new_from_file(file)
    geo_id = eccodes.codes_grib_iterator_new(grib_id, False)
    geo_value = eccodes.codes_grib_iterator_next(geo_id)

    while (geo_value):
        latitude = geo_value[0]
        longitude = geo_value[1]
        coordinates.append((latitude, longitude))
        geo_value = eccodes.codes_grib_iterator_next(geo_id)

    eccodes.codes_grib_iterator_delete(geo_id)

    # Extract element's data
    elements = []

    grib_keys = ['level', 'dataDate', 'dataTime', 'name', 'shortName', 'units']
    dict_keys = ['pressure_level', 'date', 'time', 'name', 'short_name', 'units']

    document_count = 0
    while 1:
        
        if grib_id is None:
            break
        document_count += 1

        element = {}
        
        for dict_key, grib_key in zip(dict_keys, grib_keys):
            element[dict_key] = eccodes.codes_get(grib_id, grib_key)
        
        element['values'] = eccodes.codes_get_values(grib_id).tolist()
        elements.append(element)

        eccodes.codes_release(grib_id)
        grib_id = eccodes.codes_grib_new_from_file(file)

    print(document_count)
    return coordinates, elements



def split_era5(coordinates, elements, document_size):
    documents = []

    sub_elements = [elements[i : i + document_size] for i in range(0, len(elements), document_size)]

    for sub_element in sub_elements:
        document =  {
                        'coordinates': coordinates,
                        'elements': sub_element
                    }
        documents.append(document)

    return documents