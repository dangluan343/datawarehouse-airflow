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


def extract_staging_era5(documents):

    dimension_element = []
    dimension_time = []
    dimension_location = []
    fact_era5 = []

    for document in documents:
        
        for element_data in document['elements']:

            for i in range(len(element_data['values'])):
                element = {'name': element_data['name'], 'short_name': element_data['short_name'], 'unit': element_data['units']}
                time = {'date': element_data['date'], 'hour': element_data['time']}
                location = {'latitude': document['coordinates'][i][0], 'longitude': document['coordinates'][i][1], 'altitude': element_data['pressure_level']}

                dimension_element.append(element)
                dimension_time.append(time)
                dimension_location.append(location)

                value = [element_data['values'][i]]
                fact_to_dimensions = {'value': value, 'element_id': element, 'time_id': time, 'location_id': location}
                fact_era5.append(fact_to_dimensions)

    return  {
                'fact_era5': fact_era5, 
                'dimension_element': dimension_element, 
                'dimension_time': dimension_time,
                'dimension_location': dimension_location
            }



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