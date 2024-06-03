def generate_dimension_key(dimension, type):
    key = ''
    if type == 'element':
        name = dimension['name'].lower().replace(" ", "")
        key = 'e_' + name

    elif type == 'time':
        year = f'0{dimension["year"]}' if dimension["year"] < 10 else str(dimension["year"])
        month = f'0{dimension["month"]}' if dimension["month"] < 10 else str(dimension["month"])
        day = f'0{dimension["day"]}' if dimension["day"] < 10 else str(dimension["day"])
        hour = f'0{dimension["hour"]}' if dimension["hour"] < 10 else str(dimension["hour"])
        minute = f'0{dimension["minute"]}' if dimension["minute"] < 10 else str(dimension["minute"])
        second = f'0{dimension["second"]}' if dimension["second"] < 10 else str(dimension["second"])
        key = 't_' + year + month + day + hour + minute + second
        
    elif type == 'location':
        key = f'l_{dimension["latitude"]}{dimension["longitude"]}{dimension["altitude"]}'
    return key

def assign_dimension_key(dirty_to_clean, type):
    # Used for fact link to dimension
    dirty_to_key = dirty_to_clean

    # Used for loading dimensions
    key_to_clean = {}

    # Used for merging dimension from all sources
    keys = []

    for key, clean_dimension_value in dirty_to_key.items():
        surrogate_key = generate_dimension_key(clean_dimension_value, type)
        dirty_to_key[key] = surrogate_key
        key_to_clean[surrogate_key] = clean_dimension_value
        keys.append(surrogate_key)

    # temporary way to maintain the order of dimension
    keys.sort()
    return {
        'dirty_to_key': dirty_to_key, 
        'key_to_clean': key_to_clean, 
        'keys':keys
    }