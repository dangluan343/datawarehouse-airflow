import heapq
import json
def merge_sorted_dimension_from_all(dimension_all_sources):
    heap = []
    unique_dimension_keys = []

    # Add the first element from each list to the min-heap
    for index, dimension_one_source in enumerate(dimension_all_sources):
        if dimension_one_source:
            heapq.heappush(heap, (dimension_one_source[0], index, 0))

    prev_dimension_value = None  # Track the previously added value

    while heap:
        dimension_value, list_index, element_index = heapq.heappop(heap)

        # Skip duplicate values
        if dimension_value != prev_dimension_value:
            unique_dimension_keys.append(dimension_value)
            
        prev_dimension_value = dimension_value

        # Move to the next element in the list
        element_index += 1
        if element_index < len(dimension_all_sources[list_index]):
            heapq.heappush(heap, (dimension_all_sources[list_index][element_index], list_index, element_index))

    return unique_dimension_keys


def lookup_dimension_keys(facts, dirty_to_key_element, dirty_to_key_time, dirty_to_key_location):
    for fact in facts:
        dirty_element = json.dumps(fact['element_id'], sort_keys=False)
        fact['element_id'] = dirty_to_key_element[dirty_element]

        dirty_time = json.dumps(fact['time_id'], sort_keys=False)
        fact['time_id'] = dirty_to_key_time[dirty_time]

        dirty_location = json.dumps(fact['location_id'], sort_keys=False)
        fact['location_id'] = dirty_to_key_location[dirty_location]
    return facts

def lookup_dimension_keys_radar_sweep(facts, dirty_to_key_time, dirty_to_key_location):
    for fact in facts:
        

        dirty_time = json.dumps(fact['time_id'], sort_keys=False)
        fact['time_id'] = dirty_to_key_time[dirty_time]

        dirty_location = json.dumps(fact['location_id'], sort_keys=False)
        fact['location_id'] = dirty_to_key_location[dirty_location]
    return facts


