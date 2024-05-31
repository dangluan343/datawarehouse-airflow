from helpers.utils import * 
class RadarStagingLoader:
    def __init__(self, connect):
        self.connect = connect
    
    def load_radar_location(self, radar_info):
        connect = self.connect
        
        longitude = radar_info['longitude']
        latitude = radar_info['latitude']
        altitude = radar_info['altitude']

        location_surrogate_key = create_location_surrogate_key(longitude, latitude, altitude)

        radar_location_query = {
            'longitude': longitude,
            'latitude': latitude,
            'altitude': altitude,
        }

        # Check if radar location already exists
        radar_location_info = connect.find_one(
            database='staging_area', 
            collection='radar_location', 
            query={'_id': location_surrogate_key}
        )

        if not radar_location_info:
            radar_location_query['_id'] = location_surrogate_key
            # Create new radar location if not found
            new_radar_location = connect.insert_one(
                database='staging_area', 
                collection='radar_location', 
                document=radar_location_query
            )
        return location_surrogate_key

    def load_radar_sweep(self, radar_sweep_info):
        connect = self.connect
        inserted_radar_sweep = connect.insert_one(
            database='staging_area', 
            collection='radar_sweep', 
            document=radar_sweep_info
        )
        return inserted_radar_sweep.inserted_id
        
    def load_radar_data(self, radar_data):
        connect = self.connect
        connect.insert_many(
            database='staging_area', 
            collection='radar_data', 
            documents=radar_data
        )
        