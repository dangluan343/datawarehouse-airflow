class RadarStagingLoader:
    def __init__(self, connect):
        self.connect = connect #MongoHook obj

    def load_dim_time(self, time):
        connect = self.connect()
        time_query = {
            '_id': time['_id']
        }
     # Check if radar location already exists
        find_time = connect.find_one(
            database='core', collection='dimension_time', query=time_query
        )

        if not find_time:
            # Create new time if not found
            connect.insert_one(
                database='core', collection='dimension_time', document=time
            )

    
    # def load_radar_location(self, radar_info):
    #     connect = self.connect
    #     radar_location_query = {
    #         'longitude': radar_info['longitude'],
    #         'latitude': radar_info['latitude'],
    #         'altitude': radar_info['altitude'],
    #     }

    #     # Check if radar location already exists
    #     radar_location_info = connect.find_one(
    #         database='staging_area', collection='radar_location', query=radar_location_query
    #     )

    #     if not radar_location_info:
    #         # Create new radar location if not found
    #         new_radar_location = connect.insert_one(
    #             database='staging_area', collection='radar_location', document=radar_location_query
    #         )

    # def load_radar_sweep(self, radar_sweep_info):
    #     connect = self.connect
    #     connect.insert_one(
    #         database='staging_area', collection='radar_sweep', document=radar_sweep_info
    #     )
        
    # def load_radar_data(self, radar_data):
    #     connect = self.connect
    #     connect.insert_many(
    #     database='staging_area', collection='radar_data', documents=radar_data
    # )
        