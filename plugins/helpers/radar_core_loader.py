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

    