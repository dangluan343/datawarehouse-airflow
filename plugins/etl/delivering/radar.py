class RadarStagingLoader:
    def __init__(self, connect):
        self.connect = connect
    

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
        