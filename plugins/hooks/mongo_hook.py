from airflow.hooks.base_hook import BaseHook
from pymongo import MongoClient

class MongoDBHook(BaseHook):
    def __init__(self, conn_id='mongodb_default'):
        super().__init__()
        self.conn_id = conn_id
        self.client = None
    
    def __del__(self):
        if(self.client):
            self.client.close()
    
    def get_conn(self):
        if self.client is None:
            conn = self.get_connection(self.conn_id)
            self.client = MongoClient(host=conn.host,
                                      port=conn.port,
                                      username=conn.login,
                                      password=conn.password,
                                      authSource=conn.schema)
        return self.client

    def find(self, database, collection, query={}, projection=None):
        """
        Mô tả chức năng của hàm:
        Tìm kiếm nhiều document trong 1 collection bằng query object

        Input:
        - database (str): Tên database.
        - collection (str): Tên collection trong database.
        - query (dict): Dictionary chứa các cột trong bảng để tìm.
        Ví dụ: {"field1": value, "field2": value}

        Output:
        - data (list): Danh sách các đối tượng được biểu diễn dưới dạng từ điển. 
        Ví dụ: 
            [
                {
                    "_id": 662f81f04f6f9a3f46432c16,
                    "field1": value,
                    "field2": value,
                    ...
                }, 
            ...
            ]
        """
        client = self.get_conn()
        db = client[database]
        col = db[collection]

        return col.find(query,projection)

    def find_one(self, database, collection, query={}, projection=None):
        """
        Mô tả chức năng của hàm:
        Tìm kiếm 1 document trong 1 collection bằng query object

        Input:
        - database (str): Tên database.
        - collection (str): Tên collection trong database.
        - query (dict): Dictionary chứa các cột trong bảng để tìm.
        Ví dụ: {"field1": value, "field2": value}

        Output:
        - data (dict | None): document trả về. 
        Ví dụ: 
            {
                "_id": 662f81f04f6f9a3f46432c16,
                "field1": value,
                "field2": value,
                ...
            }
        """
        client = self.get_conn()
        db = client[database]
        col = db[collection]
        print(query, projection)
        return col.find_one(query,projection)


    def insert_one(self, database, collection, document):
        """
        Mô tả chức năng của hàm:
        Thêm 1 document trong 1 collection

        Input:
        - database (str): Tên database.
        - collection (str): Tên collection trong database.
        - document (InsertManyResult): Đối tượng của pymonogo
        Ví dụ: {"field1": value, "field2": value}

        Output:
        - data (InsertOneResult): document vừa được tạo. 
        Ví dụ: 
            {
                "_id": 662f81f04f6f9a3f46432c16,
                "field1": value,
                "field2": value,
                ...
            }
        """
        client = self.get_conn()
        db = client[database]
        col = db[collection]
        return col.insert_one(document)

    def insert_many(self, database, collection, documents):
        """
        Mô tả chức năng của hàm:
        Thêm nhiều document trong 1 collection

        Input:
        - database (str): Tên database.
        - collection (str): Tên collection trong database.
        - document (list(InsertManyResult)): Mảng các object InsertManyResult.
        Ví dụ: 
            [
                {
                    "_id": 662f81f04f6f9a3f46432c16,
                    "field1": value, 
                    "field2": value,
                    ...
                },
                ...
            ]

        Output:
        - data (dict): document vừa được tạo. 
        Ví dụ: 
            {
                "_id": 662f81f04f6f9a3f46432c16,
                "field1": value,
                "field2": value,
                ...
            }
        """
        client = self.get_conn()
        db = client[database]
        col = db[collection]
        return col.insert_many(documents)
    def delete_many(self, database, collection, filter):
        """
        Xoá nhiều documents trong collection dựa trên bộ lọc cho trước.

        Parameters:
        - database (str): Tên của database.
        - collection (str): Tên của collection.
        - filter (dict): Bộ lọc để xác định documents cần xoá. Mặc định là {} để xoá tất cả documents.

        Returns:
        - result (pymongo.results.DeleteResult): Kết quả của hoạt động xoá, bao gồm số documents bị xoá và các thông tin khác.
        """
        client = self.get_conn()
        db = client[database]
        col = db[collection]
        return col.delete_many(filter)

    def aggregate(self, database, collection, pipeline):
        client = self.get_conn()
        db = client[database]
        col = db[collection]
        return col.aggregate(pipeline)

# Implement other CRUD operations as needed

#%%
from pymongo import MongoClient

connect = MongoClient('mongodb://localhost:27017')
db = connect['core']
fact = db['fact_era5']

match_time_ids_pipeline = [
    {
        "$match": {
            "time_id": {"$in": ['t_20240517000000']}
        }
    }
]

fact_data = fact.aggregate(match_time_ids_pipeline)
print(list(fact_data))
connect.close()





# %%
