 


def get_file_name_from_absolute_path(path):
    return path.split('/')[-1]
# %%


def write_audit_log_file(func):
    def wrapper(*args, **kwargs):

        connect = kwargs.get('connect')
        source_info = kwargs.get('source_info')
        
        paths = func(*args, **kwargs)
        print(f"Task audit_log_file executed with paths: {paths}")

        audit_log_file_documents = []
        for absolute_path in paths:
            file_name = get_file_name_from_absolute_path(absolute_path)
            audit_log_file_document = {
                '_id': file_name,
                'file_name': file_name,
                'absolute_path': absolute_path,
            }

            #nốt 2 dict
            audit_log_file_document.update(source_info)
            audit_log_file_documents.append(audit_log_file_document)

        connect.insert_many(
            database='audit_log', 
            collection='audit_log_file', 
            documents=audit_log_file_documents
        )

        return paths
    return wrapper
