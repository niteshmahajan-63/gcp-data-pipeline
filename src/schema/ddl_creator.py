from src.dbutils.connection import run_query
import re
from src.fileutils.browser import traverse, file_skip
# from src.schema.target_merge_queries import process_patients_new_to_target_merge_rule


all_queries = []

def create_ddl(client, schema_folder=None, delete=None, rule=None):
    file_paths = traverse(folder_name = 'ddl-queries', filter=schema_folder)
    for file_path in file_paths:
        print("running for {}".format(file_path))
        if file_skip(filename=file_path):
            continue
        with open(file_path) as f:
            file_data = f.read()
            query = file_data
            try:
                run_query(client, query)
                # print(query)
            except Exception as e:
                # print(e)
                all_queries.append(query)
                pass

            # return
    print( "  ;  ".join(all_queries))

