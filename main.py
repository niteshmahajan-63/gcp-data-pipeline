import os
from src.schema.ddl_creator import create_ddl

cwd = os.getcwd()
dir_path = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir_path)

print('path: ', dir_path)

def run(client):
    folders = "pre_merge,patients_merge"
    folder_list = folders.split(',')
    
    for folder in folder_list:
        create_ddl(client, schema_folder=folder, delete=True)
