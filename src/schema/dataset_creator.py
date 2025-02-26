from google.cloud import bigquery
import os

from src.dbutils.connection import run_query

def creator(client, project_id):
    base_directory = r'C:\nitesh\gcp-data-pipeline\ddl-queries'
    region = 'asia-south1'
    datasetcreate = False
    failedqueries=[]

    for root, dirs, files in os.walk(base_directory):
        if datasetcreate == True:
            for directory in dirs:
                dataset_id = directory
                dataset_ref = client.dataset(dataset_id, project=project_id)

                try:
                    if not client.get_dataset(dataset_ref):
                        # Create the dataset
                        dataset = bigquery.Dataset(dataset_ref)
                        client.create_dataset(dataset)
                        print(f"Dataset {dataset_id} created in project {project_id} in the {region} location.")
                    else:
                        print(f"Dataset {dataset_id} already exists in project {project_id} in the {region} location.")
                except Exception as e:
                        dataset = bigquery.Dataset(dataset_ref)
                        client.create_dataset(dataset)
                        print(f"Dataset {dataset_id} created in project {project_id} in the {region} location.")
            
       
        for file_name in files:
            file_path = os.path.join(root, file_name)
            
            # Check if the file is a text file (you can adjust this condition as needed)
            if file_name.endswith('.sql'):
                with open(file_path, 'r') as file:
                    file_content = file.read()
                    # Now 'file_content' contains the content of the file as a string
                    # print(file_content)
                    query = file_content
                    try:
                        run_query(client, query)
                        print(f'table created sucessfully:{file_name}')
                        # print(query)
                    except Exception as e:
                        print(e)
                        failedqueries.append(query)
                        pass

                    # return
            print( "  ;  ".join(failedqueries))
