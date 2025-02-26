from main import run as runDDL
from src.pipeline.main import start as startPipeline
from src.dbutils.connection import create_client
from src.schema.dataset_creator import creator


def start():
    try:
        client,project_id, sync_ingest_only = create_client()
        # creator(client,project_id)
        # print(clientp)
        # runDDL(client)
        startPipeline(client,sync_ingest_only)
        # print(client)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    
start()