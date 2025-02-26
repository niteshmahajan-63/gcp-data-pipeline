import os
import sys
from datetime import datetime
from src.sync_ingest.sync_sourcedata import syncDumpData
from src.sync_ingest.sync_dumptorawdata import syncRawData
from src.dbutils.connection import *
# from src.sync_ingest.logging_module import getLogger
filename =os.path.basename(__file__).split('.')[0]
# logger = getLogger(filename)
from google.cloud import storage    
from src.dbutils.connection import read_config
config_file = "config.ini"
config = read_config(config_file)

class ingestworker():
    def startingestworker(self, client, queue_obj=None):
        print("running dump & raw load process===============>")
        now = datetime.now()
        try:
            syncDumpData().start(now, client)
            syncRawData().start(now, client)       

        except Exception as error :
            print('>>>>> {} :: got exception  '.format(filename))
            if queue_obj:
                queue_obj.put(sys.exc_info())

            raise error    

