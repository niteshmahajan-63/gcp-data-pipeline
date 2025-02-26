import logging
import os
import sys
from datetime import datetime, timedelta

now = datetime.utcnow().strftime("%y%m%d")

syncErrorLogFile = 'errorlogs_' + now
syncAllLogFile = 'alllogs_' + now
filename =os.path.basename(__file__).split('.')[0]
dir_path = os.path.dirname(os.path.realpath(__file__))

#set root logger to info level
logging.getLogger().setLevel(logging.DEBUG)

def getLogger(modulename = filename):
    
    # Create a custom logger
    logger = logging.getLogger(f'__{modulename}__')
    logger.handlers = []

    # Create handlers
    c_handler = logging.StreamHandler()
    fa_handler = logging.FileHandler(f'{dir_path}/logs/{syncAllLogFile}.log')
    fe_handler = logging.FileHandler(f'{dir_path}/logs/{syncErrorLogFile}.log')

    c_handler.setLevel(logging.DEBUG)
    fa_handler.setLevel(logging.DEBUG)
    fe_handler.setLevel(logging.WARNING)

    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    fa_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fe_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    c_handler.setFormatter(c_format)
    fa_handler.setFormatter(fa_format)
    fe_handler.setFormatter(fe_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(fa_handler)
    logger.addHandler(fe_handler)
    
    # logger.debug('debug msg will be logged here')
    # logger.info('info msg will be logged here')
    # logger.warning('warning msg will be logged here')
    # logger.error('error msg will be logged here', exc_info=True)
    # logger.exception('exception will be logged here')
    # logger.critical('critical msg will be logged here')
    
    return logger

