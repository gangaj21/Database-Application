import socket
import os
import logging
import numpy as np
import datetime
from logging.handlers import RotatingFileHandler
import pandas as pd
import configparser

def read_config(config_file_name):
    config = configparser.ConfigParser(interpolation=None)
    if not os.path.exists(config_file_name):
        raise FileNotFoundError(f"The file {config_file_name} does not exist.")
    config.read(config_file_name)
    return config

config = read_config(r"C:\Users\Sajal Jain\Desktop\VS_CODE\BT_CustomerDatabaseManagement\db.config")

for key,value in config['address'].items():
    if key=='log_file_direc':
        log_file_direc = value
    if key=='trace_file_direc':
        trace_file_direc = value

for key,value in config['log_file_handle'].items():
    if key=='log_level':
        log_level = int(value)
    if key=='log_format':
        log_format = value
    if key=='log_datefmt':
        log_datefmt = value
    if key=='log_filemode':
        log_filemode = value

for key,value in config['trace_file_handle'].items():
    if key=='trace_level':
        trace_level = int(value)
    if key=='trace_format':
        trace_format = value
    if key=='trace_datefmt':
        trace_datefmt = value
    if key=='trace_filemode':
        trace_filemode = value

index = 0
dict_of_Trans={}
for key,value in config['test_cases'].items():
    if int(value)>0:
        index+=1
        dict_of_Trans[key] = index
if len(dict_of_Trans)==0:
    raise KeyError("No test_case has been selected. Select atleast one in db.config file")

log_file_name = socket.gethostname()+'_'+'monitor'+'.log'
if not os.path.exists(log_file_direc):
    os.makedirs(log_file_direc)
log_file_path = os.path.join(log_file_direc, log_file_name)

trace_file_name = socket.gethostname()+'_'+'trace'+'.trc'
if not os.path.exists(trace_file_direc):
    os.makedirs(trace_file_direc)
trace_file_path = os.path.join(trace_file_direc,trace_file_name)

with open(log_file_path, 'w'):
    pass
with open(trace_file_path,'w'):
    pass

#CRITICAL=50
#ERROR=60
#WARNING=30
#INFO=20
#DEBUG=10
#NOTEST=0

logger = logging.getLogger("logger")
if log_level==10:
    logger.setLevel(logging.DEBUG)
if log_level==20:
    logger.setLevel(logging.INFO)
if log_level==30:
    logger.setLevel(logging.WARNING)
if log_level==50:
    logger.setLevel(logging.CRITICAL)
if log_level==60:
    logger.setLevel(logging.ERROR)

tracer = logging.getLogger("tracer")
if trace_level==10:
    tracer.setLevel(logging.DEBUG)
if trace_level==20:
    tracer.setLevel(logging.INFO)
if trace_level==30:
    tracer.setLevel(logging.WARNING)
if trace_level==50:
    tracer.setLevel(logging.CRITICAL)
if trace_level==60:
    tracer.setLevel(logging.ERROR)

log_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=1024*1024,
            mode=log_filemode
    )

log_formatter = logging.Formatter(log_format,log_datefmt)
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

trace_handler = RotatingFileHandler(
            trace_file_path,
            maxBytes=1024*1024,
            mode = trace_filemode
)
# print(type(trace_format))
# print(type(trace_datefmt))
trace_formatter = logging.Formatter(trace_format,trace_datefmt)
trace_handler.setFormatter(trace_formatter)
tracer.addHandler(trace_handler)

rows = index+2
logTable = np.arange(rows*4).reshape(rows,4).astype("object")
logTable[0][0] = "Transactions"
logTable[0][1] = "Total"
logTable[0][2] = "Pass"
logTable[0][3] = "Fail"

list_of_Trans = [key for key in dict_of_Trans.keys()]
for i in range(1,rows-1):
    logTable[i][0] = list_of_Trans[i-1]
logTable[rows-1][0] = "Total"

for i in range(1,rows):
    for j in range(1,4):
        logTable[i][j]=0

def logWriter():
    with open(log_file_path,'a') as log_file:
        current_time = datetime.datetime.now().strftime(log_datefmt)
        log_file.write("\n########################################################################################\n")
        log_file.write("Time of Logging: "+current_time+"\n")
        dframe = pd.DataFrame(logTable)
        log_file.write(dframe.to_string(index = False, header=False))
        log_file.write("\n------------------------------------------------------------------------------------------\n")
        log_file.close()

def logTransaction(type, status):
    logTable[dict_of_Trans[type]][status+2] += 1
    logTable[dict_of_Trans[type]][1] += 1
    logTable[rows-1][1] +=1
    logTable[rows-1][status+2] += 1

def logDataFrame(dframe,query):
    with open(log_file_path,'a') as log_file:
        current_time = datetime.datetime.now().strftime(log_datefmt)
        log_file.write("Time of Logging: "+current_time+"\n")
        log_file.write("Query requested: "+query)
        log_file.write(dframe.to_string(index = False, header=False))
        log_file.write("----------------------------------------------------------------------------------------")
        log_file.write("----------------------------------------------------------------------------------------\n")
        log_file.close()
     
def logInfo(str):
    logger.info(str)

def logDebug(str):
    logger.debug(str)
    
def logError(str):
    logger.error(str)

def logWarning(str):
     logger.warning(str)

def traceError(str):
    tracer.error(str)

def traceDegug(str):
    tracer.debug(str)

def traceInfo(str):
    tracer.info(str)

def traceWarning(str):
    tracer.warning(str)