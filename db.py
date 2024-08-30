from logger_tracer import *
import oracledb
import configparser
import os
from datetime import datetime
from pathlib import Path
from tabulate import tabulate

def read_config(config_filename):
    config = configparser.ConfigParser()
    config.optionxform = str  # i used this because "config.optionxform = str" parse strings in keys in the format as they are, otherwise confogparser convert all capital letter into small letters
    # it will be mandatory if we are using capital letters in naming functions in main2.py
    if not os.path.exists(config_filename):
        raise FileExistsError(f"{config_filename} could not be found")
    config.read(config_filename)
    return config

config = read_config(r"C:\Users\Sajal Jain\Desktop\VS_CODE\BT_CustomerDatabaseManagement\db.config")

for key,value in config["driver_type"].items():
    if key=="thick":
        if value=="True":
            thick = True
        else:
            thick = False
    if key=="instant_client_libdir":
        instant_client_libdir = value

for key,value in config["pool"].items():
    if key=="Ispool":
        if value=="True":
            Ispool = True
        else:
            Ispool = False
    if key=="user":
        user = value
    if key=="password":
        password = value
    if key=="dsn":
        dsn = value

for key,value in config["transactions"].items():
    if key=="duration_mins":
        duration_mins = int(value)
    if key=="iterations":
        iterations = int(value)
    if key =="num_threads":
        num_threads = int(value)
    if key=="doThreading":
        if value=="True":
            doThreading=True
        else:
            doThreading=False

if duration_mins==0 and iterations==0:
    raise KeyError("Configuration is for neither duration nor iteration. Set one")
if duration_mins!=0 and iterations!=0:
    raise KeyError("Configuration is set for both duration and iteration. Unset one")

funcnames_=[]
weights_=[]
for key,value in config["test_cases"].items():
    if int(value)>0:
        funcnames_.append("run_"+key)
        weights_.append(int(value))

if thick==True:
    oracledb.init_oracle_client(lib_dir=Path(instant_client_libdir))

if Ispool==True:
    pool = oracledb.create_pool(user=user,password=password,dsn=dsn,min = 50, max = 100, increment=1, timeout=15)

def create_connection():
    if Ispool==False:
        connection = oracledb.connect(user=user,password=password,dsn=dsn)
        # _connectionArgs = {"user":"string", "password":"string", "dsn":"string", "mode":"", "threaded":"boolean", \
        #         "twophase":"boolean", "events":"boolean", "cclass":"string", "purity":"", "newpassword":"string", \
        #         "module":"string", "action":"string", "clientinfo":"string", "edition":"string","wallet_location":"string",\
        #         "wallet_password":"string"}
        cursor = connection.cursor()
        if connection.is_healthy()==True:
            return connection,cursor
        else:
            traceInfo("connection could not be created")
    
    if Ispool==True:
        # _poolArgs = {"user":"string", "password":"string", "dsn":"string" , "getmode":"","min":"int", "max":"int", \
        #         "increment":"int",  "events":"boolean","wallet_location":"string","wallet_password":"string","enable_drcp":"boolean"}
        connection = pool.acquire()
        cursor = connection.cursor()
        return connection,cursor
        if connection.is_healthy()==True:
            return connection,cursor
        else:
            traceInfo("connection could not be created")
    
# connection = create_connection()

def close_conn(connection,cursor):
    cursor.close()
    connection.close()

def pool_monitor():
    summary = "opened connections: "+str(pool.opened)+"\n"
    summary += "busy connections: "+str(pool.busy)+"\n"
    summary += "idle connections: "+str(pool.opened-pool.busy)+"\n"
    return summary

def session_monitor():
    connection,cursor = create_connection()
    query = """
    SELECT sid, serial#, username, status, osuser, machine, program
    FROM v$session
    WHERE status = 'ACTIVE'
    """
    cursor.execute(query)
    sessions = cursor.fetchall()
    close_conn(connection,cursor)
    return tabulate(sessions, headers=["SID", "Serial#", "Username", "Status", "OS User", "Machine", "Program"], tablefmt="grid")

def monitor_lock():
    print(datetime.now())
    connection, cursor = create_connection()
    query = """
    SELECT
        l.session_id,
        s.username,
        s.osuser,
        o.object_name,
        o.object_type,
        l.locked_mode
    FROM
        v$locked_object l
        JOIN dba_objects o ON l.object_id = o.object_id
        JOIN v$session s ON l.session_id = s.sid
    """
    cursor.execute(query)
    locks = cursor.fetchall()
    close_conn(connection, cursor)
    print(datetime.now())
    return tabulate(locks, headers=["Session ID", "Username", "OS User", "Object Name", "Object Type", "Locked Mode"], tablefmt="grid")

def monitor_blocking_sessions():
    connection,cursor = create_connection()
    query = """
SELECT
    b.HOLDING_SESSION AS blocking_session,
    w.WAITING_SESSION AS waiting_session,
    o.object_name,
    o.object_type
FROM
    dba_blockers b
    JOIN dba_waiters w ON b.holding_session = w.waiting_session
    JOIN dba_objects o ON w.WAITING_CON_ID = o.object_id
"""
    cursor.execute(query)
    blockers = cursor.fetchall()
    close_conn(connection,cursor)
    return (tabulate(blockers, headers=["Blocking Session", "Waiting Session", "Object Name", "Object Type"], tablefmt="grid"))