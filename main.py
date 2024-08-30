from db import *
from action import *
from logger_tracer import *
from datetime import datetime
import time
import threading
import random
import queue
import functools

global carry_on
carry_on = True

queue_of_errors = queue.Queue()
global count
count = 0
lock = threading.Lock()
def Simu_logwrite():
    error_count = 0
    global count
    global carry_on
    while carry_on==True:
        logWriter()
        with lock:
            error_count = count
            count=0
        while error_count>0 and queue_of_errors.qsize()>0:
            ele = queue_of_errors.get()
            logError(ele)
            error_count-=1
        # logInfo("blocking sessions: \n" + monitor_blocking_sessions())
        print("log writer called")
        time.sleep(5)   #set timer in config

log_thread = threading.Thread(target=Simu_logwrite)
log_thread.start()

def randomly_shuffle(list):
    n = len(list)
    for _ in range(n):
        deter = random.randint(0,1)
        if deter==0:
            ind1 = random.randint(0,n-1)
            ind2 = random.randint(0,n-1)
            choose = random.randint(0,2)
            if choose==1:
                part = random.randint(0,n-1)
                ind1 = random.randint(0,part)
                ind2 = random.randint(part,n-1)
            temp = list[ind1]
            list[ind1] = list[ind2]
            list[ind2] = temp

start_time = -1
def chk_wl_running(): #chnage name to is_workload_running
    if duration_mins==0:
        return True
    diff = (datetime.now()-start_time).total_seconds()/60
    if diff<duration_mins:
        return True
    else:
        return False

def log_function_call(func):
    @functools.wraps(func)
    def wrapper():
        if chk_wl_running()==False:
            return func()
        thread_name = threading.current_thread().name
        traceInfo(f"{thread_name} called the {func.__name__}")
        return func()
    return wrapper

def fn_run_status(func):
    @functools.wraps(func)
    def wrapper():
        if chk_wl_running()==False:
            return
        global count
        status = func()
        if not status[0]:
            logTransaction(func.__name__[4:], 1)
            queue_of_errors.put(f"_{func.__name__}_" + status[-1])
            with lock:
                count += 1
        else:
            logTransaction(func.__name__[4:], 0)
    return wrapper

@log_function_call
@fn_run_status
def run_addnewcustomer():
    return addnewcustomer()

@log_function_call
@fn_run_status
def run_findcustomer():
    current_Custmax = get_largest_customer_id()
    cust_id = random.randint(1,current_Custmax)
    return findcustomer(cust_id)

@log_function_call
@fn_run_status
def run_place_order():
    cust_id = gen_random_customer_id()
    products_count = random.randint(1,6)
    products = gen_random_product_ids(products_count)
    return place_order(cust_id, products)

@log_function_call
@fn_run_status
def run_deleting_order():
    order_id = gen_random_order_id()
    return deleting_order(order_id)

@log_function_call
@fn_run_status
def run_deleting_cust():
    cust_id = gen_random_customer_id()
    return deleting_cust(cust_id)

@log_function_call
@fn_run_status
def run_update_item():
    order_id=gen_random_order_id()
    item_id = random.randint(1,6)
    products = gen_random_product_ids(1)
    new_prodid=products[0]
    qty=random.randint(1,4)
    return update_item(order_id,item_id,new_prodid,qty)

@log_function_call
@fn_run_status
def run_update_address():
    cust_id=gen_random_customer_id()
    new_add="xyz"
    return update_address(cust_id,new_add)

@log_function_call
@fn_run_status
def run_delete_item():
    order_id = gen_random_order_id() 
    item_id = random.randint(1,6)
    return delete_item(order_id,item_id)

# querying data
@log_function_call
@fn_run_status
def run_orders_ofcust():
    cust_id = gen_random_customer_id()
    return order_summary_ofCustomer(cust_id,default_startDateTime,default_endDateTime)

@log_function_call
@fn_run_status
def run_orders_ofcust_customdate():
    cust_id = gen_random_customer_id()
    # dates as string
    startDateTime = "2024-06-10 16:17:00"
    return order_summary_ofCustomer(cust_id,startDateTime,default_endDateTime)

@log_function_call
@fn_run_status
def run_summarise_orders_forcust():
    cust_id = gen_random_customer_id()
    return summarise_orders_forcust(cust_id,default_startDateTime,default_endDateTime)

@log_function_call
@fn_run_status
def run_summarise_orders_forcust_customdate():
    cust_id = gen_random_customer_id()
    startDateTime = "2024-06-10 16:17:00"
    return summarise_orders_forcust(cust_id,startDateTime,default_endDateTime)

def test_run(queue_of_tasks):
        if duration_mins==0:
            for func in queue_of_tasks:
                func()
        else:
            while chk_wl_running()==True:
                for func in queue_of_tasks:
                    func()

funcnames = [globals()[name] for name in funcnames_]
sum = 0
for weight in weights_:
    sum+=weight
if iterations!=0:
    weights = [int(weight*iterations/sum) for weight in weights_]

    # queue_of_tasks = queue.Queue()
    list_of_tasks = []
    for funcname,weight in zip(funcnames,weights):
        while weight>0:
            # queue_of_tasks.put(funcname)
            list_of_tasks.append(funcname)
            weight-=1

if duration_mins!=0:
    start_time=datetime.now()
    weights = [weight for weight in weights_]
    # queue_of_tasks = queue.Queue()
    list_of_tasks = []
    for funcname,weight in zip(funcnames,weights):
        while weight>0:
            # queue_of_tasks.put(funcname)
            list_of_tasks.append(funcname)
            weight-=1
    randomly_shuffle(list_of_tasks)
    randomly_shuffle(list_of_tasks)

workload_info = "Workload started here"
logInfo(workload_info)
if doThreading:
    threads=[]
    for i in range(num_threads):
        thread = threading.Thread(target=test_run, args=(list_of_tasks,))
        traceInfo(f"------------------Starting new thread:{thread.name}------------------")
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
else:
    test_run()

logInfo("bringing workload down in 10secs...")
time.sleep(5)
carry_on = False
time.sleep(5)
log_thread.join()
print("workload brought down here")
str = "workload is ended"
logInfo(str)
cnt = 0
for thread in threads:
    if thread.is_alive():
        cnt+=1
if log_thread.is_alive():
    cnt+=1
print("thread alive: ",cnt)