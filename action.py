from db import *
import time
import oracledb
from logger_tracer import *
from datetime import datetime, timedelta
from tabulate import tabulate
import random

datetime_format = "%Y-%m-%d %H:%M:%S"
default_startDateTime = '1800-01-01 00:00:00'
default_endDateTime = time.strftime(datetime_format,time.localtime())

def clear_table(Table): # good to go
    connection,cursor = create_connection()
    truncate_sql = f"""
        TRUNCATE TABLE {Table}
    """
    cursor.execute(truncate_sql)
    #print(Table," is truncated")
    close_conn(connection,cursor)

def get_warehouse_id(product_id): # good to go
    connection,cursor = create_connection()
    try:
        cursor.execute("SELECT WHID FROM STOCK WHERE PRODID = :PRODUCT_ID",PRODUCT_ID = product_id)
        whid = cursor.fetchone()[0]
        close_conn(connection,cursor)
        return whid
    except oracledb.DatabaseError as e:
        error,  = e.args
        #print(f"warehouse could not process product request for {str(product_id)} due to {error.message}")
        traceError(f"warehouse could not process product request for {str(product_id)} due to {error.message}")
        close_conn(connection,cursor)
        return None
    except Exception as e:
        #print(f"warehouse could not process product request for {str(product_id)} due to {str(e)}")
        traceError(f"warehouse could not process product request for {str(product_id)} due to {str(e)}")
        close_conn(connection,cursor)
        return None
    
def get_price(product_id): # good to go
    connection,cursor = create_connection()
    try:
        cursor.execute("SELECT PRICE FROM PRODUCTS WHERE PRODID = :PRODUCT_ID",PRODUCT_ID = product_id)
        price = cursor.fetchone()[0]
    except oracledb.DatabaseError as e:
        error,  = e.args
        #print(f"could get price of product {str(product_id)} due to {error.message}")
        traceError(f"could get price of product {str(product_id)} due to {error.message}")
        close_conn(connection,cursor)
        return None
    except Exception as e:
        #print(f"could get price of product {str(product_id)} due to {str(e)}")
        traceError(f"could get price of product {str(product_id)} due to {str(e)}")
        close_conn(connection,cursor)
        return None
    close_conn(connection,cursor)
    return price

def gen_random_items(order_id,item_id,product_id,qty): # good to go
    data = []
    data.append(order_id)
    data.append(item_id)
    data.append(product_id) 
    whid = get_warehouse_id(product_id)
    if whid!=False:
        data.append(whid)
    else:
        return False
    order_date = datetime.now()
    two_day = timedelta(days=2)
    sell_by_date = order_date+two_day
    order_date_str = order_date.strftime(datetime_format)
    sell_by_date_str = sell_by_date.strftime(datetime_format)
    order_date = datetime.strptime(order_date_str, datetime_format)
    sell_by_date = datetime.strptime(sell_by_date_str,datetime_format)
    data.append(sell_by_date)
    price = get_price(product_id)
    if price!=False:
        data.append(price)
    else:
        return False
    data.append(1)
    data.append(qty)
    data.append('Description')
    data.append('Itemtext')
    return data

def gen_random_product_ids(cnt): # good to go
    connection,cursor = create_connection()
    
    random_ids = """
        SELECT PRODID FROM (
        SELECT PRODID FROM PRODUCTS ORDER BY DBMS_RANDOM.VALUE
        )
        WHERE ROWNUM <= :COUNT
    """
    cursor.execute(random_ids, [cnt])
    rows = cursor.fetchall()
    close_conn(connection,cursor)
    return [ids for ids, in rows]

def gen_random_order(order_id, cust_id): # good to go
    data = []
    data.append(order_id)
    data.append(cust_id)
    data.append(None)
    data.append('Orderinfox')
    order_date = datetime.now()
    one_day = timedelta(days=1)
    ship_date = order_date+one_day
    order_date_str = order_date.strftime(datetime_format)
    ship_date_str = ship_date.strftime(datetime_format)
    order_date = datetime.strptime(order_date_str, datetime_format)
    ship_date = datetime.strptime(ship_date_str,datetime_format)
    data.append(order_date)
    data.append(ship_date)
    data.append('Delname')
    data.append('Deladdress')
    data.append('Delcity')
    data.append('12 ')
    data.append('12345 ')
    return data

def gen_random_cust(id):  # good to go
    data = []
    data.append(id)
    data.append("random"+str(id))
    data.append("address"+str(id))
    data.append("city"+str(id))
    data.append('20')
    data.append('92250')
    data.append('456')
    data.append('3276527')
    data.append("type"+str(id))
    data.append("sale"+str(id))
    data.append("details1_"+str(id))
    data.append("details2_"+str(id))
    data.append(67000)
    return data

def gen_random_order_id():    # good to go
    connection,cursor = create_connection()
    
    # random_id = '''
    #     SELECT ORDID FROM(
    #     SELECT ORDID FROM ORDERS ORDER BY DBMS_RANDOM.VALUE
    #     )
    #     WHERE ROWNUM = 1
    # '''
    # cursor.execute(random_id)
    # row = cursor.fetchone()
    # close_conn(connection,cursor)
    # return row[0]
    try:
        cursor.execute("SELECT MAX(ORDID) FROM ORDERS")
        maxm,=cursor.fetchone()
        cursor.execute("SELECT MIN(ORDID) FROM ORDERS")
        minm,=cursor.fetchone()
        close_conn(connection,cursor)
        return random.randint(minm,maxm)
    except:
        close_conn(connection,cursor)
        return 1 

def gen_new_order_id(connection,cursor):   # good to go
    times = 10
    while times>0:
        try:
            cursor.execute("SELECT ORDERS_SEQ.NEXTVAL FROM DUAL")
            row = cursor.fetchone()
            return row[0]
        except oracledb.DatabaseError as e:
            times-=1
            error,= e.args
            msg = error.message
        except Exception as e:
            times-=1
            msg = str(e)
    return [False,f"could not gen order id due to {msg}"]

def gen_new_customer_id():    # good to go
    connection,cursor = create_connection()
    times = 10
    while times>0:
        try:
            cursor.execute("SELECT CUSTOMERS_SEQ.NEXTVAL FROM DUAL")
            row = cursor.fetchone()
            close_conn(connection,cursor)
            return row[0]
        except oracledb.DatabaseError as e:
            times-=1
            error,= e.args
            msg = error.message
        except Exception as e:
            times-=1
            msg = str(e)
    close_conn(connection,cursor)
    return [False,f"could not gen order id due to {msg}"]

def get_largest_customer_id():   # good to go
    connection,cursor = create_connection()
    # cursor.execute("SELECT CUSTID FROM CUSTOMERS ORDER BY CUSTID DESC")
    cursor.execute("SELECT MAX(CUSTID) FROM CUSTOMERS")
    row = cursor.fetchone()
    if row == None:
        close_conn(connection,cursor)
        return 0
    close_conn(connection,cursor)
    return (row[0])

def gen_random_customer_id():     # good to go
    connection,cursor = create_connection()
    # sql = """
    #     SELECT CUSTID FROM (
    #     SELECT CUSTID FROM CUSTOMERS ORDER BY DBMS_RANDOM.VALUE
    #     )
    #     WHERE ROWNUM = 1
    # """
    # cursor.execute(sql)
    # row = cursor.fetchone()
    # close_conn(connection,cursor)
    # return row[0]
    try:
        cursor.execute("SELECT MAX(CUSTID) FROM CUSTOMERS")
        maxm,=cursor.fetchone()
        cursor.execute("SELECT MIN(CUSTID) FROM CUSTOMERS")
        minm,=cursor.fetchone()
        close_conn(connection,cursor)
        return random.randint(minm,maxm)
    except:
        close_conn(connection,cursor)
        return 1

def gen_largest_order_id():   # good to go
    connection,cursor = create_connection()
    # cursor.execute("SELECT ORDID FROM ORDERS ORDER BY ORDID DESC")
    cursor.execute("SELECT MAX(ORDID) FROM ORDERS")
    row = cursor.fetchone()
    if row == None:
        close_conn(connection,cursor)
        return 0
    close_conn(connection,cursor)
    return (row[0])

def get_orders_ofcust(cust_id):    # good to go
    connection,cursor = create_connection()
    cursor.execute("SELECT ORDID FROM ORDERS WHERE CUSTID = :CUST_ID", CUST_ID = cust_id)
    rows = cursor.fetchall()
    orders = []
    for row in rows: 
        if row==None:
            return orders
        orders.append(row[0])
    close_conn(connection,cursor)
    return orders

def insert_customer(cust_id):   # good to go
    data = gen_random_cust(cust_id)
    connection,cursor = create_connection()
    
    insert_sql = """
        INSERT INTO CUSTOMERS VALUES (:CUSTID,:NAME,:ADDRESS,:CITY,:STATE,:ZIP,:AREA,:PHONE,:BUSINESSTYPE,:SALESMAN,:CUSTDETAILS1,:CUSTDETAILS2,:CREDITLIM)
    """
    try:
        cursor.execute(insert_sql,data)
        connection.commit()
        traceInfo(f"entries inserted successfully for customer with id: {str(data[0])}")
        close_conn(connection,cursor)
        return [True]
    except oracledb.DatabaseError as e:
        error, = e.args
        if error.message[0:9] == "ORA-00001":
            traceWarning(f"customer {cust_id} already present")
        else:
            traceError(f"entries could not be created for customer {cust_id} due to {error.message}")
        close_conn(connection,cursor)
        return [False,f"entries could not be created for customer {cust_id} due to {error.message}"]
    except Exception as e:
        connection.rollback()
        traceError(f"could not insert customer {cust_id} due to {str(e)}")
        close_conn(connection,cursor)
        return [False,f"could not insert customer {cust_id} due to {str(e)}"]

def insert_order(order_id,cust_id,connection,cursor):    # good to go
    data = gen_random_order(order_id,cust_id)
    insert_sql = """
        INSERT INTO ORDERS VALUES (:ORDID ,:CUSTID ,:PRIORITY ,:ORDERINFO ,:ORDERDATE ,:SHIPDATE ,:DELNAME ,:DELADDRESS,:DELCITY ,:DELSTATE ,:DELZIP)
    """
    try:
        cursor.execute(insert_sql,data)
        traceInfo(f"order added successfully for order with id: {str(data[0])}")
        return [True]
    except oracledb.DatabaseError as e:
        error, = e.args
        connection.rollback()
        traceError(f"order {order_id} could not be added due to {error.message}")
        return [False,f"order {order_id} could not be added due to {error.message}"]
    except Exception as e:
        traceError(f"could not add order {order_id} due to {str(e)}")
        connection.rollback()
        return [False,f"could not insert order {order_id} due to {str(e)}"]

def insert_items(order_id, products,connection,cursor):  # good to go
    item_id=1
    Products = {}
    for product in products:
        if Products.get(product,0)==0:
            Products[product] = 1
        else:
            Products[product]+=1
    for product,qty in Products.items():
        data = gen_random_items(order_id,item_id,product,qty)
        insert_sql = """
        INSERT INTO ITEMS VALUES (:ORDID ,:ITEMID ,:PRODID ,:WHID ,:SELLBYDATE,:PRICE ,:PACKAGES ,:QTY ,:DESCRIPTION,:ITEMTEXT)
        """
        try:
            cursor.execute(insert_sql,data)
        except oracledb.DatabaseError as e:
            error, = e.args
            #print(f"item for product {product} could not be added due to {error.message} under order {order_id}")
            traceError(f"item for product {product} could not be added due to {error.message} under order {order_id}")
            connection.rollback()
            return False
        except Exception as e:
            traceError(f"item for product {product} could not be added due to {str(e)} under order {order_id}")
            connection.rollback()
        item_id+=1
    traceInfo(f"items added successfully under order_id:{data[0]}")
    return True

def deleting_cust(cust_id):    # good to go
    connection,cursor = create_connection()
    delete_cust =""" DELETE FROM CUSTOMERS WHERE CUSTID = :ID """
    delete_order = """ DELETE FROM ORDERS WHERE CUSTID = :ID """
    delete_item = """ DELETE FROM ITEMS WHERE ORDID IN(
                            SELECT ORDID FROM ORDERS WHERE CUSTID=:ID
                        )
      """
    try:
        connection.begin()
        cursor.execute(delete_item,[cust_id])
        cursor.execute(delete_order,[cust_id])
        cursor.execute(delete_cust,[cust_id])
        connection.commit()
        traceInfo(f"successfully deleted customer with id:{cust_id}")
    except oracledb.DatabaseError as e:
        error, = e.args
        #print(f"could not delete customer_id {str(cust_id)} due to {error.message}")
        traceError(f"could not delete customer_id {str(cust_id)} due to {error.message}")
        connection.rollback()
        close_conn(connection,cursor)
        return [False,f"could not delete customer_id {str(cust_id)} due to {error.message}"]
    except Exception as e:
        #print(f"could not delete customer_id {str(cust_id)} due to {str(e)}")
        connection.rollback()
        close_conn(connection,cursor)
        traceError(f"could not delete customer_id {str(cust_id)} due to {str(e)}")
        return [False,"could not delete customer_id {str(cust_id)} due to {str(e)}"]
    close_conn(connection,cursor)
    return [True]

def deleting_order(order_id,connection,cursor):   # good to go
    delete_order =""" DELETE FROM ORDERS WHERE ORDID = :ID """
    delete_item=""" DELETE FROM ITEMS WHERE ORDID = :ID """
    try:
        connection.begin()
        cursor.execute(delete_item,[order_id])
        cursor.execute(delete_order,[order_id])
        connection.commit()
        traceInfo(f"succefully deleted order with id: {order_id}")
        return [True]
    except oracledb.DatabaseError as e:
        error, = e.args
        #print(f"could not delete order_id {str(order_id)} due to {error.message}")
        traceError(f"could not delete order_id {str(order_id)} due to {error.message}")
        connection.rollback()
        close_conn(connection,cursor)
        return [False,f"could not delete order_id {str(order_id)} due to {error.message}"]
    except Exception as e:
        #print(f"could not delete order_id {str(order_id)} due to {str(e)}")
        traceError(f"could not delete order_id {str(order_id)} due to {str(e)}")
        connection.rollback()
        close_conn(connection,cursor)
        return [False,f"could not delete order_id {str(order_id)} due to {str(e)}"]

def findcustomer(cust_id):  # good to go
    
    connection,cursor = create_connection()
    
    find_sql = """
    SELECT CUSTID FROM CUSTOMERS WHERE CUSTID = :CUSTOMER_ID
    """
    try:
        cursor.execute(find_sql, [cust_id])
        row = cursor.fetchone()
        close_conn(connection,cursor)
        if row==None:
            traceInfo(f"customer {str(cust_id)} is not present in database")
            return [True]
        else:
            #print(f"found customer {str(cust_id)} in databse")
            traceInfo(f"found customer {str(cust_id)} in database")
            return [True]
    except oracledb.DatabaseError as e:
        error, = e.args
        #print(f"could not find customer {str(cust_id)} due to error {error.message}")
        traceError(f"could not find customer {str(cust_id)} due to error {error.message}")
        close_conn(connection,cursor)
        return [False,f"could not find customer {str(cust_id)} due to error {error.message}"]
    except Exception as e:
        #print(f"could not add customer due to {str(e)}")
        close_conn(connection,cursor)
        traceError(f"could not find customer {str(cust_id)} due to error {str(e)}")
        return [False,f"could not add customer due to {str(e)}"]

def chckIf_oId_present(order_id,connection,cursor):
    find_sql = """
    SELECT ORDID FROM ORDERS WHERE ORDID = :ORDER_ID
    """
    try:
        cursor.execute(find_sql,[order_id])
        if len(cursor.fetchall())==0:
            return False
        else:
            return True
    except oracledb.DatabaseError as e:
        return False
    except Exception as e:
        return False

def place_order(cust_id,products):  # good to go
    connection,cursor = create_connection()
    new_orderId = gen_new_order_id(connection,cursor)
    times = 10
    while times>10:
        if chckIf_oId_present(new_orderId,connection,cursor)==True:
            new_orderId = gen_new_order_id(connection,cursor)
            times-=1
        else:
            break
    if chckIf_oId_present(new_orderId,connection,cursor)==True:
        traceError(f"Could Not Place order for {str(new_orderId)} for customer {str(cust_id)}")
        close_conn(connection,cursor)
        return [False,f"Could not place order for {str(new_orderId)}. Check if Sequence is Valid"]
    
    if insert_items(new_orderId,products,connection,cursor):
        isPlaced = insert_order(new_orderId,cust_id,connection,cursor)
    else:
        connection.rollback()
        close_conn(connection,cursor)
        return [False,isPlaced[1]]
    if isPlaced[0]:
        connection.commit()
    else:
        connection.rollback()
        close_conn(connection,cursor)
        return [False,isPlaced[1]]
    close_conn(connection,cursor)
    return [True]

def chckIf_cId_present(cust_id):
    connection,cursor = create_connection()
    find_sql = """
    SELECT CUSTID FROM CUSTOMERS WHERE CUSTID = :CUSTOMER_ID
    """
    try:
        cursor.execute(find_sql,[cust_id])
        if len(cursor.fetchall())==0:
            close_conn(connection,cursor)
            return False
        else:
            close_conn(connection,cursor)
            return True
    except oracledb.DatabaseError as e:
        close_conn(connection,cursor)
        return False
    except Exception as e:
        close_conn(connection,cursor)
        return False

def addnewcustomer(): # good to go
    new_cust = gen_new_customer_id()
    times = 10
    while times>0:
        if chckIf_cId_present(new_cust)==True:
            new_cust = gen_new_customer_id()
            times-=1
        else:
            break
    if chckIf_cId_present(new_cust)==True:
        return [False,f"could not gen new customer, check if your Sequence is valid"]
    return insert_customer(new_cust) 

def order_summary_ofCustomer(customer_id,startDateTime,endDateTime):
    connection,cursor = create_connection()
    start = datetime.strptime(startDateTime,datetime_format)
    end = datetime.strptime(endDateTime,datetime_format)
    localtime_ = time.strftime(datetime_format,time.localtime())
    localtime_ = datetime.strptime(localtime_,datetime_format)
    try:
        cursor.execute("SELECT ORDID,ORDERDATE,SHIPDATE, (SELECT SUM(PRICE*QTY) FROM ITEMS WHERE ITEMS.ORDID = ORDERS.ORDID) AS BILLL, CASE WHEN SHIPDATE> :LOCALTIME THEN 'PENDING' ELSE 'ORDER close_connD' END AS TRANSACTION FROM ORDERS WHERE CUSTID = :CUSTOMER_ID AND ORDERDATE>= :STARTDATE AND ORDERDATE<= :ENDDATE ORDER BY ORDERDATE ",CUSTOMER_ID = customer_id,STARTDATE = start,ENDDATE = end,LOCALTIME = localtime_)
        data = cursor.fetchall()
        close_conn(connection,cursor)
        if len(data)==0:
            logInfo(f"No order has been placed by customer {customer_id}")
            return [True]
        headers = ["Order ID", "Order Date", "Ship Date", "Bill", "Transaction Status"]
        logInfo(f"Order summary for customer {customer_id} was requested, successfully provided")
        traceInfo("\n########################################################\n"+"Summary of Order for customer_id "+str(customer_id)+": \n"+tabulate(data, headers=headers, tablefmt="grid") + "\n########################################################\n")
        return [True]
    except oracledb.DatabaseError as e:
        error, = e.args
        traceError(f"Could not gen order summary for customer {customer_id} due to {error.message}")
        logError(f"Could not gen order summary for customer {customer_id} due to {error.message}")
        close_conn(connection,cursor)
        return [False,f"Could not gen order summary for customer {customer_id} due to {error.message}"]
    except Exception as e:
        traceError(f"Could not gen order summary for customer {customer_id} due to {str(e)}")
        logError(f"Could not gen order summary for customer {customer_id} due to {str(e)}")
        close_conn(connection,cursor)
        return [False,f"Could not gen order summary for customer {customer_id} due to {str(e)}"]
    
def summarise_orders_forcust(customer_id,startDateTime,endDateTime):
    
    connection,cursor = create_connection()
    
    start = datetime.strptime(startDateTime,datetime_format)
    end = datetime.strptime(endDateTime,datetime_format)
    localtime_ = time.strftime(datetime_format,time.localtime())
    localtime_ = datetime.strptime(localtime_,datetime_format)
    try:
        cursor.execute("SELECT ORDID,ORDERDATE,SHIPDATE, (SELECT SUM(PRICE*QTY) FROM ITEMS WHERE ITEMS.ORDID = ORDERS.ORDID) AS BILLL, CASE WHEN SHIPDATE> :LOCALTIME THEN 'PENDING' ELSE 'ORDER close_connD' END AS TRANSACTION FROM ORDERS WHERE CUSTID = :CUSTOMER_ID AND ORDERDATE>= :STARTDATE AND ORDERDATE<= :ENDDATE ORDER BY ORDERDATE ",CUSTOMER_ID = customer_id,STARTDATE = start,ENDDATE = end,LOCALTIME = localtime_)
        data = cursor.fetchall()
        if len(data)==0:
            logInfo(f"No order has been placed by customer {customer_id}")
            return [True]
        headers = ["Order ID", "Order Date", "Ship Date", "Bill", "Transaction Status"]
        summary = "\n"+"Summary of Order for customer_id "+str(customer_id)+": \n"+tabulate(data, headers=headers, tablefmt="grid") + "\n"
        cursor.execute("SELECT ORDID FROM ORDERS WHERE CUSTID = :CUSTOMER_ID AND ORDERDATE>= :STARTDATE AND ORDERDATE<= :ENDDATE ORDER BY ORDERDATE ",CUSTOMER_ID = customer_id,STARTDATE = start,ENDDATE = end)
        rows = cursor.fetchall()
        orders = [order for order, in rows]
        header = ['ITEMID', 'QTY','PRODID','DESCRIPTION','PRICE']
        for order in orders:
            cursor.execute(f"SELECT ITEMID, QTY,PRODID,DESCRIPTION,PRICE FROM ITEMS WHERE ORDID={order}")
            data = cursor.fetchall()
            summary+="Ordersummary for order id "+str(order)+"\n"+tabulate(data,headers=header,tablefmt="grid")+"\n--------------------------------------------------------\n"
        close_conn(connection,cursor)
        logInfo(f"full order summary for customer {customer_id} was requested. Successfully provided")
        summary+="\n###################################################################\n"
        traceInfo(summary)
        return [True]
    except oracledb.DatabaseError as e:
        error, = e.args
        close_conn(connection,cursor)
        traceError(f"could not provide full order summary for customer {customer_id} due to {error.message}")
        logError(f"could not provide full order summary for customer {customer_id} due to {error.message}")
        return [False,f"could not provide full order summary for customer {customer_id} due to {error.message}"]
    except Exception as e:
        close_conn(connection,cursor)
        traceError(f"could not provide full order summary for customer {customer_id} due to {str(e)}")
        logError(f"could not provide full order summary for customer {customer_id} due to {str(e)}")

def update_address(cust_id,new_add):
    connection,cursor = create_connection()
    sql1 = """SELECT ADDRESS FROM CUSTOMERS WHERE CUSTID = :CUST_ID FOR UPDATE"""
    sql2 = """ UPDATE CUSTOMERS SET ADDRESS = :NEW_ADD WHERE CUSTID = :CUST_ID"""
    # sql3 = """SELECT DELADDRESS FROM ORDERS WHERE CUSTID = :CUSTID FOR UPDATE"""
    # sql4 = """ UPDATE ORDERS SET DELADDRESS=:NEW_ADD WHERE CUSTID=:CUST_ID"""
    try:
        cursor.execute(sql1,[cust_id])
        cursor.execute(sql2,[new_add,cust_id])
        # cursor.execute(sql3,[cust_id])
        # cursor.execute(sql4,[new_add,cust_id])
        connection.commit()
        traceInfo(f"updated customer {cust_id} address to {new_add}")
        # traceInfo(f"updated order address to {new_add} for customer {cust_id}")
        close_conn(connection,cursor)
        return [True]
    except oracledb.DatabaseError as e:
        error, = e.args
        traceError(f"address could not be updated for customer {cust_id} due to {error.message}")
        connection.rollback()
        close_conn(connection,cursor)
        return [False,f"address could not be updated for customer {cust_id} due to {error.message}"]
    except Exception as e:
        traceError(f"address could not be updated for customer {cust_id} due to {str(e)}")
        connection.rollback()
        close_conn(connection,cursor)
        return [False,f"address could not be updated for customer {cust_id} due to {str(e)}"]
    
def update_item(order_id,item_id,new_prodid,qty):
    connection,cursor = create_connection()
    sql1 = """ SELECT * FROM ITEMS WHERE ITEMID=:ITEM_ID AND ORDID=:ORDER_ID FOR UPDATE """
    update_cols = ['ORDID','ITEMID','PRODID','WHID','SELLBYDATE','PRICE','PACKAGES','QTY','DESCRIPTION','ITEMTEXT']
    data = gen_random_items(order_id,item_id,new_prodid,qty)
    if data==False:
        connection.rollback()
        close_conn(connection,cursor)
        traceError(f"could not proceed change item request for item {item_id} and order {order_id}")
        return [False,f"could not proceed change item request for item {item_id} and order {order_id}"]
    update_clause = ','.join(f"{col}=:{i}" for i,col in enumerate(update_cols))
    sql2 = f" UPDATE ITEMS SET {update_clause} WHERE ITEMID=:ITEM_ID AND ORDID = :ORDER_ID "
    try:
        bind_values = data+[item_id,order_id]
        cursor.execute(sql1,[item_id,order_id])  # lock
        length = len(cursor.fetchall())
        if length==0:
            traceInfo(f"no item with id {item_id} under order_id {order_id} was found")
            connection.commit()
            close_conn(connection,cursor)
            return [True]
        cursor.execute(sql2,bind_values)
        connection.commit()
        traceInfo(f"item details has been updated for item {item_id} and order {order_id}")
        close_conn(connection,cursor)
        return [True]
    except oracledb.DatabaseError as e:
        error, = e.args
        connection.rollback()
        traceError(f"item details could not be updated for item {item_id} and order {order_id} due to {error.message}")
        close_conn(connection,cursor)
        return [False,f"item details could not be updated for item {item_id} and order {order_id} due to {error.message}"]
    except Exception as e:
        connection.rollback()
        traceError(f"item details could not be updated for item {item_id} and order {order_id} due to {str(e)}")
        close_conn(connection,cursor)
        return [False,f"item details could not be updated for item {item_id} and order {order_id} due to {str(e)}"]

def delete_item(order_id, item_id):
    connection,cursor = create_connection()
    sql1 = """ SELECT * FROM ITEMS WHERE ORDID=:ORDER_ID AND ITEMID=:ITEM_ID"""
    sql2 = """ DELETE FROM ITEMS WHERE ORDID=:ORDER_ID AND ITEMID=:ITEM_ID """
    try:
        cursor.execute(sql1,[order_id,item_id])
        length = len(cursor.fetchall())
        if length==0:
            traceInfo(f"no item with id {item_id} under order_id {order_id} was found")
            connection.commit()
            close_conn(connection,cursor)
            return [True]
        cursor.execute(sql2,[order_id,item_id])
        connection.commit()
        traceInfo(f"successfully deleted item {item_id} with order_id {order_id}")
        close_conn(connection,cursor)
        return [True]
    except oracledb.DatabaseError as e:
        error, = e.args
        connection.rollback()
        close_conn(connection,cursor)
        traceError(f"could not delete item {item_id} with order_id {order_id} due to {error.message}")
        logError(f"could not delete item {item_id} with order_id {order_id} due to {error.message}")
        return [False,f"could not delete item {item_id} with order_id {order_id} due to {error.message}"]
    except Exception as e:
        connection.rollback()
        close_conn(connection,cursor)
        traceError(f"could not delete item {item_id} with order_id {order_id} due to {str(e)}")
        logError(f"could not delete item {item_id} with order_id {order_id} due to {str(e)}")
        return [False,f"could not delete item {item_id} with order_id {order_id} due to {str(e)}"]
