import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, timedelta
import string
import random

DB_NAME = 'haas_sql_class'
NUM_CUSTOMERS = 1000
NUM_TRANSACTIONS = 1000

def id_generator(size=6, chars=string.ascii_uppercase):
    return ''.join(random.choice(chars) for _ in range(size))

def email_generator(size=6, chars=string.ascii_lowercase):
    return ''.join(random.choice(chars) for _ in range(size))

def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

passwd=input("Please enter the password for the database: ")

try:
    cnx = mysql.connector.connect(user='sqladmin', password=passwd,
                              host='sql101db.cgpndeefyiyy.us-east-1.rds.amazonaws.com')
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    else:
        print(err)
else:
    cursor = cnx.cursor()
    try:
        cnx.database = DB_NAME  #comment this out and replace with create_database for the first time
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            cnx.database = DB_NAME
        else:
            print(err)
    else:
        TABLES = {}
        TABLES['Product_Customer_Info'] = (
        "CREATE TABLE `Product_Customer_Info` ("
        "  `TransactionID` int(11) NOT NULL AUTO_INCREMENT,"
        "  `Name` varchar(28) NOT NULL,"
        "  `Email` varchar(28) NOT NULL,"
        "  `Product_Purchased` varchar(16) NOT NULL,"
        "  `Income` int(15) NOT NULL,"
        "  `State` varchar(28) NOT NULL,"
        "  `Purchase_date` date NOT NULL,"
        "  PRIMARY KEY (`TransactionID`)"
        ") ENGINE=InnoDB")
        
        TABLES['Product_Info'] = (
        "CREATE TABLE `Product_Info` ("
        "  `ProductID` int(4) NOT NULL,"            
        "  `Product_Name` varchar(16) NOT NULL,"
        "  `Price`  int(11) NOT NULL,"
        "  PRIMARY KEY (`ProductID`), UNIQUE KEY `Product_Name` (`Product_Name`)"
        ") ENGINE=InnoDB")
        
        TABLES['Customer_Info'] = (
        "CREATE TABLE `Customer_Info` ("
        "  `CustomerID` int(11) NOT NULL,"            
        "  `Name` varchar(28) NOT NULL,"
        "  `Email` varchar(28) NOT NULL,"
        "  `Income` int(15) NOT NULL,"
        "  `State` varchar(28) NOT NULL,"
        "  PRIMARY KEY (`CustomerID`), UNIQUE KEY `Name` (`Name`)"
        ") ENGINE=InnoDB")        

        TABLES['Sales_Info'] = (
        "CREATE TABLE `Sales_Info` ("
        "  `CustomerID` int(11) NOT NULL,"            
        "  `ProductID` int(4) NOT NULL,"            
        "  `StoreID` int(4) NOT NULL,"            
        "  `Quantity` int(4) NOT NULL,"            
        "  `Purchase_date` date NOT NULL"
        ") ENGINE=InnoDB")


        for name, ddl in TABLES.items():
            try:
                if (name=='Product_Customer_Info'):
                    cursor.execute("DROP TABLE Product_Customer_Info")
                elif (name=='Product_Info'):
                    cursor.execute("DROP TABLE Product_Info")
                elif (name=='Customer_Info'):
                    cursor.execute("DROP TABLE Customer_Info")
                elif (name=='Sales_Info'):
                    cursor.execute("DROP TABLE Sales_Info")
                else:
                    print("Unknown table name ",name)
                print("Dropping table {}: ".format(name), end='')
                cursor.execute(ddl)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                elif err.errno == errorcode.ER_BAD_TABLE_ERROR:
                    print("Unknown table")
                else:
                    print(err.msg)
            else:
                print("OK")
        
            if (name=='Product_Customer_Info'):
                #Product_Customer_Info table related operations
        
                add_customer = ("INSERT INTO Product_Customer_Info "
                          "(TransactionID, Name, Email, Product_Purchased, Income, State, Purchase_date) "
                          "VALUES (%(TransactionID)s, %(Name)s, %(Email)s, %(Product_Purchased)s, %(Income)s, %(State)s, %(Purchase_date)s)")
                # Insert customer information
                product = ['iPhone', 'iPad', 'iWatch', 'Mac', 'Charger', 'Earphones']
                State = ['New York', 'California', 'Nevada', 'Iowa', 'Wyoming', 'Montana', 'Washington']
                # Keep a count of the number of entries
                iterations=0
                for x in range(NUM_TRANSACTIONS):
                    now = date(2018, 5, 5)+timedelta(random.randint(1,101))
                    productrandomindex = random.randint(0,len(product)-1) 
                    staterandomindex = random.randint(0,len(State)-1)             
                    data_customer = {
                        'TransactionID': x+1,
                        'Name': id_generator(),
                        'Email': email_generator()+'@gmail.com',
                        'Product_Purchased': str(product[productrandomindex]),
                        'Income':random.randint(100000,10000000),
                        'State': str(State[staterandomindex]),
                        'Purchase_date': now
                    }
                    try:
                        cursor.execute(add_customer,data_customer)
                    except mysql.connector.Error as err:
                        print(err.msg)
                    else:
                        cnx.commit()
                        iterations+=1
                    for row in cursor:
                        print(row)
                print("Completed creating the product_customer table with ",iterations," customers")
            
            elif (name=='Product_Info'):
                #Product_Info table operations
            
                add_product = ("INSERT INTO Product_Info "
                          "(ProductID, Product_Name, Price)"
                          "VALUES (%(ProductId)s, %(Product_Name)s, %(Price)s)")
                # Insert product information
                pid = [1, 2, 3, 4, 5, 6]
                pname = ['iPhone', 'iPad', 'iWatch', 'Mac', 'Charger', 'Earphones']
                prodprice = [999, 1200, 399, 2500, 39, 129]
                iterations=0
                for x in range(6):
                    data_product = {
                        'ProductId': int(pid[x]),
                        'Product_Name': str(pname[x]),
                        'Price': int(prodprice[x])
                    }
                    try:
                        cursor.execute(add_product,data_product)
                    except mysql.connector.Error as err:
                        print(err.msg)
                    else:
                        cnx.commit()
                        iterations+=1
                    for row in cursor:
                        print(row)
                print("Completed creating the product table with ",iterations," products")

            elif (name=='Customer_Info'):
                #Customer_Info table operations
            
                add_customer_info = ("INSERT INTO Customer_Info "
                                  "(CustomerID, Name, Email, Income, State) "
                                  "VALUES (%(CustomerID)s, %(Name)s, %(Email)s, %(Income)s, %(State)s)")
                # Insert customer information
                State_Info = ['New York', 'California', 'Nevada', 'Iowa', 'Wyoming', 'Montana', 'Washington']
                # Keep a count of the number of entries
                iterations=0
                for x in range(NUM_CUSTOMERS):
                    staterandomindex = random.randint(0,len(State_Info)-1)             
                    data_customer_info = {
                        'CustomerID': x+1,
                        'Name': id_generator(),
                        'Email': email_generator()+'@gmail.com',
                        'Income':random.randint(100000,10000000),
                        'State': str(State[staterandomindex])
                    }
                    try:
                        cursor.execute(add_customer_info,data_customer_info)
                    except mysql.connector.Error as err:
                        print(err.msg)
                    else:
                        cnx.commit()
                        iterations+=1
                    for row in cursor:
                        print(row)
                print("Completed creating the customer info table with ",iterations," customers")


            elif (name=='Sales_Info'):
                #Sales_Info table operations    
                add_sales_info = ("INSERT INTO Sales_Info "
                                  "(CustomerID, ProductID, StoreID, Quantity, Purchase_date) "
                                  "VALUES (%(CustomerID)s, %(ProductID)s, %(StoreID)s, %(Quantity)s, %(Purchase_date)s)")
               # Keep a count of the number of entries
                iterations=0
                for x in range(NUM_TRANSACTIONS):
                    now_si = date(2018, 5, 5)+timedelta(random.randint(1,101))
                    data_sales_info = {
                        'CustomerID': random.randint(1,NUM_CUSTOMERS+100),
                        'ProductID': random.randint(1,6),
                        'StoreID': random.randint(1,20),
                        'Quantity':random.randint(1,10),
                        'Purchase_date': now_si
                    }
                    try:
                        cursor.execute(add_sales_info,data_sales_info)
                    except mysql.connector.Error as err:
                        print(err.msg)
                    else:
                        cnx.commit()
                        iterations+=1
                    for row in cursor:
                        print(row)
                print("Completed creating the sales info table with ",iterations," transactions")            
                
            else:
                print("Bad table name ", name)
    cursor.close()
cnx.close()

