import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, timedelta
import string
import random

DB_NAME = 'testdB'

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
                              host=xxxxxx) #Enter name of the endpoint in AWS
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
        "  `CustomerID` int(11) NOT NULL AUTO_INCREMENT,"
        "  `Name` varchar(28) NOT NULL,"
        "  `Email` varchar(28) NOT NULL,"
        "  `Product_Purchased` varchar(16) NOT NULL,"
        "  `Income` int(15) NOT NULL,"
        "  `State` varchar(28) NOT NULL,"
        "  `Purchase_date` date NOT NULL,"
        "  PRIMARY KEY (`CustomerID`)"
        ") ENGINE=InnoDB")
        for name, ddl in TABLES.items():
            try:
                cursor.execute("DROP TABLE Product_Customer_Info")
                print("Creating table {}: ".format(name), end='')
                cursor.execute(ddl)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else:
                print("OK")
        add_customer = ("INSERT INTO Product_Customer_Info "
                  "(CustomerID, Name, Email, Product_Purchased, Income, State, Purchase_date) "
                  "VALUES (%(CustomerId)s, %(Name)s, %(Email)s, %(Product_Purchased)s, %(Income)s, %(State)s, %(Purchase_date)s)")
        # Insert customer information
        product = ['iPhone', 'iPad', 'iWatch', 'Mac', 'Charger', 'Earphones']
        State = ['New York', 'California', 'Nevada', 'Iowa', 'Wyoming', 'Montana', 'Washington']
        # Keep a count of the number of entries
        iterations=0
        for x in range(100):
            now = date(2009, 5, 5)+timedelta(x)
            productrandomindex = random.randint(0,len(product)-1) 
            staterandomindex = random.randint(0,len(State)-1)             
            data_customer = {
                'CustomerId': x+1,
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
            for row in cursor:
                print(row)
        print("Completed creating the table with ",iterations," customers")
    cursor.close()
cnx.close()

