import argparse
import mysql.connector as mariadb
from mysql.connector import Error
import numpy as np
import pandas as pd
from getpass import getpass
import os


class functionaldetails():
    def __init__(self):
        return
    
    def get_database_connection(self, host, database, user, password):
        connection = mariadb.connect(host=host, database=database, user=user, password=password)
        if connection.is_connected():
            db_Info = connection.get_server_info()
            
        #cursor = connection.cursor()
        
        return(connection)

    def get_customer_details(self, dbconn, ssn, lastname):
        cursor = dbconn.cursor()
        query="select * from cdw_sapp_customer where substr(ssn,6,4) = %s and Last_name = %s"
        cursor.execute(query,(ssn, lastname,))
        records = cursor.fetchall()

        return records

    def print_cust_details(self, records):
        if len(records) == 0:
            print('Could find any account details with given input')
        else:
            print("SSN:\t\t"+ '***-**-'+ssn)
            print("Name:\t\t"+(records[0][1]+' '+records[0][2].title()+' '+records[0][3]))
            print("CC No:\t\t"+records[0][4])
            print("Address:\t" + records[0][5])
            print("\t\t" + records[0][6])
            print("\t\t" + records[0][7]+','+records[0][8]+' - '+str(records[0][9]))
            print("Ph No:\t\t" + records[0][10])
            print("Email:\t\t" + records[0][11])
        
        return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get or Update Customer Details by provinding last name and last 4 digits of ssn')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--getacct', action='store_true', help='get customer account details')
    group.add_argument('--updateacct', action='store_true', help='update customer account details')
    group.add_argument('--getbill', action='store_true', help='get monthly bill')
    group.add_argument('--gettx', action='store_true', help='get transaction details for customer')
    parser.add_argument('--lastname', required=True)
    parser.add_argument('--ssnlastfour', required=True)
    args = parser.parse_args()

    option = 8

    print("Select an option (1-8):")

    print('1)    Used to display the transactions made by customers living in a given zip code for a given month and year. \
                 Order by day in descending order. \
        2)    Used to display the number and total values of transactions for a given type. \
        3)    Used to display the number and total values of transactions for branches in a given state.\
        4)  Used to check the existing account details of a customer. \
        5)  Used to modify the existing account details of a customer. \
        6)  Used to generate a monthly bill for a credit card number for a given month and year. \
                    7) Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order. \
        8)  Exit')

    while(option !=8 ):
        pass
    
        #read the database username and password from secret.txt
    secrets_file = os.path.join("files", "secret.txt")
    with open(secrets_file, "r") as f:		
        lines = f.readlines()
    for line in lines:
        words = line.split("=")
        if (words[0] == "user"):
            user = words[1].strip()
        elif (words[0] == "password"):
            password = words[1].strip()
    f.close()


    cd = functionaldetails()
    lastname = args.lastname
    ssn = args.ssnlastfour
    # read from file, atleast the secret arguments
    host='localhost'
    database='project_db'
    user='root'
    password='root'
    dbconn = cd.get_database_connection(host, database, user, password)
    # get customer account details
    if args.getacct:
        records = cd.get_customer_details(dbconn, ssn, lastname)
        cd.print_cust_details(records)
    # update customer account details
    if args.updateacct:
        records = cd.get_customer_details(dbconn, ssn, lastname)
        print("Accept the value or provide an updated value:")
        displaystr = "Ph No (" + records[0][10] + "): "
        newphone = input(displaystr)
        if (newphone):
            print(newphone)
        else:
            print("keeping existing value")
