import mysql.connector as mariadb
from mysql.connector import Error
import numpy as np
import pandas as pd
from getpass import getpass
import os

import re


class FunctionalDetails():
    def __init__(self):
        return
    
    def get_database_connection(self, host, database, user, password):
        connection = mariadb.connect(host=host, database=database, user=user, password=password)
        if connection.is_connected():
            db_Info = connection.get_server_info()
        return(connection)

    def get_user_input(self):            
        flag = True
        zip_code = input("Enter Your Zip Code:  ")
        while (flag):
            if not zip_code.isdigit() or zip_code == 0:
                zip_code = input('enter a valid zipcode:  ')
            else:
                flag=False
        
        transaction_year = input("Enter the year for transactions records:  ")
        flag=True
        while (flag):
            if not transaction_year.isdigit() or len(transaction_year)!=4:
                transaction_year = input("Enter valid year format XXXX:  ")
            else:
                flag = False
                
        transaction_month = input("Enter the month in the form (XX):  ").zfill(2)
        flag = True
        while (flag):
            if  not transaction_month.isdigit():
                transaction_month = input('Please enter valid month:  ')
            else:
                flag=False
        return(zip_code,transaction_month,transaction_year)

    def get_tn_type_input(self,dbconn):
        transaction_type = input("Enter The Type Of Transaction :    ")
        flag = True
        cursor = dbconn.cursor()

        query = "select distinct(transaction_type) from cdw_sapp_credit_card"
        
        cursor.execute(query,)
        records = cursor.fetchall()
        
        type_lst =[]
        for row in records:
            type_lst += row
            
        while flag:
            if transaction_type.title() not in type_lst:
                print("Please select the from the following list:")
                print(type_lst)
                transaction_type =input('Invalid transaction_type, Re enter the transaction type:  '   )
            else:
                flag=False
        return(transaction_type)
    
    def get_branch_input(self,dbconn):
        
        cursor = dbconn.cursor()
        flag = True
        query = "select distinct(branch_state) from cdw_sapp_branch" 
        cursor.execute(query,)
        records = cursor.fetchall()
        branch_state = input("Enter the state in XX format :    ")
        state_lst =[]
        for row in records:
            state_lst += row
        while flag:
            if branch_state.upper() not in state_lst:
                print(state_lst)
                branch_state =input('Please select the state from the list:   '   )
            else:
                flag=False
            
            return(branch_state)

    def get_input(self,dbconn):

        cursor = dbconn.cursor()
        flag = True  
        ssn = getpass("Enter the last 4 digits of SSN:    " )
        while flag:
            if not ssn.isdigit() or len(ssn) != 4:
                ssn =getpass('Invalid ssn, Re enter the ssn without -:    ')
            else:
                flag=False
                
        lastname = input('Enter the lastname:   ')
        query = 'select last_name from cdw_sapp_customer'
        cursor.execute(query,)
        records = cursor.fetchall()
        name_lst = []
        flag = True
        for record in records:
            name_lst += record
        while flag:
            if lastname.title() not in name_lst:
                print('Last name not found in the records')
                y_input = input('press y to continue, n to exit:   ')
                if y_input.lower() == 'y':
                    lastname = input('Enter the lastname:   ')
                else:
                    print('Please come back again')
                    return(0,0,0)
            else:
                flag=False
        query="select * from cdw_sapp_customer where substr(ssn,6,4) = %s and Last_name = %s"
    
        cursor.execute(query,(ssn,lastname,))
        
        records = cursor.fetchall()
        
        if len(records) == 0:
            print('Could find any account with given details.')
        
        return(records[0][0],ssn,lastname)

   
    def get_monthyear(self):
        transaction_year = input("Enter the year for transactions records:  ")
        flag=True
        while (flag):
            if not transaction_year.isdigit() or len(transaction_year)!=4:
                transaction_year = input("Enter valid year format XXXX:  ")
            else:
                flag = False
            
        transaction_month = input("Enter the month in the form (XX):  ").zfill(2)
        flag = True
        while (flag):
            if  not transaction_month.isdigit():
                transaction_month = input('Please enter valid month:   ')
            else:
                flag=False
        return(transaction_month,transaction_year)

    
    def get_range(self):
            
        start_date = input("Enter the start date for transactions recordsin the format XXXXXXXX:  ")
        flag=True
        while (flag):
            if not start_date.isdigit() or len(start_date)!=8:
                start_date = input("Enter valid Date format XXXXXXXX[20180516,yearmndy]:   ")
            else:
                flag = False
            
        end_date = input("Enter the end date in the form XXXXXXXX[20180516,yearmndy]: ")
        flag = True
        while (flag):
            if not end_date.isdigit() or len(end_date)!=8:
                end_date = input('Please enter valid date in the format XXXXXXXX[20180516,yearmndy]:   ')
            else:
                flag=False
                
        if start_date>=end_date:
            print('please enter valid range')
            flag=True
            while (flag):
                if not start_date.isdigit() or len(start_date)!=8:
                    start_date = input("Enter valid year format XXXXXXXX[20180516,yearmndy]:  ")
                else:
                    flag = False

            end_date = input("Enter the end date in the form (XXXXXXXX):  ")
            flag = True
            while (flag):
                if not end_date.isdigit() or len(end_date)!=8:
                    end_date = input('Please enter valid date in the format XXXXXXXX[20180516,yearmndy]:   ')
                else:
                    flag=False
        return(start_date,end_date)

    
    def total_customers(self,zipcode,year,month,dbconn):
        cursor = dbconn.cursor()
        query ="SELECT CUST_CC_NO,date_format(TIMEID,'%Y-%m-%d') as transaction_date,CUST_SSN,BRANCH_CODE, \
        TRANSACTION_TYPE,TRANSACTION_VALUE,TRANSACTION_ID \
        FROM (SELECT ssn   from cdw_sapp_customer  where cust_zip = %s) as customer \
        join cdw_sapp_credit_card on cdw_sapp_credit_card.cust_ssn = customer.ssn\
        where SUBSTRING(timeid, 1, 4)  = %s and SUBSTRING(timeid, 5, 2)  = %s \
        Group by timeid desc;"

        cursor.execute(query,(zipcode,year,month,))
        records = cursor.fetchall()
        print("\n List of  customer transactions in a given Zip code : ")
        print("\n\n Customer no\t\tdate\t   cust_ssn  code  type       value  id")
        if len(records) == 0:
            print('No records found with the given details.')
        else:
            for row in records:
                print(row)
        return(records)

    def transaction_value(self,dbconn,tn_type):
        cursor = dbconn.cursor()
        query = "SELECT count(*),sum(transaction_value) from cdw_sapp_credit_card where transaction_type = %s"

        cursor.execute(query,(tn_type,))
        records = cursor.fetchall()
        print("\n Number of transactions and total value for transaction type of " + tn_type + " : ")
        if len(records) == 0:
            print('No records found with the given details.')
        else:
            print(records)
        return(records)

    def transaction_value_branch(self,branch_state,dbconn):

        cursor = dbconn.cursor()

        query="select count(transaction_type) , sum(transaction_value) from cdw_sapp_credit_card \
                    join cdw_sapp_branch on cdw_sapp_branch.branch_code = cdw_sapp_credit_card.BRANCH_CODE \
                    where cdw_sapp_branch.branch_state = %s \
                    group by cdw_sapp_branch.branch_code;"
        cursor.execute(query,(branch_state,))
        records = cursor.fetchall()
        if len(records) ==0:
            print('No recods found with the given details.')
        else:
            for row in records:
                print(row)
        return(records)
        
    def customer_details(self,dbconn,ssn,lastname):
        
        cursor = dbconn.cursor()
        query="select * from cdw_sapp_customer where substr(ssn,6,4) = %s and Last_name = %s"
    
        cursor.execute(query,(ssn,lastname,))
        
        records = cursor.fetchall()
        
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
        return(records)

    def monthly_bill_per_year(self,dbconn,transaction_year,transaction_month,cust_ssn):
        query = 'select cc.cust_cc_no, sum(cc.transaction_value) from cdw_sapp_credit_card as cc\
        join cdw_sapp_customer as c on c.ssn = cc.cust_ssn \
        where SUBSTRING(timeid, 1, 4)  = %s and SUBSTRING(timeid, 5, 2)  = %s and c.ssn = %s'

        cursor = dbconn.cursor()

        cursor.execute(query,(transaction_year,transaction_month,cust_ssn,))
        records = cursor.fetchall()
        print("\n List of  customer transactions on a given credit card number : ")
        if len(records) == 0:
            print("No records found with the given details.")
        else:
            print(records)
        return(records)

    def monthly_bill_between_dates(self,dbconn, start_date,end_date,record_ssn):
                
        cursor = dbconn.cursor()

        query = 'select cc.* from cdw_sapp_credit_card as cc\
        join cdw_sapp_customer as c on c.ssn = cc.cust_ssn \
        where cc.cust_ssn = %s and cc.timeid between  %s and %s \
        order by cc.timeid desc'

        cust_ssn = record_ssn
        cursor.execute(query,(cust_ssn,start_date,end_date,))
        records = cursor.fetchall()
        if len(records) == 0:
            print('No records found for the given details.')
        else:
            print("\n List of  customer transactions on a given credit card number : ")
            for row in records:
                print(row)
        return(records)

    def modify_customer_details(self,dbconn,records,record_ssn):

        cursor = dbconn.cursor()
            
        #retreving the existing account details
        ssn=records[0][0]    
        full_street_address = records[0][5]
        city=records[0][6]
        state=records[0][7]
        country = records[0][8]
        zipcode=records[0][9]
        email=records[0][11]
        phno=records[0][10]
        
        print("\n\nEnter the values if you want to UPDATE any field or Press ENTER to keep existing values\n\n")
        
        #getting input from the user to modify the account details
        streetaddr_question = 'Street address('+full_street_address+'):'
        streetaddr_new = input(streetaddr_question)
        if not streetaddr_new:
            streetaddr_new = full_street_address
        else:
            streetaddr_new = streetaddr_new.split(',')
            streetaddr_new = streetaddr_new[0] +','+streetaddr_new[1].title()

            
        city_new_question = "City("+ city + "): "
        city_new = input(city_new_question)
        if not city_new:
            city_new = city

        
        state_question = 'State('+state+'):'
        state_new = input(state_question)
        if not state_new:
            state_new = state
        else:
            state_temp = state_new.title()
            state_new = state_validation(state_temp)
            
        # country_question = 'Country('+country+'):'
        # country_new = input(country_question)
        # if not country_new:
        #     country_new = country
        country_new=country
  
        zip_question = 'Zip Code('+str(zipcode)+'):'
        zip_new = input(zip_question).zfill(5)
        if not zip_new:
            zip_new = zipcode
        if int(zip_new) not in range(501,99950):
            print('Invalid ZIP CODE: retaining previous value ')
            zip_new = zipcode

        # https://facts.usps.com/42000-zip-codes/
        # ZIP Codes range from 00501, belonging to the Internal Revenue Service in Holtsville, NY, to 99950 in Ketchikan, AK. 
        # Easiest to remember? How about 12345, a unique ZIP Code for General Electric in Schenectady, NY.


        phno_question = 'Phone Number('+phno+'):'
        phno_new = input(phno_question)
        if not phno_new:
            phno_new = phno
        else:
            if not phno_new.isdigit() or int(phno_new) == 0 or len(phno_new)!=7:
                print('Invalid phone no: retaining previous value ')
                phno_new = phno
            else:
                phno_new = phno_new[0:3] +'-'+phno_new[3:7] #formatting the phone number to the mapping document
            
        email_question = 'Email('+email+'):'
        email_new = input(email_question)
        if not email_new:
            email_new = email
        else:
            regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            if not (re.fullmatch(regex, email_new)):
                print('Invalid Email. Retaining the previous value')
                email_new = email


        # from validate_email import validate_email
        #     is_valid = validate_email(email_address='example@example.com', \
        #     check_regex=True, check_mx=True, \
        #     from_address='my@from.addr.ess', helo_host='my.host.name', \ 
        #     smtp_timeout=10, dns_timeout=10, use_blacklist=True)

        
        #formatting the user input before updating the database
        city_new =city_new.title()
        country_new = country_new.title()
        #state_new=state_new.upper()
           
        #updating the database
        
        query = 'update cdw_sapp_customer \
        set full_street_address = %s, cust_city = %s, \
        cust_state = %s ,cust_country = %s, \
        cust_zip = %s, cust_phone = %s, \
        cust_email=%s \
        where ssn = %s'

        cursor.execute(query,(streetaddr_new,city_new,state_new,country_new,zip_new,phno_new,email_new, ssn,))
        
        print('\n\nSucessfully updated')
        
        query = 'select * from cdw_sapp_customer where ssn = %s'
        cursor.execute(query,(ssn,))
        
        #printing the account details after the update
        
        record = cursor.fetchall()
        print("\n\nUpdated Account Details:\n")
        print("SSN:\t\t"+ '***-**-'+str(record_ssn))
        print("Name:\t\t"+(record[0][1]+' '+record[0][2].title()+' '+record[0][3]))
        print("CC No:\t\t"+record[0][4])
        print("Address:\t" + record[0][5])
        print("\t\t" + record[0][6])
        print("\t\t" + record[0][7]+','+record[0][8]+' - '+str(record[0][9]))
        print("Ph No:\t\t" + record[0][10])
        print("Email:\t\t" + record[0][11])

        dbconn.commit()
        return


def state_validation(state):
    us_state_to_abbrev = {
        "Alabama": "AL",
        "Alaska": "AK",
        "Arizona": "AZ",
        "Arkansas": "AR",
        "California": "CA",
        "Colorado": "CO",
        "Connecticut": "CT",
        "Delaware": "DE",
        "Florida": "FL",
        "Georgia": "GA",
        "Hawaii": "HI",
        "Idaho": "ID",
        "Illinois": "IL",
        "Indiana": "IN",
        "Iowa": "IA",
        "Kansas": "KS",
        "Kentucky": "KY",
        "Louisiana": "LA",
        "Maine": "ME",
        "Maryland": "MD",
        "Massachusetts": "MA",
        "Michigan": "MI",
        "Minnesota": "MN",
        "Mississippi": "MS",
        "Missouri": "MO",
        "Montana": "MT",
        "Nebraska": "NE",
        "Nevada": "NV",
        "New Hampshire": "NH",
        "New Jersey": "NJ",
        "New Mexico": "NM",
        "New York": "NY",
        "North Carolina": "NC",
        "North Dakota": "ND",
        "Ohio": "OH",
        "Oklahoma": "OK",
        "Oregon": "OR",
        "Pennsylvania": "PA",
        "Rhode Island": "RI",
        "South Carolina": "SC",
        "South Dakota": "SD",
        "Tennessee": "TN",
        "Texas": "TX",
        "Utah": "UT",
        "Vermont": "VT",
        "Virginia": "VA",
        "Washington": "WA",
        "West Virginia": "WV",
        "Wisconsin": "WI",
        "Wyoming": "WY",
        "District of Columbia": "DC",
        "American Samoa": "AS",
        "Guam": "GU",
        "Northern Mariana Islands": "MP",
        "Puerto Rico": "PR",
        "United States Minor Outlying Islands": "UM",
        "U.S. Virgin Islands": "VI",
        }

    # invert the dictionary
    abbrev_to_us_state  = dict(map(reversed, us_state_to_abbrev.items()))
    if (state in abbrev_to_us_state):
        state_abrev = abbrev_to_us_state[state]
        return(state_abrev)
    else:
        return



if __name__ == "__main__":

    fd = FunctionalDetails()
    host='localhost'
    database='creditcard_capstone'
    # read the database username and password from secret.txt
    user = os.getenv("user", default=None)
    password = os.getenv("password", default=None)
    
    dbconn = fd.get_database_connection(host, database, user, password)

    option = '0'

    while(option != '8' ):
        
        print("\nList of Options:")
        print("----------------")
        print('1)    Used to display the transactions made by customers living in a given zip code for a given month and year. \
            Order by day in descending order. \
            \n2)    Used to display the number and total values of transactions for a given type. \
            \n3)    Used to display the number and total values of transactions for branches in a given state.\
            \n4)    Used to check the existing account details of a customer. \
            \n5)    Used to modify the existing account details of a customer. \
            \n6)    Used to generate a monthly bill for a credit card number for a given month and year. \
            \n7)    Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order. \
            \n8)    Exit')
        option = input('\nSelect an option from list : ')
        if (option == '1'):
            zipcode,month,year = fd.get_user_input()
            records = fd.total_customers(zipcode,year,month,dbconn)
        elif (option == '2'):
            tn_type = fd.get_tn_type_input(dbconn)
            records = fd.transaction_value(dbconn,tn_type)
        elif (option == '3'):
            branch_state = fd.get_branch_input(dbconn)
            records = fd.transaction_value_branch(branch_state,dbconn)
        elif (option == '4'):
            record_ssn,ssn,lastname = fd.get_input(dbconn)
            records = fd.customer_details(dbconn,ssn,lastname)
        elif (option == '5'):
            record_ssn,ssn,lastname = fd.get_input(dbconn)
            records = fd.customer_details(dbconn,ssn,lastname)
            fd.modify_customer_details(dbconn,records,ssn)
        elif (option == '6'):
            record_ssn,ssn,lastname = fd.get_input(dbconn)
            month,year = fd.get_monthyear()
            records = fd.monthly_bill_per_year(dbconn,year,month,record_ssn)
        elif (option == '7'):
            start,end = fd.get_range()
            record_ssn,ssn,lastname = fd.get_input(dbconn)
            records = fd.monthly_bill_between_dates(dbconn,start,end,record_ssn)
        elif (option == '8'):
            dbconn.close()
            print('THANK YOU')
            break
        else:
            print('Invalid Input.\nPlease try again.')
            continue
        print("\nPress Enter to Continue.")
        input()
    