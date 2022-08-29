# Capstone Project for Data Engineering Boot Camp
This project is executed as part of Capstone Project for Data Engineering Course from Per Scholas

## Overview
Manage ETL process and Data Analysis and Visualization for:
- Credit Card System
-  Loan Application System

## Architecture

## Configuration
Created virtual environment for the project. The username and password for the maria db are stored as environment variables and pushed into gitignore
- Install MariaDB
- Install MySQL Workbench
- Create MySQL Database
- Install Spark, Pyspark
- Setup environment variables for database username and password

## Execution
For Module -1 Data Extraction and Visualization of json files:
##### Before adding the data to the tables, drop the existing tables.
##### Then set the primary keys and foreign keys
#####  Alter tables to format the columns according to the mapping document

###

For module 2,
Its an Interactive Console program. 
Execute Application , for modules 2.1 and 2.2

```
python cust_details.py

```

### Clone repository and copy input files
- Clone this git repository
- Download the input files

### Credit Card System

#### Extract, Transform and Load 
- Create files directory
- Copy input files
- Start Jupyter Notebook
- Execute branch.ipynb
- Execute customer.ipynb
- Execute creditcard.ipynb
- Copy SQL statements to MySQL Client and Execute
This would update the primary keys and change the table schema

#### Data Analysis and Visualization
- Change to req3 directory
```
cd req3
```
- Execute Application
```
python dash_req3.py
```
- In browser window, navigate to [Application](http://127.0.0.1:8050)

### Loan Application System

#### Extract and Load
- Execute loandata.ipynb
- Copy SQL statements to MySQL Client and Execute

#### Data Visualization for Loan Application System
- Change to req5 directory
```
cd req5
```
- Execute Application
```
python dash_req5.py
```
- In browser window, navigate to [Application](http://127.0.0.1:8055)







