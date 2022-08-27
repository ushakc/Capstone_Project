import dash
from dash import html, dcc
import plotly.express as px
import pandas as pd
import sys
import os
import findspark
import spark
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *


dash.register_page(__name__)

markdown_text = '''
### Functional Requirements 5.2
Create and plot a chart that shows the difference in application approvals based on Property Area.
'''

# get the database username and password from secret.txt
secrets_file = os.path.join("..", "files", "secret.txt")
with open(secrets_file, "r") as file1:
    secret_lines = file1.readlines()
for line in secret_lines:
    words = line.split("=")
    if (words[0] == "user"):
        user = words[1].strip()
    elif (words[0] == "password"):
        password = words[1].strip()

spark = SparkSession.builder.appName('req5_data_visual').getOrCreate()
df = spark.read\
     .format("jdbc")\
     .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone")\
     .option("dbtable", "creditcard_capstone.cdw_sapp_loan_application")\
     .option("user", user)\
     .option("password", password)\
     .load()
pd_df_loan = df.toPandas()
pd_df_property = pd_df_loan[['Application_ID','Application_Status','Property_Area']]
pd_as = pd_df_property.groupby(['Property_Area', 'Application_Status'])['Application_ID'].count().to_frame().reset_index()
pd_as = pd_as.rename(columns={"Application_ID": "Approved"})
fig = px.pie(pd_as, values='Approved', names='Property_Area')

layout = html.Div([
    dcc.Markdown(children=markdown_text),
    dcc.Graph(
        id='req-5-2',
        figure=fig
    )
])