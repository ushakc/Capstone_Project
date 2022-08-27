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
### Functional Requirements 5.3
Create a multi-bar plot that shows the total number of approved applications per each application demographic.
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
pd_df_loan_approved = pd_df_loan[pd_df_loan['Application_Status'] == 'Y']
groupby_columns = ['Dependents','Education', 'Gender', 'Income', 'Property_Area']
pd_g = pd_df_loan_approved.groupby(groupby_columns)['Application_ID'].count().to_frame().reset_index()
pd_g['Approved'] = pd_g['Application_ID']
pd_g['Demographics'] = pd_g['Dependents'] + "_" + pd_g['Education'] + "_" + pd_g['Gender'] + \
                       "_" + pd_g['Income'] + "_" + pd_g['Property_Area']
fig = px.bar(pd_g, y='Demographics',x='Approved')
fig.update_layout(
    autosize=False,
    width=1000,
    height=1500,)
#pd_df_appl_status['approved'] = 1
#fig = px.histogram(pd_df_appl_status, x='Income', y='approved',color = 'Gender', barmode='group')

layout = html.Div([
    dcc.Markdown(children=markdown_text),
    html.Label('Demographics'),
    dcc.Dropdown(['Dependents', 'Education', 'Gender', 'Income', 'Property Area'],
                    ['Dependents', 'Education', 'Gender', 'Income', 'Property Area'],
                    multi=True),
    dcc.Graph(
        id='req-5-2',
        figure=fig
    )
])