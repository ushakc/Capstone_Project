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
### Functional Requirements 5.1
Create a bar chart that shows the difference in application approvals for Married Men vs Married Women 
based on income ranges. (number of approvals)
'''

user = os.getenv("user", default=None)
password = os.getenv("password", default=None)

spark = SparkSession.builder.appName('req5_data_visual').getOrCreate()
df = spark.read\
     .format("jdbc")\
     .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone")\
     .option("dbtable", "creditcard_capstone.cdw_sapp_loan_application")\
     .option("user", user)\
     .option("password", password)\
     .load()
pd_df_loan = df.toPandas()
pd_df_appl_status = pd_df_loan[pd_df_loan['Application_Status']=='Y']
pd_df_appl_status = pd_df_appl_status[pd_df_appl_status['Married']=='Yes']
pd_df_appl_status['approved'] = 1
fig = px.histogram(pd_df_appl_status, x='Income', y='approved',color = 'Gender', barmode='group')

layout = html.Div([
    dcc.Markdown(children=markdown_text),
    dcc.Graph(
        id='req-5-1',
        figure=fig
    )
])