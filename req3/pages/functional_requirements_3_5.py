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
### Functional Requirements 3.5
Find and plot each branches healthcare transactions, showing which branch 
processed the highest total dollar value of health care transactions (First 20)
'''

user = os.getenv("user", default=None)
password = os.getenv("password", default=None)

spark = SparkSession.builder.appName('req3_data_visual').getOrCreate()
df = spark.read\
     .format("jdbc")\
     .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone")\
     .option("dbtable", "creditcard_capstone.cdw_sapp_credit_card")\
     .option("user", user)\
     .option("password", password)\
     .load()
pd_df_cc= df.toPandas()
pd_df_cc_healthcare = pd_df_cc[pd_df_cc['TRANSACTION_TYPE'] == 'Healthcare']
pd_df_br_hc_tx = pd_df_cc_healthcare.groupby(by='BRANCH_CODE')['TRANSACTION_VALUE'].sum().sort_values(ascending=False).head(20).reset_index()
fig = px.bar(pd_df_br_hc_tx, x='BRANCH_CODE', y='TRANSACTION_VALUE')
fig.update_xaxes(type='category')
layout = html.Div([
    dcc.Markdown(children=markdown_text),
    dcc.Graph(
        id='req-3-3',
        figure=fig
    )
])