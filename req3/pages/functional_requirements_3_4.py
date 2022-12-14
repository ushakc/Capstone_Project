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
### Functional Requirements 3.4
Find and plot the top three months with the largest transaction data
'''

# get the database username and password from secret.txt
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
pd_df_cc = df.toPandas()
pd_df_cc['MONTHYEAR'] = pd_df_cc['TIMEID'].str[:-2]
pd_top3 = pd_df_cc.groupby(['MONTHYEAR'])['TRANSACTION_VALUE'].sum().sort_values(ascending=False).head(10).to_frame().reset_index()

fig = px.scatter(pd_top3, x='MONTHYEAR', y='TRANSACTION_VALUE')
#fig = px.bar(pd_top3, x='MONTHYEAR', y='TRANSACTION_VALUE')
fig.update_xaxes(type='category')
#fig.update_yaxes(dtick=25000)


layout = html.Div([
    dcc.Markdown(children=markdown_text),
    dcc.Graph(
        id='req-3-4',
        figure=fig
    )
])