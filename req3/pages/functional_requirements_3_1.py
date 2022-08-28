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
### Functional Requirements 3.1
Find and plot transactions, showing which transaction type occurs more often
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
pd_df = df.toPandas()
fig = px.histogram(pd_df, x='TRANSACTION_TYPE')

layout = html.Div([
    dcc.Markdown(children=markdown_text),
    dcc.Graph(
        id='req-3-1',
        figure=fig
    )
])