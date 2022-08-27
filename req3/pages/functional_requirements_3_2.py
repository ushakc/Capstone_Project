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
### Functional Requirements 3.2
Find and plot states, showing which state has the highest number of customers
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

spark = SparkSession.builder.appName('req3_data_visual').getOrCreate()
df = spark.read\
     .format("jdbc")\
     .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone")\
     .option("dbtable", "creditcard_capstone.cdw_sapp_customer")\
     .option("user", user)\
     .option("password", password)\
     .load()
pd_df = df.toPandas()
fig = px.histogram(pd_df, y='CUST_STATE')

layout = html.Div([
    dcc.Markdown(children=markdown_text),
    dcc.Graph(
        id='req-3-2',
        figure=fig
    )
])

fig.update_layout(
margin=dict(l=30, r=30, t=20, b=30),
paper_bgcolor="LightSteelBlue",
)
