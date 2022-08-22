import dash
from dash import html, dcc

dash.register_page(__name__, path='/')

markdown_text = '''
## Functional Requirements

### Functional Requirements 3.1
Find and plot transactions, showing which transaction type occurs more often

### Functional Requirements 3.2
Find and plot states, showing which state has the highest number of customers

### Functional Requirements 3.3
Find and plot the sum of all transactions for each customer, and which customer
has the highest transaction amount

### Functional Requirements 3.4
Find and plot the top three months with the largest transaction data

### Functional Requirements 3.5
Find and plot each branches healthcare transactions, showing which branch 
processed the highest total dollar value of health care transactions
'''

layout = html.Div([
    dcc.Markdown(children=markdown_text)
])