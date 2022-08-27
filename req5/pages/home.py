import dash
from dash import html, dcc

dash.register_page(__name__, path='/')

markdown_text = '''
## Functional Requirements

### Functional Requirements 5.1
Create a bar chart that shows the difference in application approvals for Married Men vs Married Women 
based on income ranges. (number of approvals)

### Functional Requirements 5.2
Create and plot a chart that shows the difference in application approvals based on Property Area.

### Functional Requirements 5.3
Create a multi-bar plot that shows the total number of approved applications per each application demographic. 
'''

layout = html.Div([
    dcc.Markdown(children=markdown_text)
])