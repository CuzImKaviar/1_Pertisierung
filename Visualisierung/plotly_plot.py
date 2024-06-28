import sys
import os

# Füge das übergeordnete Verzeichnis zum Python-Suchpfad hinzu, damit der Databse_class import endlich korrekt funktionieren
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.')))

from Database.database import Database
import numpy as np
import pandas as pd
import plotly.graph_objs as go
from plotly.subplots import make_subplots

# Initialize the database
db = Database('../Database/teaching_factory.db')

# Fetch and prepare data for bottle 579
data_579 = db.fetch_records('Drop_Vibration', 'bottle = 579')
values_579 = [x[1] for x in data_579]
linspace_579 = np.linspace(0, 1, num=len(data_579))

# Fetch and prepare data for bottle 822
data_822 = db.fetch_records('Drop_Vibration', 'bottle = 822')
values_822 = [x[1] for x in data_822]
linspace_822 = np.linspace(0, 1, num=len(data_822))

fig = make_subplots(rows=2, cols=1, subplot_titles=('Drop Vibration for Bottle 579, not cracked', 'Drop Vibration for Bottle 822, cracked'))

# Add the first plot
fig.add_trace(
    go.Scatter(x=linspace_579, y=values_579, mode='lines', name='Bottle 579'),
    row=1, col=1
)

# Add the second plot
fig.add_trace(
    go.Scatter(x=linspace_822, y=values_822, mode='lines', name='Bottle 822'),
    row=2, col=1
)

fig.update_layout(height=600, width=800, title_text="Drop Vibration Analysis")

fig.show()