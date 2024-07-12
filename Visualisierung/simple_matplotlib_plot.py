import sys
import os

# Füge das übergeordnete Verzeichnis zum Python-Suchpfad hinzu, damit der Databse_class import endlich korrekt funktionieren
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.')))

from Database.database import Database
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

db = Database('../Database/teaching_factory.db')
data_579 = db.fetch_records('Drop_Vibration', 'bottle = 579')
values_579 = [x[1] for x in data_579]
linspace = np.linspace(0, 1, num=len(data_579))

plt.subplot(2, 1, 1)
plt.plot(linspace, values_579)
plt.title('Drop Vibration for Bottle 579, not cracked')


data_822 = db.fetch_records('Drop_Vibration', 'bottle = 822')
values_822 = [x[1] for x in data_822]
linspace = np.linspace(0, 1, num=len(data_822))
plt.subplot(2, 1, 2)
plt.plot(linspace, values_822)
plt.title('Drop Vibration for Bottle 822, cracked')

plt.subplots_adjust(hspace=0.5)

plt.show()