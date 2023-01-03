import findspark
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext
import os
import numpy
import pandas as pd
import sqlite3
from pyspark.sql import functions
import matplotlib.pyplot as plt
import seaborn as sn
from datetime import date, timedelta

now = date.today()

for i in range(2, 9):
  currentDate = now - timedelta(days=i)
  value = str(currentDate)
  dbName = value + '-data.db'
  print('loading db : ', dbName)
  velib = sqlite3.connect(dbName)


df = pd.read_csv('prepare.csv')

_f_d = velib.execute("""select * from status
    left join statusConso on statusConso.id = status.idConso limit 1""")
names = list(map(lambda x: x[0], _f_d.description))
names[13] = 'DROP'
df.columns = names
df = df.iloc[:, :15]
df = df.drop(['DROP'], axis=1)

df.to_csv('velib.csv', index=False)
