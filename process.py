import findspark
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext
import os
import numpy
import pandas as pd
import sqlite3
from pyspark.sql import functions

os.environ['SPARK_HOME'] = '/content/spark-3.3.1-bin-hadoop3'


findspark.init()

sc = SparkContext('local')
sql_c = SQLContext(sc)

print('spark initialisation done')

df = []

for i in range(0, 7):
  velib = sqlite3.connect('2019-03-1{}-data.db'.format(i))
  fetches = velib.execute("""select * from status
    left join statusConso on statusConso.id = status.idConso""").fetchall()
  if len(df) == 0:
    df = pd.DataFrame.from_records(fetches)
  else:
    df = pd.concat((df, pd.DataFrame.from_records(fetches)))
print('db import done')

_f_d = velib.execute("""select * from status
    left join statusConso on statusConso.id = status.idConso""")
names = list(map(lambda x: x[0], _f_d.description))
df.columns = names

df.to_csv('velib.csv')
del df

trips = sql_c.read.csv("velib.csv", header=True, sep=",")
print('db loaded to spark')
