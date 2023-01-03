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

counter=0

now = date.today()

for i in range(2, 9):
  currentDate = now - timedelta(days=i)
  value = str(currentDate)
  dbName = value + '-data.db'
  print('loading db : ', dbName)
  velib = sqlite3.connect(dbName)
  fetches = velib.execute("""select * from status
    left join statusConso on statusConso.id = status.idConso""").fetchall()
  if counter == 0:
    df = pd.DataFrame.from_records(fetches)
    df.to_csv('prepare.csv', index=False)
  else:
    df = pd.DataFrame.from_records(fetches)
    df.to_csv('prepare.csv', mode='a', index=False, header=False)
  counter+=1
print('db import done')
