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

os.environ['SPARK_HOME'] = os.environ['LOCAL_HOME'] + '/spark-3.3.1-bin-hadoop3'


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
names[13] = 'DROP'
df.columns = names
df = df.iloc[:, :15]
df = df.drop(['DROP'], axis=1)

df.to_csv('velib.csv')
del df

trips = sql_c.read.csv("velib.csv", header=True, sep=",")
print('db loaded to spark')

trips_day = trips.withColumn('day', functions.dayofmonth('date'))\
                .withColumn('month', functions.month('date'))\
                .withColumn('year', functions.year('date'))\
                .withColumn('hour', functions.hour('date'))\
                .withColumn('minute', functions.minute('date'))\
                .rdd

def map_trips(row):
  index = (int(row['code']),int(row['day']),int(row['month']),int(row['hour']))
  value = numpy.asarray([int(row['minute']),int(row['nbBike']),int(row['nbEBike']),int(row['nbEDock'])])
  return index, value

trips_per_station = trips_day.map(map_trips)

def group_per_station(value_x, value_y):
  if value_x[0] < value_y[0]:
    return value_x
  return value_y

trips_count_station = trips_per_station.reduceByKey(group_per_station)

bikes_per_station = numpy.asarray(trips_count_station.collect())
bikes_per_station.shape

data_raw = []
for station in bikes_per_station:
  data_raw.append(station.flatten())

data_raw = numpy.asarray(data_raw)
data_raw.shape

data = pd.DataFrame(data=data_raw,
                    columns=['code', 'day', 'month', 'hour', 'minute', 'bikes', 'Ebikes', 'slots'])
data = data.drop(['minute'], axis=1)

data['availability'] = (data['bikes'] + data['Ebikes']) / data['slots']

sn.set()

def filter_data(codesList, station_code):
  for d in codesList:
    if d == 10107:
      return d
  return -1

for d in list(filter_data, data['code']):
  station_code = d
  plt.figure(figsize=(14,9))
  days=['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
  
  for day in range(10,11):
    data_sub_station = data.loc[(data['code'] == station_code) & (data['day'] == day), ['hour', 'availability']]
    data_sub_station = data_sub_station.sort_values('hour')
    hours = numpy.asarray([[x, x+1] for x in range(23)]).flatten()
    avail = numpy.asarray([[data_sub_station.values[i, 1], data_sub_station.values[i, 1]] for i in range(23)]).flatten()

    plt.plot(hours, avail, label=days[day - 10])
  
  plt.savefig('exports/' + str(station_code) + '.png')

print('process done')
