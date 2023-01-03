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

df = pd.DataFrame(data=data_raw,
                    columns=['code', 'day', 'month', 'hour', 'minute', 'bikes', 'Ebikes', 'slots'])
df.to_csv('tmp.csv')
