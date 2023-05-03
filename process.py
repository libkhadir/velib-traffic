from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions
import matplotlib.pyplot as plt
from datetime import date, timedelta

now = date.today()

def get_spark_session(env, appName):
    return SparkSession. \
        builder. \
        master(env). \
        appName(appName). \
        config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.41.2.1'). \
        getOrCreate()

spark = get_spark_session('local', 'demo spark')

spark.sql('SELECT current_date').show()

sql_c = SQLContext(spark.sparkContext)
data_dict = {}
for k in range(2, 9):
  currentDate = now - timedelta(days=k)
  print('extracting {} with day={}'.format(currentDate, currentDate.day))
  i = currentDate.day
  data_dict[i] = sql_c.read.format('jdbc') \
        .options(driver='org.sqlite.JDBC', dbtable='status',
                 url=f'jdbc:sqlite:2023-04-{i}-data.db')\
        .load()
for k in range(2, 9):
  currentDate = now - timedelta(days=k)
  i = currentDate.day
  data_dict[i].createOrReplaceTempView(f"status_data_{i}")

query = "CREATE OR REPLACE TEMPORARY VIEW status_data as ("
for k in range(2, 9):
  currentDate = now - timedelta(days=k)
  i = currentDate.day
  query += f" SELECT * FROM status_data_{i}"
  if k < 8:
    query += " UNION "
query += ")"

sql_c.sql(query)

for k in range(2, 9):
  currentDate = now - timedelta(days=k)
  i = currentDate.day
  data_dict[i] = sql_c.read.format('jdbc') \
        .options(driver='org.sqlite.JDBC', dbtable='statusConso',
                 url=f'jdbc:sqlite:2023-04-{i}-data.db')\
        .option("customSchema", "date STRING")\
        .load()
for k in range(2, 9):
  currentDate = now - timedelta(days=k)
  i = currentDate.day
  data_dict[i].withColumn('day', functions.dayofmonth('date'))\
                .withColumn('month', functions.month('date'))\
                .withColumn('year', functions.year('date'))\
                .withColumn('hour', functions.hour('date'))\
                .withColumn('minute', functions.minute('date'))\
                .createOrReplaceTempView(f"status_conso_data_{i}")

query = "CREATE OR REPLACE TEMPORARY VIEW status_conso_data as ("
for k in range(2, 9):
  currentDate = now - timedelta(days=k)
  i = currentDate.day
  query += f" SELECT * FROM status_conso_data_{i}"
  if k < 8:
    query += " UNION "
query += ")"

sql_c.sql(query)


query = """CREATE OR REPLACE TEMPORARY VIEW final_status_data as (
  SELECT  status_conso_data.*  FROM status_data LEFT JOIN status_conso_data on status_conso_data.id = status_data.idConso
  where status_data.code = '10107'
)"""

sql_c.sql(query)

query = """select id, 
day, hour, minute,
nbBike as bikes,
nbEbike as ebikes,
nbEDock as slots,
((nbBike + nbEbike) / nbEDock) as availability
from final_status_data
where minute = '0'
order by day, hour asc
"""
df = sql_c.sql(query) \
    .toPandas()


plt.figure(figsize=(14,9))
days=['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
for k in range(2, 9):
  currentDate = now - timedelta(days=k)
  print(currentDate)
  plt.plot(df.loc[df['day'] == currentDate.day]['hour'].to_numpy(), df.loc[df['day'] == currentDate.day]['availability'].to_numpy(), label=days[currentDate.weekday()])

plt.xlabel("Heures")
plt.ylabel("Disponibilité (%)")
plt.legend()
plt.savefig('export.png')
