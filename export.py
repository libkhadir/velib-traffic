data = data.drop(['minute'], axis=1)

data['availability'] = (data['bikes'] + data['Ebikes']) / data['slots']

sn.set()


station_code = 10107
plt.figure(figsize=(14,9))
days=['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
for k in range(2, 9):
  currentDate = now - timedelta(days=k)
  value = str(currentDate)
  print('plotting date ', value)
  day=int(value.split('-')[2])
  print('selected day ', day)
  data_sub_station = data.loc[(data['code'] == station_code) & (data['day'] == day), ['hour', 'availability']]
  data_sub_station = data_sub_station.sort_values('hour')
  hours = numpy.asarray([[x, x+1] for x in range(23)]).flatten()
  avail = numpy.asarray([[data_sub_station.values[i, 1], data_sub_station.values[i, 1]] for i in range(23)]).flatten()

  plt.plot(hours, avail, label=days[currentDate.weekday()])
  plt.xlabel("Heures")
  plt.ylabel("Disponibilité (%)")
  plt.legend()
  plt.savefig('export.png')
  
print('process done')
