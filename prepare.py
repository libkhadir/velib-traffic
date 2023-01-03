
df.read_csv('prepare.csv')

_f_d = velib.execute("""select * from status
    left join statusConso on statusConso.id = status.idConso""")
names = list(map(lambda x: x[0], _f_d.description))
names[13] = 'DROP'
df.columns = names
df = df.iloc[:, :15]
df = df.drop(['DROP'], axis=1)

df.to_csv('velib.csv')
