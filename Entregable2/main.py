import requests
from collections import namedtuple
import redshift_connector

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
hostname= 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
database= 'data-engineer-database'
username= 'guidotombacco_coderhouse'
pwd='Jd2iE008KW'
port_id= '5439'

r = requests.get("https://api.open-meteo.com/v1/forecast?latitude=-29.7125&longitude=-57.0877&hourly=temperature_2m,precipitation_probability,rain,visibility,windspeed_10m&timezone=America%2FSao_Paulo&forecast_days=1")


def customClimaDecoder(climaDict):
    return namedtuple('X', climaDict.keys())(*climaDict.values())

df = pd.read_json(r.text)

redshift_connector.paramstyle = 'numeric'
engine= create_engine("postgresql://postgres:david9.25.38@localhost:5432/Semana6_DE")
conn = redshift_connector.connect(
    host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    database='data-engineer-database',
    port=5439,
    user='guidotombacco_coderhouse',
    password='Jd2iE008KW'
)

conn.autocommit = True
cursor = conn.cursor()

df.to_sql()

for i in range(len(tiempo)):
    sql = 'insert into clima(fecha, temperatura, precipitacion, lluvia, visibilidad, velocidadViento) VALUES(:1, :2, :3, :4, :5, :6)'
    
    cursor.execute(sql, (tiempo[i], temperatura[i], precipitacion[i], lluvia[i], visibilidad[i], viento_velocidad[i]))

cursor.close()
conn.close()

