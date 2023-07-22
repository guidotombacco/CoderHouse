import requests
from collections import namedtuple
import json
from json import JSONEncoder
import redshift_connector

r = requests.get("https://api.open-meteo.com/v1/forecast?latitude=-29.7125&longitude=-57.0877&hourly=temperature_2m,precipitation_probability,rain,visibility,windspeed_10m&timezone=America%2FSao_Paulo&forecast_days=1")


def customClimaDecoder(climaDict):
    return namedtuple('X', climaDict.keys())(*climaDict.values())


json_object = json.loads(r.text, object_hook=customClimaDecoder)

tiempo = json_object.hourly.time
temperatura = json_object.hourly.temperature_2m
precipitacion = json_object.hourly.precipitation_probability
lluvia = json_object.hourly.rain
visibilidad = json_object.hourly.visibility
viento_velocidad = json_object.hourly.windspeed_10m

redshift_connector.paramstyle = 'numeric'

conn = redshift_connector.connect(
    host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    database='data-engineer-database',
    port=5439,
    user='guidotombacco_coderhouse',
    password='Jd2iE008KW'
)

conn.autocommit = True
cursor = conn.cursor()

for i in range(len(tiempo)):
    sql = 'insert into clima(fecha, temperatura, precipitacion, lluvia, visibilidad, velocidadViento) VALUES(:1, :2, :3, :4, :5, :6)'
    
    cursor.execute(sql, (tiempo[i], temperatura[i], precipitacion[i], lluvia[i], visibilidad[i], viento_velocidad[i]))

cursor.close()
conn.close()

