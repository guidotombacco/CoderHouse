import requests
from collections import namedtuple

import pandas as pd
from sqlalchemy import create_engine
import json

#Ejecutar 'pip install sqlalchemy-redshift' para importar dependencias de redshift y que funcione el engine

r = requests.get("https://api.open-meteo.com/v1/forecast?latitude=-29.7125&longitude=-57.0877&hourly=temperature_2m,precipitation_probability,rain,visibility,windspeed_10m&timezone=America%2FSao_Paulo&forecast_days=1")


def customClimaDecoder(climaDict):
    return namedtuple('X', climaDict.keys())(*climaDict.values())

df = pd.DataFrame()
json_object = json.loads(r.text, object_hook=customClimaDecoder)

df["fecha"] = json_object.hourly.time
df["temperatura"] = json_object.hourly.temperature_2m
df["precipitacion"] = json_object.hourly.precipitation_probability
df["lluvia"] = json_object.hourly.rain
df["visibilidad"] = json_object.hourly.visibility
df["velocidadViento"] = json_object.hourly.windspeed_10m

connection_string = "redshift+psycopg2://guidotombacco_coderhouse:Jd2iE008KW@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database"

engine = create_engine(connection_string)
df.to_sql('clima', engine, if_exists='append', index=False)


