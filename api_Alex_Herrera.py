#!/usr/bin/env python
# coding: utf-8

# # API

# ## LIBRERIAS

# In[117]:


import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

import requests

from sqlalchemy import create_engine


from utils import read_api_credentials
from utils import read_config_file
from utils import build_conn_string
from utils import connect_to_db


# ## DESARROLLO

# ### Conexión a Redshift

# In[118]:


config_file = read_config_file("pipeline.conf")
conn_string = build_conn_string(config_file, "redshift", "postgresql")

conn_string = str(conn_string)


# In[119]:


conn_string


# In[120]:


engine = create_engine(conn_string)
connection = engine.connect()


# In[121]:


conn_string


# In[122]:


conn = connect_to_db(conn_string)


# In[123]:


connect_to_db(conn_string)


# ### Extracción de datos

# ##### Devuelve la posición de los vehículos monitoreados actualizada cada 30 segundos. Si no se pasan parámetros de entrada, retorna la posición actual de todos los vehículos monitoreados.

# In[124]:


url = 'https://apitransporte.buenosaires.gob.ar'


# In[125]:


client = read_api_credentials('pipeline.conf','api_cba')


# In[126]:


endpoint = 'colectivos/vehiclePositions?client_id='+client['api_user']+'&client_secret='+client['api_key']+'&json=1'


# In[127]:


full_url = f"{url}/{endpoint}"


# In[128]:


r = requests.get(full_url)
r.json()


# In[129]:


data = r.json()


# In[130]:


entity_list = data['_entity']

# Convertir la lista a un DataFrame de pandas
df = pd.json_normalize(entity_list)


# ### DATAFRAME

# In[131]:


df.columns


# In[132]:


tipos_de_datos = df.dtypes

diccionario_tipos_de_datos = tipos_de_datos.to_dict()

diccionario_tipos_de_datos


# In[133]:


df


# In[134]:


df = pd.DataFrame(df)


# In[135]:


df.columns


# In[136]:


table = pa.Table.from_pandas(df)

pq.write_table(table, 'table.parquet')


# alternativa a utilizar table = df.to_parquet('table.parquet', index=False)


# In[137]:


#lectura del archivo parquet
pd.read_parquet('table.parquet')


# ### CREACION DE TABLA EN REDSHIFT

# In[138]:


df.columns


# In[139]:


df.rename(columns={'_vehicle._position._latitude': 'vehicle_position_latitude', '_vehicle._position._longitude': 'vehicle_position_longitude', '_vehicle._vehicle._id': '_vehicle_vehicle_id'}, inplace=True)


# In[140]:


# Teniendo en cuenta un análisis por identificar mi primary key, defino a 'vehicle_vehicle_label' con el mismo se logra identificar al vehiculo con un id asociado unico, de manera que se puede realizar un seguimiento de sus diferentes estados cada con respecto al tiempo (cada 30 seg)

# Por ahora solo defnire 3 columnas con el fin de lograr cumplir los requisitos del entregable 1 y proximamente dependiendo del contexto de los entregables se agregaran columnas.

conn.execute("""
    drop table if exists alexherrera78_coderhouse.colectivo;
    CREATE TABLE IF NOT EXISTS alexherrera78_coderhouse.colectivo(
        vehicle_position_latitude DECIMAL, 
        vehicle_position_longitude DECIMAL,
        vehicle_vehicle_id INT PRIMARY KEY
        )
 """)


