#!/usr/bin/env python
# coding: utf-8

# In[1]:


from configparser import ConfigParser
from pathlib import Path

import sqlalchemy as sa

import requests


# In[2]:


def read_api_credentials(config_file: Path, section: str) ->  dict:
    config = ConfigParser()
    config.read(config_file)
    api_credentials = dict(config[section])
    return api_credentials



# In[3]:


def read_config_file(file_path: str) ->  ConfigParser:
    config = ConfigParser()
    config.read(file_path)
    return config


# In[4]:


def build_conn_string(config: ConfigParser, section: str, dbengine: str) -> str:
    host = config.get(section, "host")
    port = config.get(section, "port")
    user = config.get(section, "db_user")
    password = config.get(section, "pwd")
    database = config.get(section, "db_name")
    
    conn_string = f"{dbengine}://{user}:{password}@{host}:{port}/{database}"
    
    return conn_string


# In[6]:


def connect_to_db(conn_string):
    """
    Crea una conexión a la base de datos.

    Parametros:
    conn_string: cadena de conexión a la base de datos

    Retorna:
    conn: objeto de conexión a la base de datos
    """
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn

