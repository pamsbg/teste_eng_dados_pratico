# -*- coding: utf-8 -*-
"""
Created on Wed Nov 22 08:10:01 2023

@author: pamsb
"""



import requests
import json
import pandas as pd
import time
import json
import psycopg2 
from sqlalchemy import create_engine 
   
  
def chamar_api(api_query):
    resposta = requests.get(api_query)
    resultados = json.loads(resposta.text)
    return resultados

    
def gerar_coordenadas(city, state, country):
    api_key ="f1afc2f58ef497b608768be2ac3fd373"
    api_query="http://api.openweathermap.org/geo/1.0/direct?q=" +city +","+ state +"," + country + "&limit=1&appid=" +api_key
    resultados = chamar_api(api_query)
    print(resultados)
    lat = 0
    lon = 0
    for i in resultados:
        print(i)
        lat = i['lat']
        lon = i['lon']
    return lat, lon


def consultar_tempo(city, state, country):
    api_key ="42fca0bce7ab3caca0df1defc3bd1970"
    conjunto_dados=[]
    lat, lon = gerar_coordenadas(city, state, country)
    lat = str(lat)
    lon = str(lon)
    api_query= "https://api.openweathermap.org/data/3.0/onecall?lat=" + lat + "&lon=" + lon +"&appid="+ api_key
    #api_query= "https://api.openweathermap.org/data/3.0/onecall?lat=-23.5324859&lon=-46.7916801&appid=42fca0bce7ab3caca0df1defc3bd1970"
    resultados = chamar_api(api_query)
    print(resultados)
       
    return resultados

def criar_tabela(data):
   
    # establish connections 
    conn_string = 'postgres://postgres:pass@127.0.0.1/Weather_Database'
    
    db = create_engine(conn_string) 
    conn = db.connect() 
    conn1 = psycopg2.connect( 
    	database="Weather_Database", 
    user='postgres', 
    password='pass', 
    host='127.0.0.1', 
    port= '5432'
    ) 
    
    conn1.autocommit = True
    cursor = conn1.cursor() 
    
    # drop table if it already exists 
    cursor.execute('drop table if exists weather_info') 
    
    sql = '''CREATE TABLE weather_return_api(id int , 
    lat float , lon float,  timezone char(200), timezone_offset char(20),
    current varchar(255), minutely varchar(255), hourly varchar(255), 
    daily varchar(255), alerts varchar(255));'''
    
    cursor.execute(sql) 
    
    # converting data to sql 
    data.to_sql('weather_info', conn, if_exists= 'append') 
    
    # fetching all rows 
    sql1='''select * from weather_info;'''
    cursor.execute(sql1) 
    for i in cursor.fetchall(): 
    	print(i) 
    
    conn1.commit() 
    conn1.close() 

def formatar_json(data):
    data_new = json.dumps(data)
    data_new = json.loads(data_new)  
    
    keys = []
    vals = []
    
    for k,v in data_new.items():
        keys.append(k)
        vals.append(v)
    
    data_new = pd.DataFrame(zip(keys,vals)).T
    new_header = data_new.iloc[0]
    data_new = data_new[1:]
    data_new.columns = new_header
    print(data_new.columns)
    print(data_new.head(10))
    return data_new

def handler():                
    city = "Osasco"
    state = "São Paulo"
    country = "Brazil"
    
    data = consultar_tempo(city, state, country)  
    print(data)
    weather_dataframe = formatar_json(data)
    criar_tabela(weather_dataframe)