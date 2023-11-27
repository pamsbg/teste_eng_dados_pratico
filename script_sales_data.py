# -*- coding: utf-8 -*-
"""
Created on Wed Nov 22 18:37:10 2023

@author: pamsb
"""
import time
import pandas as pd
#from sqlalchemy import create_engine
import pyspark as ps
#import psycopg2 
import pandas as pd 
from sqlalchemy import create_engine 
import logging


def check_unicidade(sales_data):

    try:
        #checar se todos os ids são unicos
        ids_unicos = sales_data['transaction_id'].nunique()
        if ids_unicos < sales_data.shape[0]:
            sales_data = sales_data.drop_duplicates(subset='transaction_id', keep='first')
        
           
        return sales_data
    except Exception as e:
            # registrar a exceção
            logging.exception(f'Ocorreu um erro: {e}')
            
def check_duplicidade(sales_data):

    try:
        #checar se tem linhas duplicadas
        duplicados = sales_data[sales_data.duplicated()]
        
        #remover duplicados
        if duplicados.shape[0] > 0:
            sales_data = sales_data.drop_duplicates(subset='transaction_id', keep='first')
       
              
        return sales_data
    except Exception as e:
        # registrar a exceção
        logging.exception(f'Ocorreu um erro: {e}')

def check_vazios(sales_data):

    try:
        #checar se tem linhas vazias
        linhas_vazias = sales_data["sale_value"].isna().sum()
        
    
        #remover linhas vazias
        if linhas_vazias > 0:
            sales_data = sales_data.dropna(how="any", axis=0)
            
            
        return sales_data
    except Exception as e:
        # registrar a exceção
        logging.exception(f'Ocorreu um erro: {e}')

def check_negativos(sales_data):

    try:    
        #Confirme se os valores de vendas não são negativos.
        positivos = sales_data["sale_value"].where(sales_data["sale_value"] >= 0).shape[0]
        total = sales_data.shape[0]
        negativos = total - positivos
        if negativos > 0:
            print("Há " + str(negativos) + " valores negativos. Observe as linhas abaixo")
            print(sales_data.where(sales_data["sale_value"] < 0))
        else:
            print("não há números negativos na coluna sale_value")
        
        return sales_data
    except Exception as e:
        # registrar a exceção
        logging.exception(f'Ocorreu um erro: {e}')

def check_data(sales_data):
    try:    
        #verificar se a coluna date é timestamp e converter se não
        é_timestamp = pd.core.dtypes.common.is_datetime_or_timedelta_dtype(sales_data)
        
        if é_timestamp == False:
            sales_data['date'] = pd.to_datetime(sales_data['date'])
           
        return sales_data
    except Exception as e:
        # registrar a exceção
        logging.exception(f'Ocorreu um erro: {e}')

def limpeza_dados(sales_data):
    try:
    
        # checar formato dos dados    
        sales_data.info()
        
        #checar unicidade
        sales_data_unico = check_unicidade(sales_data)
        
        #checar duplicidade
        sales_data_unico = check_duplicidade(sales_data_unico)
        
        #checar vazios 
        sales_data_vazios = check_vazios(sales_data_unico)
        
        #checar negativos
        sales_data_negativos = check_negativos(sales_data_vazios)
        
        #checar data
        sales_data_limpo = check_data(sales_data_negativos)
    
        return sales_data_limpo
    except Exception as e:
        # registrar a exceção
        logging.exception(f'Ocorreu um erro: {e}')

def conversao (df, coluna, taxa_conversao, currency):
    try:
        df_convertido = df.copy()
        df_convertido[coluna] = df[coluna] * taxa_conversao
        df_convertido['currency'] = currency
        return df_convertido
    except Exception as e:
        # registrar a exceção
        logging.exception(f'Ocorreu um erro: {e}')




def criar_tabela(data):
    try:
        contador = 0
        # criar conexão com db
        conn_string = 'postgres://postgres:pass@127.0.0.1/Weather_Database'
        
        db = create_engine(conn_string) 
        conn = db.connect() 
        conn1 = psycopg2.connect( 
        	database="Sales_Database", 
        user='postgres', 
        password='pass', 
        host='127.0.0.1', 
        port= '5432'
        ) 
        
        conn1.autocommit = False
        cursor = conn1.cursor() 
        
        # deletar tabela se ela já existir
        cursor.execute('drop table if exists sales_info') 
        
        #criar tabela
        sql = '''CREATE TABLE sales_info(id int , 
        transaction_id int , date datetime,  product_id int, seller_id int,
        sale_value float, currency char(3));'''
        
        cursor.execute(sql) 
        
        # converter dataframe para sql 
        data.to_sql('sales_info', conn, if_exists= 'append') 
        
        # Quantidade de linhas ingeridas no banco de dados de sua escolha é igual a quantidade de linhas originais
        sql1='''select * from sales_info;'''
        cursor.execute(sql1) 
        for i in cursor.fetchall(): 
            contador = contador + 1
            print(i) 
            
        if contador == data.shape[0]:
            print("quantidade de linhas ingeridas de forma correta")
            conn1.commit() 
            conn1.close() 
        else:
            print("quantidade de linhas ingeridas de forma incorreta. A operação será revertida")
            conn1.rollback()
            conn1.close()
    except Exception as e:
        # registrar a exceção
        logging.exception(f'Ocorreu um erro: {e}')

        

        
    
def handler():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        start_time = time.time()
        sales_data = pd.read_csv("sales_data.csv", sep=',')
        sales_data = pd.DataFrame(sales_data)
        sales_data.head()
        
        sales_data_limpo = limpeza_dados(sales_data)
        sales_data_convertido = conversao(sales_data_limpo, "sale_value", 0.75, "USD")
        
        #criar_tabela(sales_data_convertido)
        
        end_time = time.time()
        
        # tempo de execução do script
        elapsed_time = end_time - start_time
        print('Execution time:', elapsed_time, 'seconds')
        logging.info(f'Execution time {elapsed_time} seconds')
    except Exception as e:
        # registrar a exceção
        logging.exception(f'Ocorreu um erro: {e}')

    
