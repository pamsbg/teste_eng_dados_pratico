# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 20:18:47 2023

@author: pamsb
"""

    

import pandas as pd
import pytest 
import logging 


#cenário: base de dados com coluna com erro de digitação, números negativos em sale_value e valor que deveria ser data com um "-" a mais

from script_sales_data import check_unicidade, check_duplicidade, check_vazios, check_negativos, check_data, limpeza_dados, conversao
        


# criar um dataframe esperado com os dados limpos
sales_data = pd.DataFrame({
    "transaction_id": [1, 2, 3, 4, 4, 5, 6, 7, 8, 9],
    "date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-04", "2023-01-05", "2023-01-06", "2023-01-07", "2023-01-08", "2023-01-09"],
    "product_id": [101, 102, 103, 104, 104, 105, 106, 107, 108, 109],
    "seller_id": [11, 12, 13, 14, 14, 15, 16, 17, 18, 19],
    "sale_value": [100, 200, 300, 400, 400, 500, 600, 700, 800, 900],
    "currency": ["USD", "BRL", "FICT", "USD", "USD", "USD", "USD", "USD", "USD", "USD"]
    
})


taxa_conversao = 0.2 
currency = "USD"

def test_check_unicidade(): 
    # verifique se a função remove IDs de transação duplicados
    sales_data_unico = check_unicidade(sales_data) 
    assert sales_data_unico["transaction_id"].nunique() == sales_data_unico.shape[0] 
    # check that the function returns a dataframe 
    assert isinstance(sales_data_unico, pd.DataFrame) 

    
    
    
def test_check_duplicidade(): 
    # verifique se a função remove linhas duplicadas
    sales_data_duplicado = check_duplicidade(sales_data) 
    assert sales_data_duplicado.shape[0] == sales_data.drop_duplicates().shape[0] 
    # verifique se a função retorna um dataframe
    assert isinstance(sales_data_duplicado, pd.DataFrame) 
 
    
    
def test_check_vazios(): 
    # crie um dataframe com valores ausentes
    sales_data_vazio = sales_data.copy() 
    sales_data_vazio.loc[0, "sale_value"] = None 
    # verifique se a função remove linhas com valores ausentes
    sales_data_sem_vazio = check_vazios(sales_data_vazio) 
    assert sales_data_sem_vazio.shape[0] == sales_data.shape[0] - 1 
    # verifique se a função retorna um dataframe
    assert isinstance(sales_data_sem_vazio, pd.DataFrame) 
  

def test_check_negativos(): 
    # crie um dataframe com valores negativos
    sales_data_negativo = sales_data.copy() 
    sales_data_negativo.loc[0, "sale_value"] = -100 
  
    # verifique se a função retorna um dataframe
    assert isinstance(sales_data_negativo, pd.DataFrame) 
  


def test_check_data(): 
    # verifique se a função converte a coluna de data em data e hora
    sales_data_data = check_data(sales_data) 
    assert pd.core.dtypes.common.is_datetime_or_timedelta_dtype(sales_data_data["date"]) 
    #  verifique se a função retorna um dataframe
    assert isinstance(sales_data_data, pd.DataFrame) 
  
    
    
    
def test_limpeza_dados(): 
    # verifique se a função aplica todas as etapas de limpeza
    sales_data_limpo = limpeza_dados(sales_data) 
    assert sales_data_limpo.shape[0] == sales_data.drop_duplicates().shape[0] - 1 
    assert sales_data_limpo["transaction_id"].nunique() == sales_data_limpo.shape[0] 
    assert sales_data_limpo["sale_value"].isna().sum() == 0 
    assert sales_data_limpo["sale_value"].min() >= 0 
    assert pd.core.dtypes.common.is_datetime_or_timedelta_dtype(sales_data_limpo["date"]) 
    # verifique se a função retorna um dataframe
    assert isinstance(sales_data_limpo, pd.DataFrame) 
     
    
    
def test_conversao(): 
    # verifique se a função converte a coluna sale_value para outra moeda
    sales_data_convertido = conversao(sales_data, "sale_value", taxa_conversao, currency) 
    assert sales_data_convertido["sale_value"].equals(sales_data["sale_value"] * taxa_conversao) 
    assert sales_data_convertido["currency"].unique() == [currency] 
    #verifique se a função retorna um dataframe
    assert isinstance(sales_data_convertido, pd.DataFrame) 
   
    
    
# criar uma função de teste para verificar se a função limpeza_dados retorna o dataframe esperado
def test_limpeza_dados():
    # aplicar a função limpeza_dados no dataframe de exemplo
    sales_data_cleaned = limpeza_dados(sales_data)
    
    # usar o método assert_frame_equal do pandas para comparar os dois dataframes
    pd.testing.assert_frame_equal(sales_data_cleaned, sales_data_clean)
    
def test_limpeza_dados_caminho_feliz():
    # aplicar a função limpeza_dados no dataframe de exemplo
    sales_data_cleaned = limpeza_dados(sales_data)
    
    # usar o método assert_frame_equal do pandas para comparar os dois dataframes
    pd.testing.assert_frame_equal(sales_data_clean, sales_data_clean)
    