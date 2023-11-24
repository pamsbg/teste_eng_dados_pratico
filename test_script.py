# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 20:18:47 2023

@author: pamsb
"""

    

import pandas as pd

#cenário: base de dados com coluna com erro de digitação, números negativos em sale_value e valor que deveria ser data com um "-" a mais

from script_sales_data import limpeza_dados
        

   
    
# criar um dataframe com alguns dados sujos
sales_data = pd.DataFrame({
    "transactionn_id": [1, 2, 3, 4, 4, 5, 6, 7, 8, 9],
    "date": ["2023-01--01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-04", "2023-01-05", "2023-01-06", "2023-01-07", "2023-01-08", "2023-01-09"],
    "product_id": [101, 102, 103, 104, 104, 105, 106, 107, 108, 109],
    "seller_id": [11, 12, 13, 14, 14, 15, 16, 17, 18, 19],
    "sale_value": [100, -200, 300, 400, 400, 500, 600, 700, 800, 900],
    "currency": ["USD", "BRL", "FICT", "USD", "USD", "USD", "USD", "USD", "USD", "USD"]
    
    
})

# criar um dataframe esperado com os dados limpos
sales_data_clean = pd.DataFrame({
    "transaction_id": [1, 2, 3, 4, 4, 5, 6, 7, 8, 9],
    "date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-04", "2023-01-05", "2023-01-06", "2023-01-07", "2023-01-08", "2023-01-09"],
    "product_id": [101, 102, 103, 104, 104, 105, 106, 107, 108, 109],
    "seller_id": [11, 12, 13, 14, 14, 15, 16, 17, 18, 19],
    "sale_value": [100, 200, 300, 400, 400, 500, 600, 700, 800, 900],
    "currency": ["USD", "BRL", "FICT", "USD", "USD", "USD", "USD", "USD", "USD", "USD"]
    
})

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
    