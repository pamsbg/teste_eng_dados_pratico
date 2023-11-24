# -*- coding: utf-8 -*-
"""
Created on Wed Nov 22 18:43:00 2023

@author: pamsb
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType
from pyspark.sql.functions import sum, col, desc, count, mean as _mean, weekofyear, count, countDistinct



#criar um objeto spark 
spark = SparkSession.builder.getOrCreate()

#definir schema
schema = StructType([StructField("user_id", IntegerType(), True),
StructField("page_url", StringType(), True),StructField("session_duration", IntegerType(), True),
StructField("date", DateType(), True)])

#ler csv
df = spark.read.csv("website_logs.csv", header = True, schema=schema)

df.show(10)

#agrupar as páginas do site e contar quantidade de visualizações
df_views = df.groupBy("page_url").count().sort("count").show()

#calcular a média da duração da sessão dos usuários
df_media = df.select(
    _mean(col('session_duration')).alias('mean')).collect()

mean = df_media[0]['mean']
print(" a média da sessão dos usuários é " + str(mean))


# agrupar os dados por semana e usuário
df_week_user = df.groupBy(weekofyear("date").alias("week"), "user_id")

# contar o número de páginas visitadas por cada usuário em cada semana
df_page_count = df_week_user.agg(count("page_url").alias("page_count"))

# filtrar os usuários que visitaram mais de uma página em cada semana
df_returned = df_page_count.filter(df_page_count["page_count"] > 1)

# contar o número de usuários que retornaram ao site por semana
df_returned_count = df_returned.groupBy("week").agg(countDistinct("user_id").alias("returned_count"))


df_returned_count.show()

# somar o valor de todas as semanas
df_total = df_returned_count.agg(sum("returned_count").alias("total"))


df_total.show()