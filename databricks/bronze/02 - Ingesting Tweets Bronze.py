# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Ingestão camada bronze zone

# COMMAND ----------

# MAGIC %md
# MAGIC ### imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.ml.feature import RegexTokenizer
import re


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Lendo dados da camanda Raw para processamento e criação de dados na camada Bronze

# COMMAND ----------

bucket_path = '/mnt/bucket-dados-deltalake/raw'

df = spark.read.parquet(bucket_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Removendo apenas linhas com valor NULO para gerar dados mais consumíveis na camanda Bronze 

# COMMAND ----------

import numpy as np

df = df.dropna()
df = df.drop_duplicates(subset=['text'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Carregando Tweets após limpeza para uma Delta Table na camada Bronze

# COMMAND ----------

df.createOrReplaceGlobalTempView("updates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parametros

# COMMAND ----------

bucket_path = '/mnt/bucket-dados-deltalake'
table_name = 'tweets_tokens_bronze'
camada = 'bronze'
database = 'twitter_bronze'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Banco de dados
# MAGIC Criando a estruturado de banco de dados

# COMMAND ----------

# Cria a tabela se ela não existe
spark.sql("""
          CREATE TABLE IF NOT EXISTS " + database + "." + table_name + " 
                 (id long,
                  text string,
                  created_at string,
                  conversation_id string,
                  author_id string,                  
                  public_metrics_retweet_count long,
                  public_metrics_reply_count long,
                  public_metrics_like_count long,
                  public_metrics_quote_count long,
                  in_reply_to_user_id string,
                  load_at date) 
             USING DELTA 
             PARTITIONED BY (load_at) 
             LOCATION '" + {bucket_path}/{camada}/{database}/{table_name} + "'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO twitter_bronze.tweets_tokens_bronze AS bronze
# MAGIC USING global_temp.updates
# MAGIC ON bronze.id = updates.id
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *
