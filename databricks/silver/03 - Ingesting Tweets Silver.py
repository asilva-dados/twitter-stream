# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Ingestão camada silver zone
# MAGIC 
# MAGIC Este notebook tem como objetivo fazer o primeiro nivel de ingestão na camada **silver-zone**

# COMMAND ----------

# MAGIC %md
# MAGIC ### imports

# COMMAND ----------

from textblob import TextBlob
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from pyspark.sql.functions import col, udf

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Funções para limpeza e análise de sentimento dos textos dos Tweets coletados

# COMMAND ----------

def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # remove puntuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # remove number
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # remove hashtag
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    return tweet


# COMMAND ----------

# Get the subjectifvity
def getSubjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity


# Get the polarity
def getPolarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity

# Calc the sentiment
def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'    
    else:
        return 'Positive'


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Carregando dados da camanda Bronze para efetuar limpeza, gerar colunas com análise de sentimento e enriquecer a camanda Silver

# COMMAND ----------

df = spark.sql("select * from twitter_bronze.tweets_tokens_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Limpeza dos Tweets para gerar dados mais consumíveis na camanda Bronze 

# COMMAND ----------

clean_tweets = F.udf(cleanTweet, StringType())
df_clean_tweets = df.withColumn('processed_text', clean_tweets(col("text")))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Criando novas colunas com análise de sentimento para gerar mais valor aos dados

# COMMAND ----------

# Aplicando funções para gerar análise de sentimento
subjectivity = F.udf(getSubjectivity, FloatType())
polarity = F.udf(getPolarity, FloatType())
sentiment = F.udf(getSentiment, StringType())

subjectivity_tweets = df_clean_tweets.withColumn('subjetividade', subjectivity(col("processed_text")))
polarity_tweets = subjectivity_tweets.withColumn("polaridade", polarity(col("processed_text")))
sentiment_tweets = polarity_tweets.withColumn("sentimento", sentiment(col("polaridade")))
sentiment_tweets = sentiment_tweets.withColumn("load_at", F.current_timestamp())


# COMMAND ----------

sentiment_tweets.createOrReplaceGlobalTempView("updates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

bucket_path = '/mnt/bucket-dados-deltalake'
table_name = 'tweets_tokens_silver'
camada = 'silver'
database = 'twitter_silver'

# COMMAND ----------

# Cria a tabela se ela não existe.
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
                  processed_text string, 
                  subjetividade string, 
                  polaridade string, 
                  sentimento string,
                  load_at date) 
             USING DELTA 
             PARTITIONED BY (load_at) 
             LOCATION '" + {bucket_path}/{camada}/{database}/{table_name} + "'
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO twitter_silver.tweets_tokens_silver AS silver
# MAGIC USING global_temp.updates
# MAGIC ON silver.id = updates.id
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *
