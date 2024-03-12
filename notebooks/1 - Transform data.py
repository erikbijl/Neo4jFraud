# Databricks notebook source
from neo4j import GraphDatabase
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

# MAGIC %md
# MAGIC Data comes from the [Credit Card Transactions Fraud Detection Dataset](https://www.kaggle.com/datasets/kartik2112/fraud-detection/data) Kaggle competition.

# COMMAND ----------

storage_account_name =  dbutils.secrets.get(scope="kv_db", key="saName")
storage_account_access_key =  dbutils.secrets.get(scope="kv_db", key="saKeyAccess")
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)
blob_container = "fraud"
file_location = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/" 

# COMMAND ----------

# MAGIC %md
# MAGIC Load training data

# COMMAND ----------

file_name = "fraudTrain.csv"
file_path = file_location + file_name
train_df = spark.read.format("csv").load(file_path, inferSchema = True, header = True)

# COMMAND ----------

file_name = "fraudTest.csv"
file_path = file_location + file_name
test_df = spark.read.format("csv").load(file_path, inferSchema = True, header = True)

# COMMAND ----------

train_df = train_df.withColumn('train', F.lit(1))
test_df = test_df.withColumn('train', F.lit(0))

df = (
    train_df
    .union(test_df)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore Data

# COMMAND ----------

df.count()

# COMMAND ----------

(
    df
    .groupBy(F.col('train'))
    .agg(
        F.count('*').alias('count'),
        F.min(F.col('trans_date_trans_time')).alias('min_date'),
        F.max(F.col('trans_date_trans_time')).alias('max_date')
    )
).display()

# COMMAND ----------

(
    df
    .groupBy(F.col('train'), F.col('is_fraud'))
    .agg(
        F.count('*').alias('count')
    )
    .withColumn('Percentage', F.round((F.col('count')/train_df.count()*100),2))
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Nodes and Relationships

# COMMAND ----------

df.columns

# COMMAND ----------

df = (
    df
    .withColumnRenamed('first', 'first_name')
    .withColumnRenamed('last', 'last_name')
    .withColumnRenamed('dob', 'birthdate')
    .withColumnRenamed('cc_num', 'number')
    .withColumn('merchant', F.regexp_replace(F.col('merchant'), 'fraud_', ''))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Nodes

# COMMAND ----------

credit_card_df = (
    df
    .select('number')
    .distinct()
)

# COMMAND ----------

person_df = (
    df
    .select('first_name', 'last_name', 'gender', 'job', 'birthdate')
    .groupBy('first_name', 'last_name', 'birthdate')
    .agg(
        F.first(F.col('gender')).alias('gender'),
        F.first(F.col('job')).alias('job')
    )
    .withColumn('id', F.monotonically_increasing_id())
)

# COMMAND ----------

address_df = (
    df
    .select('street', 'zip', 'lat', 'long')
    .withColumnRenamed('lat', 'latitude')
    .withColumnRenamed('long', 'longitude')
    .distinct()
)

# COMMAND ----------

city_df = (
    df
    .select('city', 'state', 'city_pop')
    .withColumnRenamed('city', 'name')
    .withColumnRenamed('city_pop', 'population')
    .distinct()
    .withColumn('id', F.monotonically_increasing_id())
)

# COMMAND ----------

merchant_df = (
    df
    .select('merchant')
    .withColumnRenamed('merchant', 'name')
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Check uniqueness on column for easier creation of relationships

# COMMAND ----------

assert credit_card_df.count() == credit_card_df.select('number').distinct().count()
assert person_df.count() == person_df.select('id').distinct().count()
assert address_df.count() == address_df.select('street').distinct().count()
assert city_df.count() == city_df.select('id').distinct().count()
assert merchant_df.count() == merchant_df.select('name').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC Relationships

# COMMAND ----------

transaction_df = (
    df
    .select('number', 'merchant', 'category', 'amt', 'trans_num', 'unix_time', 'is_fraud', 'merch_lat', 'merch_long', 'train')
    .withColumnRenamed('number', 'source.number')
    .withColumnRenamed('merchant', 'target.name') 
    .withColumnRenamed('amt', 'amount')
    .withColumnRenamed('trans_num', 'number')
    .withColumnRenamed('merch_latitude', 'latitude')
    .withColumnRenamed('merch_longitude', 'longitude')
    .distinct()
)

# COMMAND ----------

has_card_df = (
    df
    .select('number', 'first_name', 'last_name', 'birthdate')
    .join(person_df, on=['first_name', 'last_name', 'birthdate'], how='inner')
    .select('id', 'number')
    .withColumnRenamed('id', 'source.id')
    .withColumnRenamed('number', 'target.number')
    .distinct()
)

# COMMAND ----------

has_address_df = (
    df
    .select('street', 'first_name', 'last_name', 'birthdate')
    .join(person_df, on=['first_name', 'last_name', 'birthdate'], how='inner')
    .select('id', 'street')
    .withColumnRenamed('id', 'source.id')
    .withColumnRenamed('street', 'target.street')
    .distinct()
)

# COMMAND ----------

located_in_df = (
    df
    .select('street', 'city', 'state')
    .distinct()
    .join(city_df, on=((df.city==city_df.name)&(df.state==city_df.state)), how='inner')
    .select('street', 'id')
    .withColumnRenamed('street', 'source.street')
    .withColumnRenamed('id', 'target.id')
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store data in DBFS

# COMMAND ----------

path = "/dbfs/FileStore/tables"

# COMMAND ----------

def write_df_to_dbfs(df, name, path):
    file_path = path + "/" + name
    (
        df
        .write
        .format('delta')
        .mode('overwrite')
        .option("overwriteSchema", "true")
        .option("header", "true")
        .save(file_path)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Store nodes

# COMMAND ----------

write_df_to_dbfs(credit_card_df, "nodes_credit_card", path)
write_df_to_dbfs(person_df, "nodes_person", path)
write_df_to_dbfs(address_df, "nodes_address", path)
write_df_to_dbfs(city_df, "nodes_city", path)
write_df_to_dbfs(merchant_df, "nodes_merchant", path)

# COMMAND ----------

# MAGIC %md
# MAGIC Store relations

# COMMAND ----------

write_df_to_dbfs(transaction_df, "relations_transaction", path)
write_df_to_dbfs(has_card_df, "relations_has_card", path)
write_df_to_dbfs(has_address_df, "relations_has_address", path)
write_df_to_dbfs(located_in_df, "relations_located_in", path)
