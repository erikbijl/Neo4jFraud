# Databricks notebook source
# MAGIC %md
# MAGIC # Run Queries

# COMMAND ----------

! pip install altair

# COMMAND ----------

from pyspark.sql import functions as F
from neo4j import GraphDatabase
import altair as alt

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Python Driver

# COMMAND ----------

database = "neo4j"
username = dbutils.secrets.get(scope="kv_db", key="neo4jAuraDSusername")
password = dbutils.secrets.get(scope="kv_db", key="neo4jAuraDSpassword")
uri = dbutils.secrets.get(scope="kv_db", key="neo4jAuraDSuri")

# COMMAND ----------

class App:
    def __init__(self, uri, user, password, database=None):
        self.driver = GraphDatabase.driver(uri, auth=(user, password), database=database)
        self.database = database

    def close(self):
        self.driver.close()

    def query(self, query):
        return self.driver.execute_query(query)
        
    def count_nodes_in_db(self):
        query = "MATCH (n) RETURN COUNT(n)"
        result = self.query(query)
        (key, value) = result.records[0].items()[0]
        return value

    def remove_nodes_relationships(self):
        query = "MATCH (n) DETACH DELETE n"
        result = self.query(query)

    def print_records(self, result):
        for record in result.records:
            print(record.values())

# COMMAND ----------

app = App(uri, username, password, database)

# COMMAND ----------

# MAGIC %md
# MAGIC # Basic Queries

# COMMAND ----------

# MAGIC %md
# MAGIC Using Python Driver

# COMMAND ----------

query = """
    MATCH (m:Merchant) WITH m LIMIT 5 RETURN m.name as name, labels(m) as labels
"""
results = app.query(query)
app.print_records(results)

# COMMAND ----------

# MAGIC %md
# MAGIC Same query using Spark Connector

# COMMAND ----------

query = """
    MATCH (m:Merchant) WITH m LIMIT 5 RETURN m.name as name, labels(m) as labels
"""

df = (
    spark
    .read
    .format("org.neo4j.spark.DataSource")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("query", query)
    .load()
)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Set number of transactions on CreditCards and Merchants 

# COMMAND ----------

query = """
    MATCH (c:CreditCard)
    OPTIONAL MATCH (c)-[t:TRANSACTION]->(m:Merchant)
    WHERE t.train = 1
    WITH c, COUNT(t) AS count_transactions
    SET c.count_transactions = count_transactions
"""
app.query(query)

# COMMAND ----------

query = """
    MATCH (m:Merchant)
    OPTIONAL MATCH (c:CreditCard)-[t:TRANSACTION]->(m)
    WHERE t.train = 1
    WITH m, COUNT(t) AS count_transactions
    SET m.count_transactions = count_transactions
"""
app.query(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribution of number of transactions per Credit Card

# COMMAND ----------

query = """
    MATCH (c:CreditCard) RETURN c.number AS number, c.count_transactions AS count_transactions
"""

df = (
    spark
    .read
    .format("org.neo4j.spark.DataSource")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("query", query)
    .load()
)
df = df.toPandas()

# COMMAND ----------

alt.Chart(df).mark_bar().encode(
    alt.X("count_transactions:Q", bin=alt.Bin(extent=[0, 4000], step=50)),
    y='count()',
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribution of Number of transactions per Merchant

# COMMAND ----------

query = """
    MATCH (m:Merchant) RETURN m.name AS name, m.count_transactions AS count_transactions
"""

df = (
    spark
    .read
    .format("org.neo4j.spark.DataSource")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("query", query)
    .load()
)
df = df.toPandas()

# COMMAND ----------

alt.Chart(df).mark_bar().encode(
    alt.X("count_transactions:Q", bin=alt.Bin(extent=[0, 5000], step=100)),
    y='count()',
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Label Credit Cards and Merchants with Fraud

# COMMAND ----------

query = """
    MATCH (c:CreditCard)-[t:TRANSACTION]->(m:Merchant)
    WHERE t.is_fraud = 1 AND t.train = 1 
    RETURN COUNT(t) as count_fraud_in_train, COUNT(DISTINCT c) AS count_unique_fraud_credit_card, COUNT(DISTINCT m) AS count_unique_fraud_merchants
"""
result = app.query(query)
result.records[0].items()

# COMMAND ----------

query = """
    MATCH (c:CreditCard)-[t:TRANSACTION]->(m:Merchant)
    WHERE t.is_fraud = 1 AND t.train = 1 
    SET c:Fraud
    SET m:Fraud
"""
app.query(query)

# COMMAND ----------

query = """
    MATCH (c:CreditCard)
    OPTIONAL MATCH (c)-[t:TRANSACTION]->(m:Merchant)
    WHERE t.is_fraud = 1 AND t.train = 1
    WITH c, COUNT(t) AS count_fraud_transactions
    SET c.count_fraud_transactions = count_fraud_transactions
"""
app.query(query)

# COMMAND ----------

query = """
    MATCH (m:Merchant)
    OPTIONAL MATCH (c:CreditCard)-[t:TRANSACTION]->(m)
    WHERE t.is_fraud = 1 AND t.train = 1
    WITH m, COUNT(t) AS count_fraud_transactions
    SET m.count_fraud_transactions = count_fraud_transactions
"""
app.query(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributions of number of transactions for fraud and non-fraud Credit Cards

# COMMAND ----------

query = """
    MATCH (c:CreditCard) RETURN c.number AS number, c.count_transactions AS count_transactions, labels(c) AS labels
"""

df = (
    spark
    .read
    .format("org.neo4j.spark.DataSource")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("query", query)
    .load()
)

# COMMAND ----------

df = df.toPandas()
df['fraud'] = df['labels'].apply(lambda x: 'Fraud' if 'Fraud' in x else 'No Fraud')

# COMMAND ----------

df[['count_transactions', 'fraud']].groupby('fraud').quantile([0.0, 0.1, 0.25, .5, 0.75, .90, .90, 1.0])

# COMMAND ----------


alt.Chart(df).transform_density(
    'count_transactions',
    as_=['count_transactions', 'density'],
    groupby=['fraud'],
).mark_area().encode(
    x="count_transactions:Q",
    y='density:Q',
    color='fraud:N'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributions of number of transactions for fraud and non-fraud Merchants
# MAGIC

# COMMAND ----------

query = """
    MATCH (m:Merchant) RETURN m.name AS name, m.count_transactions AS count_transactions, labels(m) AS labels
"""

df = (
    spark
    .read
    .format("org.neo4j.spark.DataSource")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("query", query)
    .load()
)
df = df.toPandas()
df['fraud'] = df['labels'].apply(lambda x: 'Fraud' if 'Fraud' in x else 'No Fraud')

# COMMAND ----------

(
    df[['count_transactions', 'fraud']]
    .groupby('fraud')
    .quantile([0.0, 0.1, 0.25, .5, 0.75, .90, .90, 1.0])
)

# COMMAND ----------

alt.Chart(df).transform_density(
    'count_transactions',
    as_=['count_transactions', 'density'],
    groupby=['fraud'],
).mark_area().encode(
    x="count_transactions:Q",
    y='density:Q',
    color='fraud:N'
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Plot transaction amounts

# COMMAND ----------

query = """
    MATCH (c:CreditCard)-[r:TRANSACTION]-(m:Merchant) 
    WHERE r.train = 1 
    RETURN r.amount AS amount, r.is_fraud as fraud
"""

df = (
    spark
    .read
    .format("org.neo4j.spark.DataSource")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("query", query)
    .load()
)

# COMMAND ----------

df[['count_transactions', 'fraud']].groupby('fraud').quantile([0.0, 0.1, 0.25, .5, 0.75, .90, .90, 1.0])
