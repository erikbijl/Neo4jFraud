# Databricks notebook source
# MAGIC %md
# MAGIC # Load Data to Neo4j

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from neo4j import GraphDatabase

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Data from DBFS

# COMMAND ----------

path = "/dbfs/FileStore/tables"

# COMMAND ----------

def read_from_dbfs(name, path):
    file_path = path + "/" + name
    df = (
        spark
        .read
        .format('delta')
        .option("header", "true")
        .load(file_path)
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Nodes

# COMMAND ----------

credit_card_df = read_from_dbfs("nodes_credit_card", path)
person_df = read_from_dbfs("nodes_person", path)
address_df = read_from_dbfs("nodes_address", path)
city_df = read_from_dbfs("nodes_city", path)
merchant_df = read_from_dbfs("nodes_merchant", path)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Load relations

# COMMAND ----------

transaction_df = read_from_dbfs('relations_transaction', path)
has_card_df = read_from_dbfs('relations_has_card', path)
had_address_df = read_from_dbfs('relations_has_address', path)
located_in_df = read_from_dbfs('relations_located_in', path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Load to Neo4j

# COMMAND ----------

database = "neo4j"
username = dbutils.secrets.get(scope="kv_db", key="neo4jAuraDSusername")
password = dbutils.secrets.get(scope="kv_db", key="neo4jAuraDSpassword")
uri = dbutils.secrets.get(scope="kv_db", key="neo4jAuraDSuri")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Nodes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Credit card 

# COMMAND ----------

credit_card_df

# COMMAND ----------

credit_card_df.count()

# COMMAND ----------

(
    credit_card_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("labels", ":CreditCard")
    .option("node.keys", "number")
    .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Person

# COMMAND ----------

person_df

# COMMAND ----------

person_df.count()

# COMMAND ----------

(
    person_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("labels", ":Person")
    .option("node.keys", "id")
    .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Address

# COMMAND ----------

address_df

# COMMAND ----------

address_df.count()

# COMMAND ----------

(
    address_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("labels", ":Address")
    .option("node.keys", "street")
    .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### City

# COMMAND ----------

city_df

# COMMAND ----------

city_df.count()

# COMMAND ----------

(
    city_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("labels", ":City")
    .option("node.keys", "id")
    .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merchant

# COMMAND ----------

merchant_df

# COMMAND ----------

merchant_df.count()

# COMMAND ----------

(
    merchant_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("labels", ":Merchant")
    .option("node.keys", "name")
    .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Constraints

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

query = """
    CREATE CONSTRAINT credit_card_number IF NOT EXISTS
    FOR (c:CreditCard) REQUIRE c.number IS UNIQUE
"""
app.query(query)

# COMMAND ----------

query = """
    CREATE CONSTRAINT person_id IF NOT EXISTS
    FOR (p:Person) REQUIRE p.id IS UNIQUE
"""
app.query(query)

# COMMAND ----------

query = """
    CREATE CONSTRAINT city_id IF NOT EXISTS
    FOR (c:City) REQUIRE c.id IS UNIQUE
"""
app.query(query)

# COMMAND ----------

query = """
    CREATE CONSTRAINT address_street IF NOT EXISTS
    FOR (a:Address) REQUIRE a.street IS UNIQUE
"""
app.query(query)

# COMMAND ----------

query = """
    CREATE CONSTRAINT merchant_name IF NOT EXISTS
    FOR (m:Merchant) REQUIRE m.name IS UNIQUE
"""
app.query(query)

# COMMAND ----------

query = """
    SHOW CONSTRAINTS
"""
results = app.query(query)
app.print_records(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Relations

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSACTION

# COMMAND ----------

transaction_df = transaction_df.repartition(1)

# COMMAND ----------

transaction_df.rdd.getNumPartitions()

# COMMAND ----------

transaction_df

# COMMAND ----------

transaction_df.count()

# COMMAND ----------

(
    transaction_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("relationship.save.strategy", "keys")
    .option("relationship", "TRANSACTION")
    .option("relationship.properties", "category,amount,number,unix_time,is_fraud,merch_lat,merch_long,train")
    .option("relationship.source.labels", ":CreditCard")
    .option("relationship.source.save.mode", "Match")
    .option("relationship.source.node.keys", "source.number:number")
    .option("relationship.target.labels", ":Merchant")
    .option("relationship.target.save.mode", "Match")
    .option("relationship.target.node.keys", "target.name:name")
    .save()
)

# COMMAND ----------

query = """
    CREATE INDEX transaction_train IF NOT EXISTS
    FOR ()-[r:TRANSACTION]->() ON r.train
"""
app.query(query)

# COMMAND ----------

query = """
    CREATE INDEX transaction_is_fraud IF NOT EXISTS
    FOR ()-[r:TRANSACTION]->() ON r.is_fraud
"""
app.query(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### HAS_CARD

# COMMAND ----------

has_card_df

# COMMAND ----------

has_card_df.count()

# COMMAND ----------

(
    has_card_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("relationship", "HAS_CARD")
    .option("relationship.source.labels", ":Person")
    .option("relationship.source.save.mode", "match")
    .option("relationship.source.node.keys", "source.id:id")
    .option("relationship.target.labels", ":CreditCard")
    .option("relationship.target.save.mode", "match")
    .option("relationship.target.node.keys", "target.number:number")
    .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### HAS_ADDRESS

# COMMAND ----------

had_address_df

# COMMAND ----------

had_address_df.count()

# COMMAND ----------

(
    had_address_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("relationship", "HAS_ADDRESS")
    .option("relationship.source.labels", ":Person")
    .option("relationship.source.save.mode", "match")
    .option("relationship.source.node.keys", "source.id:id")
    .option("relationship.target.labels", ":Address")
    .option("relationship.target.save.mode", "match")
    .option("relationship.target.node.keys", "target.street:street")
    .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOCATED_IN

# COMMAND ----------

located_in_df

# COMMAND ----------

located_in_df.count()

# COMMAND ----------

(
    located_in_df
    .write
    .format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("url", uri)
    .option("authentication.type", "basic")
    .option("authentication.basic.username", username)
    .option("authentication.basic.password", password)
    .option("database", database)
    .option("relationship", "LOCATED_IN")
    .option("relationship.source.labels", ":Address")
    .option("relationship.source.save.mode", "match")
    .option("relationship.source.node.keys", "source.street:street")
    .option("relationship.target.labels", ":City")
    .option("relationship.target.save.mode", "match")
    .option("relationship.target.node.keys", "target.id:id")
    .save()
)
