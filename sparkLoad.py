# Sample pyspark job to load data into neo4j

import time
import pyspark
from functools import reduce
from graphdatascience import GraphDataScience
from pyspark.sql import SparkSession
from pandas import DataFrame
import pandas as pd

startmillisec = time.time_ns() // 1000000
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
MYFILE = "/home/ec2-user/data/nodes/campaignurl.dat"
df = spark.read.option("delimiter", "|").csv(MYFILE)
df.printSchema()
oldColumns = df.schema.names
newColumns = ['adv_entity_id','url','segmentid','counter','clicks','volume','category','label']
df = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), df)
df.printSchema()

df.show()

#df = df.repartition(20, "segmentid")

cypherQuery = "with event as row CREATE (u:URL {url:row.url, adv_entity_id:row.adv_entity_id, segment:row.segmentid, category:row.category, volume:row.volume,clicks:row.clicks,counter:row.counter });"

#cypherQuery = "with event as row CREATE (u:URL {url:row.url, adv_entity_id:row.adv_entity_id});"

df.write \
  .format("org.neo4j.spark.DataSource") \
  .mode("Overwrite") \
  .option("url", "bolt://13.58.250.90:7687") \
  .option("database","iasspark") \
  .option("batch.size", 20000) \
  .option("authentication.basic.username", "neo4j") \
  .option("authentication.basic.password", "Amish_2020_Tesla") \
  .option("query", cypherQuery) \
  .save()

endmillisec = time.time_ns() // 1000000
print(endmillisec - startmillisec)

