# Databricks notebook source
#Sentiment Analysis on Demonetization in India using Apache Spark SQL

# COMMAND ----------

demonetization_tweets=spark.read.format("csv").option("header", "true").load("/FileStore/tables/demonetization_tweets.csv")

# COMMAND ----------

demonetization_tweets.show()
demonetization_tweets.registerTempTable("tweets")

# COMMAND ----------

#Calculating retweet count at each time stamp
retweet_sum=spark.sql("select sum(retweetCount) as RetweetCount,created from tweets group by created")
retweet_sum.show()
display(retweet_sum)

# COMMAND ----------

#Getting types of Devices used for Tweet
device_count = spark.sql('select sum(retweetCount), substring_index(substring_index(statusSource, ">", -2),"<",1) as status_source from tweets group by substring_index(substring_index(statusSource, ">", -2),"<",1)')
display(device_count)

# COMMAND ----------

# Getting Reaction of People on Demonetization
reaction=spark.sql('''select CASE WHEN text like '%Respect%' THEN "POSITIVE"
                   WHEN text like '%symptom%' THEN "POSITIVE"
 WHEN text like '%terrorists%' THEN "POSITIVE"\
 WHEN text like '%National%' THEN "POSITIVE"
 WHEN text like '%reform%' THEN"POSITIVE"
 WHEN text like '%support%' THEN "POSITIVE"
 WHEN text like '%#CorruptionFreeIndia%' THEN "POSITIVE"
 WHEN text like '%respect%' THEN "POSITIVE"
 WHEN text like '%Gandhi%' THEN "POSITIVE"
 WHEN text like '%vote%' THEN "POSITIVE"
 WHEN text like '%fishy%' THEN "NEGATIVE"
 WHEN text like '%disclosure%' THEN "NEGATIVE"
WHEN text like '%Reddy Wedding%' THEN "NEGATIVE"
WHEN text like '%protesting%' THEN "NEGATIVE"
WHEN text like '%hards%' THEN "NEGATIVE"
WHEN text like '%Kerala%' THEN "NEGATIVE"
WHEN text like '%hurt%' THEN "NEGATIVE"
WHEN text like '%USELESS%' THEN "NEGATIVE"
WHEN text like '%Disaster!%' THEN "NEGATIVE"
WHEN text like '%Black%' THEN "NEGATIVE"
WHEN text like '%negative%' THEN "NEGATIVE"
WHEN text like '%impact%' THEN "NEGATIVE"
WHEN text like '%opposing%' THEN "NEGATIVE"
ELSE "NEUTRAL"
     END AS Reaction
from tweets''')

reaction.show()

reaction.registerTempTable("reaction")

# COMMAND ----------

reaction_count=spark.sql("select reaction, count(reaction) from reaction group by reaction")
display(reaction_count)
