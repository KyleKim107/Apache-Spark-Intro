# Databricks notebook source
from pyspark import SparkContext as sc

# COMMAND ----------

text = ['a system of government by the whole population or all the eligible members of a state, typically through elected representatives.']
text_file = sc.parallelize(text) # RDD생성.

# COMMAND ----------

text_file.take(100)

# COMMAND ----------

words = text_file.flatMap(lambda line: line.split(" "))

# COMMAND ----------

words.take(100)

# COMMAND ----------

word_counts = words.map(lambda word: (word, 1))

# COMMAND ----------

word_counts.take(50)

# COMMAND ----------

# The reduceByKey() transformation aggregates the counts for each word by summing up their values (a + b), resulting in the total count for each unique word.
word_counts=word_counts.reduceByKey(lambda a,b: a+b)



# COMMAND ----------

word_counts.take(50)

# COMMAND ----------


