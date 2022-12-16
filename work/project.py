#!/usr/bin/env python
# coding: utf-8

# In[9]:





# In[1]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Projet Big Data").getOrCreate()


# In[4]:


## consumer.py
from kafka import KafkaConsumer
import json
import numpy as np

sortie_dict = []
topic="topic_SNCF"
consumer = KafkaConsumer(topic, bootstrap_servers=['kafka:9093'], group_id="my_group", auto_offset_reset="earliest")

for msg in consumer:
    sortie_dict = json.loads(msg.value.decode("utf-8"))


# In[ ]:


from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *

#sortie_dict
df = spark.read.format('json').options(header=True).load(sortie_dict)

