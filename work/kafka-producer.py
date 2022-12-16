#!/usr/bin/env python
# coding: utf-8

# In[1]:


#get_ipython().system('pip install kafka-python')


# In[ ]:


## producer.py
from kafka import KafkaProducer
import json
import numpy as np
import time
import requests

p = KafkaProducer(bootstrap_servers=['kafka:9093'])
topic = "topic_SNCF"

url = "https://ressources.data.sncf.com/api/records/1.0/search/?dataset="
dataset_name = "ponctualite-mensuelle-transilien"
param = {
    "rows" : "-1",
    "sort" : "date",
    "facet": ["date", "service", "ligne"],
}
def createQuery(url, dataset_name, param):
    temp = ""
    for item in param["facet"]:
        temp += "&facet=" + item
    query = url + dataset_name + "&q=" + "&rows=" + param["rows"] + "&sort=" + param["sort"] + temp
    return query

while True:
    #time.sleep(10)
    messageAPI = requests.get(createQuery(url, dataset_name, param))
    print(messageAPI.content)
    result = p.send(topic, messageAPI.content)


# In[ ]:




