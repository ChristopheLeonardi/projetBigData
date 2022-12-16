#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[1]:


import json
f = open('usr/sources.json')
data_to_search = json.load(f)
  
f.close()


# In[ ]:





# In[3]:


## producer.py
from kafka import KafkaProducer
import time
import requests

p = KafkaProducer(bootstrap_servers=['kafka:9093'])

bol = True
while bol == True:
    for query in data_to_search:
        data = requests.get(query["url"]).content
        p.send(query["dataset_name"], data)
        p.flush()



# In[ ]:




