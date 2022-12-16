#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[1]:


import json
f = open('usr/sources.json')
data_to_search = json.load(f)
  
f.close()


# In[6]:


## consumer.py
from kafka import KafkaConsumer
import json

topics = [topic["dataset_name"] for topic in data_to_search]
    
kafkaConsumer = KafkaConsumer(bootstrap_servers=['kafka:9093']);
kafkaConsumer.subscribe(topics);

for msg in kafkaConsumer:
    print(dict(json.loads(msg.value)))



# In[ ]:


print(sortie_msg)

