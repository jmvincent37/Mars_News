#!/usr/bin/env python
# coding: utf-8

# # Eat Safe, Love

# ## Notebook Set Up

# In[37]:


# Import dependencies
from pymongo import MongoClient
import pandas as pd
from pprint import pprint


# In[38]:


# Create an instance of MongoClient
mongo = MongoClient(port=27017)


# In[39]:


# assign the uk_food database to a variable name
db = mongo['uk_food']


# In[40]:


# review the collections in our database
db.list_collection_names()


# In[41]:


# assign the collection to a variable
establishments = db['establishments']
establishments.find_one()


# ## Part 3: Exploratory Analysis
# Unless otherwise stated, for each question: 
# * Use `count_documents` to display the number of documents contained in the result.
# * Display the first document in the results using `pprint`.
# * Convert the result to a Pandas DataFrame, print the number of rows in the DataFrame, and display the first 10 rows.

# ### 1. Which establishments have a hygiene score equal to 20?

# In[42]:


# Find the establishments with a hygiene score of 20
hygiene_20 = {"scores.Hygiene":20}

# Use count_documents to display the number of documents in the result
count = establishments.count_documents(hygiene_20)
print(count)

# Display the first document in the results using pprint
hygiene_results = establishments.find_one(hygiene_20)

pprint(hygiene_results)


# In[43]:


# Capture the results to a variable prior to converting to DataFrame
hygiene_results = establishments.find(hygiene_20)
for i in range(2):
    pprint(hygiene_results[i])


# In[44]:


# Convert the result to a Pandas DataFrame
hygiene_20_df = pd.DataFrame(hygiene_results)

# Display the number of rows in the DataFrame
print(len(hygiene_20_df))

# Display the first 10 rows of the DataFrame
hygiene_20_df.head(10)


# ### 2. Which establishments in London have a `RatingValue` greater than or equal to 4?

# In[45]:


# Find the establishments with London as the Local Authority and has a RatingValue greater than or equal to 4.
london_4 = {'LocalAuthorityName': {'$regex': "London"},'RatingValue': {'$gte': 4}}

# Use count_documents to display the number of documents in the result
row_count = establishments.count_documents(london_4)
print(row_count)

# Display the first document in the results using pprint
london_results = establishments.find_one(london_4)
pprint(london_results)


# In[46]:


# Capture the results to a variable prior to converting to DataFrame
london_results = establishments.find(london_4)
for i in range(2):
    pprint(london_results[i])
london_results


# In[47]:


# Convert the result to a Pandas DataFrame
london_4_df = pd.DataFrame(london_results)

# Display the number of rows in the DataFrame
print(len(london_4_df))

# Display the first 10 rows of the DataFrame
london_4_df.head(10)


# ### 3. What are the top 5 establishments with a `RatingValue` rating value of 5, sorted by lowest hygiene score, nearest to the new restaurant added, "Penang Flavours"?

# In[51]:


# Get Penang Flavours details
restaurant = establishments.find_one({'BusinessName': 'Penang Flavours'})
restaurant


# In[52]:


# Search within 0.01 degree on either side of the latitude and longitude.
# Rating value must equal 5


degree_search = 0.01
longitude = 0.08384
latitude = 51.490142


query3 = {'RatingValue': '5',
         'geocode.longitude': {'$lte': (longitude + degree_search), '$gte': (longitude - degree_search)},
         'geocode.latitude': {'$lte': (latitude + degree_search), '$gte': (latitude - degree_search)} 
                 }
sort = [('scores.Hygiene', 1)]

# Print the results

nearby_results = list(establishments.find(query3).sort(sort).limit(5))

pprint(nearby_results)


# In[53]:


#Capture the results to a variable prior to converting to DataFrame
nearby_results = establishments.find(query3).sort(sort).limit(5)
for i in range(2):
    pprint(nearby_results[i])


# In[34]:


# Convert result to Pandas DataFrame
nearby_df = pd.DataFrame(establishments.find(query3).sort(sort).limit(5))

nearby_df.head()


# ### 4. How many establishments in each Local Authority area have a hygiene score of 0?

# In[35]:


# Create a pipeline that: 
# 1. Matches establishments with a hygiene score of 0
score_matches = {'$match': {"scores.Hygiene": 0}}

# 2. Groups the matches by Local Authority
authority_matches = {'$group': {'_id': '$LocalAuthorityName', 'count': {'$sum': 1}}}

# 3. Sorts the matches from highest to lowest
sort = {'$sort': {'count': -1}}

pipeline = [score_matches, authority_matches, sort]

grouped_results = list(establishments.aggregate(pipeline))

# Print the number of documents in the result
print(len(grouped_results))

# Print the first 10 results
pprint(grouped_results[0:10])


# In[36]:


# Convert the result to a Pandas DataFrame
grouped_results_df = pd.DataFrame(grouped_results)

# Display the number of rows in the DataFrame
print(len(grouped_results_df))

# Display the first 10 rows of the DataFrame
grouped_results_df.head(10)


# In[ ]:





# In[ ]:




