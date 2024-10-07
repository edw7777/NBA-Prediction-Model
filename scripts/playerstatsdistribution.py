from sklearn.mixture import GaussianMixture
import numpy as np
import pandas as pd
import ssl
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "uri"
client = MongoClient(uri, tlsAllowInvalidCertificates=True, server_api=ServerApi('1'))
db = client.nba_data

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

collection = db['player_stats']
documents = collection.find()

data = []

for doc in documents:
    if 'advanced_stats' not in doc:
        continue
    
    player_data = {
        'name': doc.get('name', ''),
        'team_id': doc.get('team_id', ''),
        'year': doc.get('year', ''),
    }

    advanced_stats = doc.get('advanced_stats', {})

    player_data['ws'] = advanced_stats.get('ws', 0)
    player_data['vorp'] = advanced_stats.get('vorp', 0)
    player_data['per'] = advanced_stats.get('per', 0)

    data.append(player_data)

df = pd.DataFrame(data)

print(df)

gmm = GaussianMixture(n_components=3, random_state=42)
gmm.fit(df)

clusters = gmm.predict(df)

probs = gmm.predict_proba(df)

print("Cluster assignments:", clusters)
print("Cluster probabilities:\n", probs)