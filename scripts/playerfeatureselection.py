"""determining which player statistics are the most important in determining a good player"""
import pandas as pd
from sklearn.feature_selection import SelectKBest, f_regression

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
    
    career_stats = doc.get('career_stats', {})
    
    player_data.update({
    'GP': career_stats.get('GP', ''),
    'GS': career_stats.get('GS', ''),
    'MIN': career_stats.get('MIN', ''),
    'FGM': career_stats.get('FGM', ''),
    'FGA': career_stats.get('FGA', ''),
    'FG_PCT': career_stats.get('FG_PCT', ''),
    'FG3M': career_stats.get('FG3M', ''),
    'FG3A': career_stats.get('FG3A', ''),
    'FG3_PCT': career_stats.get('FG3_PCT', ''),
    'FTM': career_stats.get('FTM', ''),
    'FTA': career_stats.get('FTA', ''),
    'FT_PCT': career_stats.get('FT_PCT', ''),
    'OREB': career_stats.get('OREB', ''),
    'DREB': career_stats.get('DREB', ''),
    'REB': career_stats.get('REB', ''),
    'AST': career_stats.get('AST', ''),
    'STL': career_stats.get('STL', ''),
    'BLK': career_stats.get('BLK', ''),
    'TOV': career_stats.get('TOV', ''),
    'PF': career_stats.get('PF', ''),
    'PTS': career_stats.get('PTS', ''),
    #'MVP': career_stats.get('MVP', ''),
    #'FINALS_MVP': career_stats.get('FINALS_MVP', ''),
    #'ALL-NBA': career_stats.get('ALL-NBA', ''),
    #'ALL-STAR': career_stats.get('ALL-STAR', ''),
    #'CHAMP': career_stats.get('CHAMP', ''),
    })
    
    advanced_stats = doc.get('advanced_stats', {})
    
    player_data['ws'] = advanced_stats.get('ws', 0)
    player_data['vorp'] = advanced_stats.get('vorp', 0)
    player_data['per'] = advanced_stats.get('per', 0)
    
    player_data.update({
        'mp': advanced_stats.get('mp', 0),
        'ts_pct': advanced_stats.get('ts_pct', 0),
        'orb_pct': advanced_stats.get('orb_pct', 0),
        'drb_pct': advanced_stats.get('drb_pct', 0),
        'trb_pct': advanced_stats.get('trb_pct', 0),
        'ast_pct': advanced_stats.get('ast_pct', 0),
        'stl_pct': advanced_stats.get('stl_pct', 0),
        'blk_pct': advanced_stats.get('blk_pct', 0),
        'tov_pct': advanced_stats.get('tov_pct', 0),
        'usg_pct': advanced_stats.get('usg_pct', 0),
        'obpm': advanced_stats.get('obpm', 0),
        'dbpm': advanced_stats.get('dbpm', 0),
        'bpm': advanced_stats.get('bpm', 0),
    })

    data.append(player_data)

# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(data)

columns_to_convert = [
    'ws', 'vorp', 'per', 'mp', 'ts_pct', 'orb_pct', 'drb_pct', 'trb_pct',
    'ast_pct', 'stl_pct', 'blk_pct', 'tov_pct', 'usg_pct'
]

for column in columns_to_convert:
    df[column] = pd.to_numeric(df[column], errors='coerce')

# Feature selection for Win Shares (WS)
from sklearn.feature_selection import SelectKBest, f_regression

features = df.drop(columns=['name', 'team_id', 'year', 'ws', 'vorp', 'per'])
print(features)
targets = df[['ws', 'vorp', 'per']]

features = features.dropna()
targets = targets.loc[features.index]

# Select top k features for WS
selector = SelectKBest(score_func=f_regression, k=10)
selector.fit(features, targets['ws'])

selected_features = features.columns[selector.get_support()]
print("Selected features for WS:", selected_features)

# Repeat feature selection for VORP and PER if needed
selector_vorp = SelectKBest(score_func=f_regression, k=10)
selector_vorp.fit(features, targets['vorp'])
selected_features_vorp = features.columns[selector_vorp.get_support()]
print("Selected features for VORP:", selected_features_vorp)

selector_per = SelectKBest(score_func=f_regression, k=10)
selector_per.fit(features, targets['per'])
selected_features_per = features.columns[selector_per.get_support()]
print("Selected features for PER:", selected_features_per)