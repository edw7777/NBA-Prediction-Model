import pandas as pd
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier

import ssl
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

load_dotenv()
mongodb_uri = os.getenv('MONGODB_URI')

uri = mongodb_uri
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
    
    # Extract the nested fields
    player_data['ws'] = advanced_stats.get('ws', 0)
    player_data['vorp'] = advanced_stats.get('vorp', 0)
    player_data['per'] = advanced_stats.get('per', 0)
    
    # Add other relevant stats if needed
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

df = df.dropna()

selected_features = ['GS', 'FGM', 'FGA', 'FTM', 'FTA', 'PTS', 'mp', 'ts_pct', 'obpm', 'bpm']

x = df[selected_features]

y = df['ws']

x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

model = RandomForestRegressor()
model.fit(x_train, y_train)

y_pred = model.predict(x_test)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f'Mean Squared Error: {mse}')
print(f'R-squared: {r2}')

#hyperparameter tuning
from sklearn.model_selection import GridSearchCV

param_grid = {
    'n_estimators': [100, 200, 300],
    'max_depth': [10, 20, None],
}

grid_search = GridSearchCV(model, param_grid, cv=5, scoring='r2')
grid_search.fit(x_train, y_train)

print("Best parameters:", grid_search.best_params_)

#reevaulation
best_model = RandomForestRegressor(**grid_search.best_params_)
best_model.fit(x_train, y_train)

final_predictions = best_model.predict(x_test)
final_mse = mean_squared_error(y_test, final_predictions)
final_r2 = r2_score(y_test, final_predictions)

print(f'Final Mean Squared Error: {final_mse}')
print(f'Final R-squared: {final_r2}')


