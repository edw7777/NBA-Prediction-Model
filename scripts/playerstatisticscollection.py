"""
This script retrieves data for career stats and career awards up to a certain year for every single player on the roster
"""

import ssl
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from nba_api.stats.endpoints import commonteamroster, playercareerstats
from nba_api.stats.endpoints import playerawards
import pandas as pd
import time

TEAMS = ['Atlanta Hawks', 'Boston Celtics', 'Brooklyn Nets', 'Charlotte Hornets', 'Chicago Bulls', 'Cleveland Cavaliers', 
              'Dallas Mavericks', 'Denver Nuggets', 'Detroit Pistons', 'Golden State Warriors', 'Houston Rockets', 'Indiana Pacers', 
              'Los Angeles Clippers', 'Los Angeles Lakers', 'Memphis Grizzlies', 'Miami Heat', 'Milwaukee Bucks', 
              'Minnesota Timberwolves', 'New Orleans Pelicans', 'New York Knicks', 'Oklahoma City Thunder', 'Orlando Magic', 
              'Philadelphia 76ers', 'Phoenix Suns', 'Portland Trail Blazers', 'Sacramento Kings', 'San Antonio Spurs', 
              'Toronto Raptors', 'Utah Jazz', 'Washington Wizards']

YEARS = ["2014-15", "2015-16", "2016-17", "2017-18", "2018-19", "2019-20",
         "2020-21", "2021-22", "2022-23", "2023-24"]

TEAM_ID = {'Atlanta Hawks': 1610612737, 'Boston Celtics': 1610612738, 'Brooklyn Nets': 1610612751, 
           'Charlotte Hornets': 1610612766, 
               'Chicago Bulls': 1610612741, 'Cleveland Cavaliers': 1610612739, 'Dallas Mavericks': 1610612742, 
           'Denver Nuggets': 1610612743,
               'Detroit Pistons': 1610612765, 
               'Golden State Warriors': 1610612744, 'Houston Rockets': 1610612745, 'Indiana Pacers': 1610612754, 
           'Los Angeles Clippers': 1610612746, 
               'Los Angeles Lakers': 1610612747, 'Memphis Grizzlies': 1610612763, 'Miami Heat': 1610612748, 
           'Milwaukee Bucks': 1610612749, 
               'Minnesota Timberwolves': 1610612750, 'New Orleans Pelicans': 1610612740, 'New York Knicks': 1610612752, 
               'Oklahoma City Thunder': 1610612760, 'Orlando Magic': 1610612753, 'Philadelphia 76ers': 1610612755, 
           'Phoenix Suns': 1610612756, 
               'Portland Trail Blazers': 1610612757, 'Sacramento Kings': 1610612758, 'San Antonio Spurs': 1610612759, 
           'Toronto Raptors': 1610612761, 
               'Utah Jazz': 1610612762, 'Washington Wizards': 1610612764}


def get_team_roster(team_id, season):
    try:
        roster = commonteamroster.CommonTeamRoster(team_id=team_id, season=season).get_data_frames()[0]
        return roster
    except Exception as e:
        print(f"Error retrieving roster for team ID {team_id} in season {season}: {e}")
        return pd.DataFrame()
    
def get_player_career_stats(player_id):
    try:
        career_stats = playercareerstats.PlayerCareerStats(player_id=player_id).get_data_frames()[0]
        return career_stats
    except Exception as e:
        print(f"Error retrieving career stats for player ID {player_id}: {e}")
        return pd.DataFrame()
    
def get_player_awards(player_id, year):
    awards = playerawards.PlayerAwards(player_id=player_id).get_data_frames()[0]
    output = {"MVPS": 0, "FINALS_MVP": 0, "ALL-NBA": 0, "ALL-STAR": 0, "CHAMP": 0}
    for index, row in awards.iterrows():  
        #print(str(row["SEASON"][0:4]))
        if str(year) <= str(row["SEASON"][0:4]):
            continue
        if row["DESCRIPTION"] == "NBA Most Valuable Player":
            #print("MVP winner") 
            output["MVPS"] = output.get("MVPS")+1
        if row["DESCRIPTION"] == "All-NBA":
            #print("All-NBA")
            output["ALL-NBA"] = output.get("ALL-NBA")+1
        if row["DESCRIPTION"] == "NBA All-Star":
            #print("ALL STAR")
            output["ALL-STAR"] = output.get("ALL-STAR")+1
        if row["DESCRIPTION"] == "NBA Champion":
            #print("Champ")
            output["CHAMP"] = output.get("CHAMP")+1
        if row["DESCRIPTION"] == "NBA Finals Most Valuable Player":
            #print("FINALS MVP")
            output["FINALS_MVP"] = output.get("FINALS_MVP")+1
    return output
    
def aggregate_career_stats_up_to_year(player_career_stats, year, player_id):
    #compiles the career stats for a single player up to that year
    year_start = int(year.split('-')[0])
    
    # Extract the starting year part of the SEASON_ID
    player_career_stats['SEASON_YEAR'] = player_career_stats['SEASON_ID'].apply(lambda x: int(x.split('-')[0]))
    
    # Filter stats up to the specified year
    filtered_stats = player_career_stats[player_career_stats['SEASON_YEAR'] <= year_start]
    
    #removing counting duplicate stats
    filtered_stats = filtered_stats[filtered_stats['TEAM_ABBREVIATION'] != "TOT"]
    
    #aggregated_stats has type <class 'pandas.core.series.Series'>
    if filtered_stats.empty:
        aggregated_stats = pd.Series([0] * len(player_career_stats.columns), index=player_career_stats.columns)
    else:
        # Aggregate the stats
        aggregated_stats = filtered_stats.sum(numeric_only=True)
        #aggregated_stats.at['PLAYER_ID'] = player_id
        #aggregated_stats.at['TEAM_ID'] = team_id
        #aggregated_stats.at['SEASON_YEAR'] = year
        #aggregated_stats.at['PLAYER_AGE'] = player_age
        if aggregated_stats[7] == 0:
            FG_PCT = 0
        else:
            FG_PCT = aggregated_stats[6]/aggregated_stats[7]
        aggregated_stats.at['FG_PCT'] = FG_PCT
        if aggregated_stats[10] == 0:
            FG3_PCT = 0
        else:
            FG3_PCT = aggregated_stats[9]/aggregated_stats[10]
        aggregated_stats[11] = FG3_PCT
        if aggregated_stats[13] == 0:
            FT_PCT = 0
        else:
            FT_PCT = aggregated_stats[12]/aggregated_stats[13]
        aggregated_stats[14] = FT_PCT

        player_awards = get_player_awards(player_id, year_start)

        awards_series = pd.Series(player_awards)

        # Concatenate the awards Series to the aggregated stats Series
        aggregated_stats = pd.concat([aggregated_stats, awards_series])
        
        aggregated_stats = aggregated_stats.drop('PLAYER_ID')
        aggregated_stats = aggregated_stats.drop('TEAM_ID')
        aggregated_stats = aggregated_stats.drop('SEASON_YEAR')
        aggregated_stats = aggregated_stats.drop('PLAYER_AGE')

        print("final aggregated_stats")
        print(aggregated_stats)
    return aggregated_stats


def insert_player_career_stats(player_name, player_id, team_id, player_age, year, aggregated_stats, player_stats_collection):
    if aggregated_stats is None:
        player_stats_document = {
        "name": player_name,
        "player_id": player_id,
        "team_id": team_id,
        "player_age": player_age,
        "year": str(year),
        "career_stats": {"GP": 0, "GS": 0, "MIN": 0,
                        "FGM": 0, "FGA": 0, "FG_PCT": 0, "FG3M": 0, "FG3A": 0, "FG3_PCT": 0, "FTM": 0, "FTA": 0, "FT_PCT": 0,
                        "OREB": 0, "DREB": 0 , "REB": 0, "AST": 0, "STL": 0 , "BLK": 0, "TOV": 0, "PF": 0, "PTS": 0, "SEASON_YEAR": 0}
    }   
    else:
        player_stats_document = {
            "name": player_name,
            "player_id": player_id,
            "team_id": team_id,
            "player_age": player_age,
            "year": str(year),
            "career_stats": aggregated_stats.to_dict()
        }
        player_stats_collection.update_one(
        {"player_id": player_id, "year": str(year)},
            {"$set": player_stats_document},
            upsert=True
        )
    
def process_and_insert_player_data(player_name, player_id, team_id, player_age, year, player_stats_collection):
    player_career_stats = get_player_career_stats(player_id)
    aggregated_stats = aggregate_career_stats_up_to_year(player_career_stats, year, player_id)
    
    insert_player_career_stats(player_name, player_id, team_id, player_age, year, aggregated_stats, player_stats_collection)

def main():
  uri = "mongodb+srv://wangedw:Mymdb168@cluster0.kesjap8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
  client = MongoClient(uri, tlsAllowInvalidCertificates=True, server_api=ServerApi('1'))
  db = client.nba_data

  try:
      client.admin.command('ping')
      print("Pinged your deployment. You successfully connected to MongoDB!")
  except Exception as e:
      print(e)

  player_stats_collection = db['player_stats']

  for year in YEARS:
    for team in TEAMS:
        time.sleep(0.6)
        team_roster = get_team_roster(TEAM_ID[team], year) 
        print(team_roster)
        
        for _, player in team_roster.iterrows():
            time.sleep(1)
            process_and_insert_player_data(player["PLAYER"], player['PLAYER_ID'], player['TeamID'], player['AGE'], year, player_stats_collection)   
            break