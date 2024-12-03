import math
import time
import warnings
import pandas as pd

def get_avg_win_pct_last_n_games(team, game_date, df, n):
    prev_game_df = df[(df['Date'] < game_date) & ((df['Home'] == team) | (df['Away'] == team))].sort_values(by='Date').tail(n)

    wins = 0 

    n_games_played = len(prev_game_df)
    if n_games_played == 0:
        return 0.5
    
    result_df = prev_game_df[["Date", "Home", "Away","Result"]]
    h_df = result_df.loc[result_df['Home'] == team] 
    
    h_wins = h_df.loc[h_df['Result'] == 1]
    
    wins += len(h_wins)
      
    a_df = result_df.loc[result_df['Home'] != team]
    a_wins = a_df.loc[a_df['Result'] == 0]
    
    wins += len(a_wins)

    return wins/n


def win_probs(home_elo, away_elo, home_court_advantage) :
    h = math.pow(10, home_elo/400)
    r = math.pow(10, away_elo/400)
    a = math.pow(10, home_court_advantage/400) 

    denom = r + a*h
    home_prob = a*h / denom
    away_prob = r / denom 
  
    return home_prob, away_prob

  #odds the home team will win based on elo ratings and home court advantage

def home_odds_on(home_elo, away_elo, home_court_advantage) :
    h = math.pow(10, home_elo/400)
    r = math.pow(10, away_elo/400)
    a = math.pow(10, home_court_advantage/400)
    return a*h/r

#this function determines the constant used in the elo rating, based on margin of victory and difference in elo ratings
def elo_k(MOV, elo_diff):
    k = 20
    if MOV>0:
        multiplier=(MOV+3)**(0.8)/(7.5+0.006*(elo_diff))
    else:
        multiplier=(-MOV+3)**(0.8)/(7.5+0.006*(-elo_diff))
    return k*multiplier


#updates the home and away teams elo ratings after a game 

def update_elo(home_score, away_score, home_elo, away_elo, home_court_advantage) :
    home_prob, away_prob = win_probs(home_elo, away_elo, home_court_advantage) 

    if (home_score - away_score > 0) :
        home_win = 1 
        away_win = 0 
    else :
        home_win = 0 
        away_win = 1 
  
    k = elo_k(home_score - away_score, home_elo - away_elo)

    updated_home_elo = home_elo + k * (home_win - home_prob) 
    updated_away_elo = away_elo + k * (away_win - away_prob)
    
    return updated_home_elo, updated_away_elo


#takes into account prev season elo
def get_prev_elo(team, date, season, team_stats, elo_df, game_date) :
    prev_game = team_stats[(team_stats['Date'] < game_date) & ((team_stats['Home'] == team) | (team_stats['Away'] == team))].sort_values(by='Date').tail(1).iloc[0]

    if team == prev_game['Home'] :
        elo_rating = elo_df[elo_df['Game_ID'] == prev_game['Game_ID']]['H_Team_Elo_After'].values[0]
    else :
        elo_rating = elo_df[elo_df['Game_ID'] == prev_game['Game_ID']]['A_Team_Elo_After'].values[0]
  
    if prev_game['Season'] != season :
        return (0.75 * elo_rating) + (0.25 * 1505)
    else :
        return elo_rating
    
def main():
    df_2021 = pd.read_csv("C:/Users/wange/NBA-Prediction-Model/data/nba_df_2021.csv")
    df_2021['Date'] = pd.to_datetime(df_2021['Date'])
    df_2021['Season'] = '2021-22'

    #df_2022 = pd.read_csv('C:/Users/wange/NBA-Prediction-Model/data/nba_df_2022.csv')
    #df_2022['Date'] = pd.to_datetime(df_2022['Date'])
    #df_2022['Season'] = '2022-23'

    #df_2023 = pd.read_csv('C:/Users/wange/NBA-Prediction-Model/data/nba_df_2023.csv')
    #df_2023['Date'] = pd.to_datetime(df_2023['Date'])
    #df_2023['Season'] = '2023-24'

    frames = [df_2021] #df_2022, df_2023]
    df = pd.concat(frames)
    print(df.columns)

    for season in df['Season'].unique() :
        season_stats = df[df['Season'] == season].sort_values(by='Date').reset_index(drop=True)
        for index, row in df.iterrows() : 
            game_id = row['Game_ID']
            game_date = row['Date']
            h_team = row['Home']
            a_team = row['Away']
            
            df.loc[index,'Home_W_Pct_10'] = get_avg_win_pct_last_n_games(h_team, game_date, df, 10)
            
            df.loc[index,'Away_W_Pct_10'] = get_avg_win_pct_last_n_games(a_team, game_date, df, 10)

    df.sort_values(by = 'Date', inplace = True)
    df.reset_index(inplace=True, drop = True)
    elo_df = pd.DataFrame(columns=['Game_ID', 'H_Team', 'A_Team', 'H_Team_Elo_Before', 'A_Team_Elo_Before', 'H_Team_Elo_After', 'A_Team_Elo_After'])
    teams_elo_df = pd.DataFrame(columns=['Game_ID','Team', 'Elo', 'Date', 'Where_Played', 'Season']) 

    for index, row in df.iterrows(): 
        game_id = row['Game_ID']
        game_date = row['Date']
        season = row['Season']
        h_team, a_team = row['Home'], row['Away']
        h_score, a_score = row['H_Score'], row['A_Score'] 

        if (h_team not in elo_df['H_Team'].values and h_team not in elo_df['A_Team'].values) :
            h_team_elo_before = 1500
        else :
            h_team_elo_before = get_prev_elo(h_team, game_date, season, df, elo_df, game_date)

        if (a_team not in elo_df['H_Team'].values and a_team not in elo_df['A_Team'].values) :
            a_team_elo_before = 1500
        else :
            a_team_elo_before = get_prev_elo(a_team, game_date, season, df, elo_df, game_date)

        h_team_elo_after, a_team_elo_after = update_elo(h_score, a_score, h_team_elo_before, a_team_elo_before, 69)

        new_row = {'Game_ID': game_id, 'H_Team': h_team, 'A_Team': a_team, 'H_Team_Elo_Before': h_team_elo_before, 'A_Team_Elo_Before': a_team_elo_before, \
                                                                            'H_Team_Elo_After' : h_team_elo_after, 'A_Team_Elo_After': a_team_elo_after}
        teams_row_one = {'Game_ID': game_id,'Team': h_team, 'Elo': h_team_elo_before, 'Date': game_date, 'Where_Played': 'Home', 'Season': season}
        teams_row_two = {'Game_ID': game_id,'Team': a_team, 'Elo': a_team_elo_before, 'Date': game_date, 'Where_Played': 'Away', 'Season': season}
    
        elo_df = elo_df._append(new_row, ignore_index = True)
        teams_elo_df = teams_elo_df._append(teams_row_one, ignore_index=True)
        teams_elo_df = teams_elo_df._append(teams_row_two, ignore_index=True)

    dates = list(set([d.strftime("%m-%d-%Y") for d in teams_elo_df["Date"]]))
    dates = sorted(dates, key=lambda x: time.strptime(x, '%m-%d-%Y'))
    teams = df["Away"]
    dataset = pd.DataFrame(columns=dates)
    dataset["Team"] = teams.drop_duplicates()
    dataset = dataset.set_index("Team")

    for index, row in teams_elo_df.iterrows():
        date = row["Date"].strftime("%m-%d-%Y")
        team = row["Team"]
        elo = row["Elo"]
        dataset[date][team] = elo

    teams_elo_df['Elo'] = teams_elo_df['Elo'].astype(float)

    print(elo_df)

    #merge the team stats with the elo ratings
    df = df.merge(elo_df.drop(columns=['H_Team', 'A_Team']), on ='Game_ID')

    df.to_csv("C:/Users/wange/NBA-Prediction-Model/data/nba_df_final.csv")

if __name__ == "__main__":
    main()
                    