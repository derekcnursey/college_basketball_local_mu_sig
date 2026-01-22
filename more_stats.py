from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import datetime
from input_data import *
import csv
import random
from statistics import mean, stdev
import numpy as np
import math
import sys

host="localhost"
user="root"
password="jake3241"
database="sports"






#team_name = 'Kansas'

conn = mysql.connector.connect(
  host=host,
  user=user,
  password=password,
  database=database
)
mycursor = conn.cursor()

def fetch_max_date(table: str):
    try:
        mycursor.execute(f"SELECT MAX(Date) FROM {table}")
        row = mycursor.fetchone()
    except Exception:
        return None
    if not row or row[0] is None:
        return None
    value = row[0]
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    try:
        return datetime.datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError:
        return None


def get_latest_stats_date():
    offensive_date = fetch_max_date("offensive_averages")
    defensive_date = fetch_max_date("defensive_averages")
    if offensive_date and defensive_date:
        return max(offensive_date, defensive_date)
    return offensive_date or defensive_date


def parse_start_date(value):
    if value is None:
        return None
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, int):
        return datetime.date(value, 1, 20)
    value_str = str(value).strip()
    if value_str.isdigit() and len(value_str) == 4:
        return datetime.date(int(value_str), 1, 20)
    try:
        return datetime.datetime.strptime(value_str, "%Y-%m-%d").date()
    except ValueError:
        return None


def determine_start_date(value=None):
    parsed = parse_start_date(value)
    if parsed:
        return parsed
    latest = get_latest_stats_date()
    if latest:
        return latest + datetime.timedelta(days=1)
    today = datetime.datetime.now().date()
    return datetime.date(today.year, 1, 20)


def get_ppp(pts, possessions):
    if possessions == 0:
        return 0
    else:
        return float(pts)/possessions

def get_efficency(pts, possessions):
    if possessions == 0:
        return 0
    else:
        return float(pts) * float(100)/possessions

def get_to_pct(to, possessions):
    if possessions == 0:
        return 0
    else:
        return float(to) / possessions

def get_eff_fg_pct(made_2pts, made_3pts, tot_2pts, tot_3pts):
    if tot_2pts+tot_3pts== 0:
        return 0
    return float(made_2pts+made_3pts + 0.5*made_3pts)/(tot_2pts+tot_3pts)

def get_true_shooting_pct(pts, tot_2pts, tot_3pts, tot_fts):
    if 2*(tot_2pts+tot_3pts+0.44*tot_fts) ==0:
        return 0
    return float(pts/(2*(tot_2pts+tot_3pts+0.44*tot_fts)))

def get_ft_rate(tot_2pts, tot_3pts, tot_fts):
    if (tot_2pts+tot_3pts) == 0:
        return 0
    return float(tot_fts/(tot_2pts+tot_3pts))

def get_ft_pct(tot_fts, made_fts):
    if tot_fts == 0:
        return float(0)
    else:
        return float(made_fts/tot_fts)

def get_3pt_rate(tot_2pts, tot_3pts):
    if tot_2pts+tot_3pts== 0:
        return 0
    return float(tot_3pts/(tot_2pts+tot_3pts))

def get_3pt_pct(made_3pts, tot_3pts):
    if tot_3pts== 0:
        return 0
    return float(made_3pts/tot_3pts)

def get_2pt_pct(made_2pts, tot_2pts):
    if tot_2pts== 0:
        return 0
    return float(made_2pts/tot_2pts)

def get_steal_pct(steals, tempo):
    if tempo == 0:
        return 0
    return float(steals/tempo)

def get_block_pct(blocks, opp_tot_2pt):
    if opp_tot_2pt == 0:
        return 0
    return float(blocks/opp_tot_2pt)

def get_rebound_pct(off_reb, def_reb):
    if (off_reb+def_reb) == 0:
        return 0
    return off_reb/(off_reb+def_reb)

def get_averages(df):
    temp_dict = {}
    for stat in df.columns:
        if stat == 'date' or stat == 'opp_date':
            continue
        avg_stat = mean(df[stat])
        temp_dict[stat + '_avg'] = avg_stat
    return temp_dict

def get_std_dev(df):
    temp_dict = {}
    for stat in df.columns:
        if stat == 'date' or stat == 'opp_date':
            continue
        std_stat = stdev(df[stat])
        temp_dict[stat + '_std'] = std_stat
    return temp_dict

def get_x_game_averages(df, num_games):
    temp_dict = {}
    df = df.tail(num_games)
    for stat in df.columns:
        if stat == 'date' or stat == 'opp_date':
            continue
        avg_stat = mean(df[stat])
        temp_dict[stat +'_'+ str(num_games)+ '_games_avg'] = avg_stat
    return temp_dict

def get_x_game_stdev(df, num_games):
    temp_dict = {}
    df = df.tail(num_games)
    for stat in df.columns:
        if stat == 'date' or stat == 'opp_date':
            continue
        std_stat = stdev(df[stat])
        temp_dict[stat + '_std'] = std_stat
    return temp_dict

def get_avg_decreased_games(df, games):
    temp_dict = {}
    tot_games = df.shape[0]
    j=1.0
    weights = []
    for i in range(games,tot_games):
        j-=float(0.05)
        if j<0:
            j=0
        weights.append(j)
    weights += [1] * games
    weights = weights[0:tot_games]
    for stat in df.columns:
        if stat == 'date' or stat == 'opp_date':
            continue
        df_temp = df[stat]
        temp_dict[stat + '_avg'] = np.average(df_temp, weights=weights)

    return temp_dict

def get_stdev_decreased_games(df, games):
        temp_dict = {}
        tot_games = df.shape[0]
        j=1.0
        weights = []
        for i in range(games,tot_games):
            j-=float(0.05)
            if j<0:
                j=0
            weights.append(j)
        weights += [1] * games
        weights = weights[0:tot_games]
        for stat in df.columns:
            if stat == 'date' or stat == 'opp_date':
                continue
            df_temp = df[stat]
            average = np.average(df_temp, weights=weights)
            variance = np.average((df_temp-average)**2, weights=weights)
            temp_dict[stat + '_std'] = math.sqrt(variance)
        return temp_dict


def input_own_offensive_stats(full_df):
    temp_df = full_df.drop(full_df.filter(like='_opp_'), axis=1)
    #temp_df.to_csv('offensive_csv.csv')
    input_data(temp_df, 'offensive_averages')
def input_own_defensive_stats(full_df):
    temp_df = full_df.filter(regex='_opp_')
    temp_df_temp = full_df[['Date', 'TeamName']]
    temp_df = pd.concat([temp_df_temp,temp_df], axis=1)
    #temp_df.to_csv('defensive_csv.csv')
    input_data(temp_df, 'defensive_averages')

def do_that_shit(start_value=None):
    start_date = determine_start_date(start_value)
    year = start_date.year
    month = start_date.month
    day = start_date.day
    datetime_temp = datetime.datetime(year, month, day)

    min_year = year - 1
    if month == 12:
        min_num_games = 2
    year_max = year
    if month < 13 and month > 7:
        year_max += 1
    the_biggest_boy_df = pd.DataFrame()
    do_this =True
    min_num_games = 2

    while datetime_temp < datetime.datetime(year_max, 4, 15) and datetime_temp < datetime.datetime.now() and do_this:
        print(datetime_temp)
        if datetime_temp.year == 2012 or datetime_temp.year == 2013:
            if datetime_temp.month > 2:
                min_num_games = 5
        else:
            if datetime_temp.month < 2:
                min_num_games = 5
            elif datetime_temp.month == 2:
                min_num_games = 10
        year = datetime_temp.year
        month = datetime_temp.month
        day = datetime_temp.day
        sql = """select distinct(away_team_name) from tot_boxscores
        where date > '"""+ str(min_year)+"""-10-01'
        and date < '"""+ str(year) + """-""" + str(month) + """-""" + str(day)+"""'
        GROUP BY away_team_name
        HAVING COUNT(away_team_name) > """+ str(min_num_games)+"""
        """
        result = mycursor.execute(sql)
        df = pd.DataFrame(mycursor.fetchall())
        field_names = [i[0] for i in mycursor.description]
        df.columns = field_names
        distinct_away = df

        sql = """select distinct(home_team_name) from tot_boxscores
        where date > '"""+ str(min_year)+"""-10-01'
        and date < '"""+ str(year) + '"""-' + str(month) + """-""" + str(day)+"""'
        GROUP BY home_team_name
        HAVING COUNT(home_team_name) > """+ str(min_num_games)+"""
        """
        result = mycursor.execute(sql)
        df = pd.DataFrame(mycursor.fetchall())
        field_names = [i[0] for i in mycursor.description]
        df.columns = field_names
        distinct_home = df

        distinct_home.columns = ['team_name']
        distinct_away.columns = ['team_name']
        all_teams = pd.concat([distinct_away,distinct_home], ignore_index=True)['team_name'].unique()

        super_big_boy_df = pd.DataFrame()


        for team in all_teams:
            print(team)
            sql = """select * from tot_boxscores
            where (away_team_name = '"""+team.replace('\'','\'\'')+"""' or home_team_name = '"""+team.replace('\'','\'\'')+"""')
            and date > '"""+ str(min_year)+"""-10-01'
            and date < '"""+ str(year) + '"""-' + str(month) + """-""" + str(day)+"""'"""

            result = mycursor.execute(sql)
            df = pd.DataFrame(mycursor.fetchall())
            field_names = [i[0] for i in mycursor.description]
            df.columns = field_names

            column_names = ['date', 'pts_scored', 'assists', 'blocks', 'def_reb', 'off_reb','tot_reb', 'made_3pts', 'tot_3pts', 'made_fts', 'tot_fts', 'made_2pts', 'tot_2pts', 'fouls', 'steals', 'turnovers', 'tempo']

            team_df = pd.DataFrame(columns=column_names)
            opp_df = pd.DataFrame(columns=column_names)

            for index, row in df.iterrows():
                if row['away_team_name'] == team:
                    date = row['date']
                    pts_scored = row['away_team_pts']
                    assists = row['away_assists']
                    blocks = row['away_blocks']
                    def_reb = row['away_def_reb']
                    off_reb = row['away_off_reb']
                    tot_reb = row['away_tot_reb']
                    made_3pts = row['away_made_3pts']
                    made_fts = row['away_made_ft']
                    made_2pts = row['away_made_shots'] - made_3pts
                    fouls = row['away_personal_fouls']
                    steals = row['away_steals']
                    tot_3pts = row['away_tot_3pts']
                    tot_fts = row['away_tot_ft']
                    tot_2pts = row['away_tot_shots'] - tot_3pts
                    turnovers = row['away_turnovers']
                    poss = row['game_tempo']
                    team_df.loc[len(team_df)] = [date, pts_scored, assists, blocks, def_reb, off_reb, tot_reb, made_3pts, tot_3pts, made_fts, tot_fts, made_2pts, tot_2pts, fouls, steals, turnovers,poss]
                    pts_scored = row['home_team_pts']
                    assists = row['home_assists']
                    blocks = row['home_blocks']
                    def_reb = row['home_def_reb']
                    off_reb = row['home_off_reb']
                    tot_reb = row['home_tot_reb']
                    made_3pts = row['home_made_3pts']
                    made_fts = row['home_made_ft']
                    made_2pts = row['home_made_shots'] - made_3pts
                    fouls = row['home_personal_fouls']
                    steals = row['home_steals']
                    tot_3pts = row['home_tot_3pts']
                    tot_fts = row['home_tot_ft']
                    tot_2pts = row['home_tot_shots'] - tot_3pts
                    turnovers = row['home_turnovers']
                    opp_df.loc[len(opp_df)] = [date, pts_scored, assists, blocks, def_reb, off_reb, tot_reb, made_3pts, tot_3pts, made_fts, tot_fts, made_2pts, tot_2pts, fouls, steals, turnovers, poss]

                else:
                    date = row['date']
                    pts_scored = row['away_team_pts']
                    assists = row['away_assists']
                    blocks = row['away_blocks']
                    def_reb = row['away_def_reb']
                    off_reb = row['away_off_reb']
                    tot_reb = row['away_tot_reb']
                    made_3pts = row['away_made_3pts']
                    made_fts = row['away_made_ft']
                    made_2pts = row['away_made_shots'] - made_3pts
                    fouls = row['away_personal_fouls']
                    steals = row['away_steals']
                    tot_3pts = row['away_tot_3pts']
                    tot_fts = row['away_tot_ft']
                    tot_2pts = row['away_tot_shots'] - tot_3pts
                    turnovers = row['away_turnovers']
                    poss = row['game_tempo']
                    opp_df.loc[len(opp_df)] = [date, pts_scored, assists, blocks, def_reb, off_reb, tot_reb, made_3pts, tot_3pts, made_fts, tot_fts, made_2pts, tot_2pts, fouls, steals, turnovers, poss]
                    pts_scored = row['home_team_pts']
                    assists = row['home_assists']
                    blocks = row['home_blocks']
                    def_reb = row['home_def_reb']
                    off_reb = row['home_off_reb']
                    tot_reb = row['home_tot_reb']
                    made_3pts = row['home_made_3pts']
                    made_fts = row['home_made_ft']
                    made_2pts = row['home_made_shots'] - made_3pts
                    fouls = row['home_personal_fouls']
                    steals = row['home_steals']
                    tot_3pts = row['home_tot_3pts']
                    tot_fts = row['home_tot_ft']
                    tot_2pts = row['home_tot_shots'] - tot_3pts
                    turnovers = row['home_turnovers']
                    team_df.loc[len(team_df)] = [date, pts_scored, assists, blocks, def_reb, off_reb, tot_reb, made_3pts, tot_3pts, made_fts, tot_fts, made_2pts, tot_2pts, fouls, steals, turnovers, poss]


            team_df = team_df.reset_index(drop=True)
            opp_df = opp_df.reset_index(drop=True)

            team_df['ppp'] = team_df.apply(lambda x: get_ppp(x['pts_scored'], x['tempo']), axis=1)
            team_df['efficency'] = team_df.apply(lambda x: get_efficency(x['pts_scored'], x['tempo']), axis=1)
            team_df['to_pct'] = team_df.apply(lambda x: get_to_pct(x['turnovers'], x['tempo']), axis=1)
            team_df['eff_fg_pct'] = team_df.apply(lambda x: get_eff_fg_pct(x['made_2pts'], x['made_3pts'], x['tot_2pts'], x['tot_3pts']), axis=1)
            team_df['true_shooting_pct'] = team_df.apply(lambda x: get_true_shooting_pct(x['pts_scored'], x['tot_2pts'], x['tot_3pts'], x['tot_fts']), axis=1)
            team_df['ft_rate'] = team_df.apply(lambda x: get_ft_rate(x['tot_2pts'], x['tot_3pts'], x['tot_fts']), axis=1)
            team_df['ft_pct'] = team_df.apply(lambda x: get_ft_pct(x['tot_fts'], x['made_fts']), axis=1)

            team_df['3pt_rate'] = team_df.apply(lambda x: get_3pt_rate(x['tot_2pts'], x['tot_3pts']), axis=1)
            team_df['3p_pct'] = team_df.apply(lambda x: get_3pt_pct(x['made_3pts'], x['tot_3pts']), axis=1)
            team_df['2p_pct'] = team_df.apply(lambda x: get_2pt_pct(x['made_2pts'], x['tot_2pts']), axis=1)
            team_df['steal_pct'] = team_df.apply(lambda x: get_steal_pct(x['steals'], x['tempo']), axis=1)
            opp_df['ppp'] = opp_df.apply(lambda x: get_ppp(x['pts_scored'], x['tempo']), axis=1)
            opp_df['efficency'] = opp_df.apply(lambda x: get_efficency(x['pts_scored'], x['tempo']), axis=1)
            opp_df['to_pct'] = opp_df.apply(lambda x: get_to_pct(x['turnovers'], x['tempo']), axis=1)
            opp_df['eff_fg_pct'] = opp_df.apply(lambda x: get_eff_fg_pct(x['made_2pts'], x['made_3pts'], x['tot_2pts'], x['tot_3pts']), axis=1)
            opp_df['true_shooting_pct'] = opp_df.apply(lambda x: get_true_shooting_pct(x['pts_scored'], x['tot_2pts'], x['tot_3pts'], x['tot_fts']), axis=1)
            opp_df['ft_rate'] = opp_df.apply(lambda x: get_ft_rate(x['tot_2pts'], x['tot_3pts'], x['tot_fts']), axis=1)
            opp_df['ft_pct'] = opp_df.apply(lambda x: get_ft_pct(x['tot_fts'], x['made_fts']), axis=1)
            opp_df['3pt_rate'] = opp_df.apply(lambda x: get_3pt_rate(x['tot_2pts'], x['tot_3pts']), axis=1)
            opp_df['3p_pct'] = opp_df.apply(lambda x: get_3pt_pct(x['made_3pts'], x['tot_3pts']), axis=1)
            opp_df['2p_pct'] = opp_df.apply(lambda x: get_2pt_pct(x['made_2pts'], x['tot_2pts']), axis=1)
            opp_df['steal_pct'] = opp_df.apply(lambda x: get_steal_pct(x['steals'], x['tempo']), axis=1)

            #print(team_df)
            #print(opp_df)

            opp_df=opp_df.add_prefix('opp_')
            full_df = pd.concat([team_df,opp_df], axis = 1)
            full_df['block_pct'] = full_df.apply(lambda x: get_block_pct(x['blocks'], x['opp_tot_2pts']), axis=1)
            full_df['opp_block_pct'] = full_df.apply(lambda x: get_block_pct(x['opp_blocks'], x['tot_2pts']), axis=1)
            full_df['off_rebound_pct'] = full_df.apply(lambda x: get_rebound_pct(x['off_reb'], x['opp_def_reb']), axis=1)
            full_df['def_rebound_pct'] = full_df.apply(lambda x: get_rebound_pct(x['def_reb'], x['opp_off_reb']), axis=1)
            full_df['opp_off_rebound_pct'] = full_df.apply(lambda x: get_rebound_pct(x['opp_off_reb'], x['def_reb']), axis=1)
            full_df['opp_def_rebound_pct'] = full_df.apply(lambda x: get_rebound_pct(x['opp_def_reb'], x['off_reb']), axis=1)
            full_df['tot_rebound_pct'] = full_df.apply(lambda x: get_rebound_pct(x['tot_reb'], x['opp_tot_reb']), axis=1)
            full_df['opp_tot_rebound_pct'] = full_df.apply(lambda x: get_rebound_pct(x['opp_tot_reb'], x['tot_reb']), axis=1)


            full_df=full_df[['ppp','efficency','to_pct','eff_fg_pct','true_shooting_pct','ft_rate','ft_pct','3pt_rate','3p_pct','2p_pct','steal_pct','opp_ppp','opp_efficency','opp_to_pct','opp_eff_fg_pct','opp_true_shooting_pct','opp_ft_rate','opp_ft_pct','opp_3pt_rate','opp_3p_pct','opp_2p_pct','opp_steal_pct','block_pct','off_rebound_pct','def_rebound_pct','tot_rebound_pct','opp_block_pct','opp_off_rebound_pct','opp_def_rebound_pct','opp_tot_rebound_pct','tempo']]
            avgs = get_averages(full_df)
            std_devs = get_std_dev(full_df)
            two_game_avg = get_x_game_averages(full_df,2)
            two_game_std_devs = get_x_game_stdev(full_df,2)

            three_game_avg = get_x_game_averages(full_df,3)
            three_game_std_devs = get_x_game_stdev(full_df,3)

            four_game_avg = get_x_game_averages(full_df,4)
            four_game_std_devs = get_x_game_stdev(full_df,4)


            five_game_avg = get_x_game_averages(full_df,5)
            five_game_std_devs = get_x_game_stdev(full_df,5)


            ten_game_avg = get_x_game_averages(full_df,10)
            ten_game_std_devs = get_x_game_stdev(full_df,10)

            twelve_game_avg = get_x_game_averages(full_df,12)
            twelve_game_std_devs = get_x_game_stdev(full_df,12)

            fifteen_game_avg = get_x_game_averages(full_df,15)
            fifteen_game_std_devs = get_x_game_stdev(full_df,15)

            twenty_game_avg = get_x_game_averages(full_df,20)
            twenty_game_std_devs = get_x_game_stdev(full_df,20)


            decrease_after_three_avg = get_avg_decreased_games(full_df,3)
            decrease_after_three_std = get_stdev_decreased_games(full_df,3)
            decrease_after_five_avg = get_avg_decreased_games(full_df,5)
            decrease_after_five_std = get_stdev_decreased_games(full_df,5)
            decrease_after_seven_avg = get_avg_decreased_games(full_df,7)
            decrease_after_seven_std = get_stdev_decreased_games(full_df,7)
            decrease_after_ten_avg = get_avg_decreased_games(full_df,10)
            decrease_after_ten_std = get_stdev_decreased_games(full_df,10)
            decrease_after_twelve_avg = get_avg_decreased_games(full_df,12)
            decrease_after_twelve_std = get_stdev_decreased_games(full_df,12)
            decrease_after_fifteen_avg = get_avg_decreased_games(full_df,15)
            decrease_after_fifteen_std = get_stdev_decreased_games(full_df,15)
            decrease_after_twenty_avg = get_avg_decreased_games(full_df,20)
            decrease_after_twenty_std = get_stdev_decreased_games(full_df,20)
            big_boy_df = pd.DataFrame(data = [[datetime_temp,team]], columns=['Date', 'TeamName'])

            big_boy_df = pd.concat([big_boy_df, pd.DataFrame([avgs]).add_prefix('full_season_avg_')],axis = 1)
            big_boy_df = pd.concat([big_boy_df, pd.DataFrame([std_devs]).add_prefix('full_season_stdev_')],axis = 1)

            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([two_game_avg]).add_prefix('two_game_avg_')],axis = 1)
            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([two_game_std_devs]).add_prefix('two_game_std_devs_')],axis = 1)

            big_boy_df = pd.concat([big_boy_df, pd.DataFrame([three_game_avg]).add_prefix('three_game_avg_')],axis = 1)
            big_boy_df = pd.concat([big_boy_df, pd.DataFrame([three_game_std_devs]).add_prefix('three_game_std_devs_')],axis = 1)

            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([four_game_avg]).add_prefix('four_game_avg_')],axis = 1)
            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([four_game_std_devs]).add_prefix('four_game_std_devs_')],axis = 1)

            big_boy_df = pd.concat([big_boy_df, pd.DataFrame([five_game_avg]).add_prefix('five_game_avg_')],axis = 1)
            big_boy_df = pd.concat([big_boy_df, pd.DataFrame([five_game_std_devs]).add_prefix('five_game_std_devs_')],axis = 1)

            big_boy_df = pd.concat([big_boy_df, pd.DataFrame([ten_game_avg]).add_prefix('ten_game_avg_')],axis = 1)
            big_boy_df = pd.concat([big_boy_df, pd.DataFrame([ten_game_std_devs]).add_prefix('ten_game_std_devs_')],axis = 1)

            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([twelve_game_avg]).add_prefix('twelve_game_avg_')],axis = 1)
            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([twelve_game_std_devs]).add_prefix('twelve_game_std_devs_')],axis = 1)

            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([fifteen_game_avg]).add_prefix('fifteen_game_avg_')],axis = 1)
            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([fifteen_game_std_devs]).add_prefix('fifteen_game_std_devs_')],axis = 1)


            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([twenty_game_avg]).add_prefix('twenty_game_avg_')],axis = 1)
            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([twenty_game_std_devs]).add_prefix('twenty_game_std_devs_')],axis = 1)

            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_three_avg]).add_prefix('three_game_decreasing_avg_')],axis = 1)
            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_three_std]).add_prefix('three_game_decreasing_std_')],axis = 1)

            big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_five_avg]).add_prefix('five_game_decreasing_avg_')],axis = 1)
            big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_five_std]).add_prefix('five_game_decreasing_std_')],axis = 1)

            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_seven_avg]).add_prefix('seven_game_decreasing_avg_')],axis = 1)
            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_seven_std]).add_prefix('seven_game_decreasing_std_')],axis = 1)


            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_ten_avg]).add_prefix('ten_game_decreasing_avg_')],axis = 1)
            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_ten_std]).add_prefix('ten_game_decreasing_std_')],axis = 1)

            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_twelve_avg]).add_prefix('twelve_game_decreasing_avg_')],axis = 1)
            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_twelve_std]).add_prefix('twelve_game_decreasing_std_')],axis = 1)

            big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_fifteen_avg]).add_prefix('fifteen_game_decreasing_avg_')],axis = 1)
            big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_fifteen_std]).add_prefix('fifteen_game_decreasing_std_')],axis = 1)

            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_twenty_avg]).add_prefix('twenty_game_decreasing_avg_')],axis = 1)
            #big_boy_df = pd.concat([big_boy_df, pd.DataFrame([decrease_after_twenty_std]).add_prefix('twenty_game_decreasing_std_')],axis = 1)
            super_big_boy_df = pd.concat([super_big_boy_df,big_boy_df],axis =0)

        input_own_offensive_stats(super_big_boy_df)
        input_own_defensive_stats(super_big_boy_df)
        datetime_temp += datetime.timedelta(days=1)

if __name__ == '__main__':
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    do_that_shit(arg)









            #blasdjhfa
