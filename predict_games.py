# predict_games.py
"""
Build features for *today's* games to use with the new bball CLI + Torch models.

Assumptions
----------
- You already have a local MySQL database "sports" populated with:
    * daily_data
    * sub_offensive_averages
    * sub_defensive_averages
    * (whatever your existing get_all_stats / get_stats rely on)
- You have a Bart Torvik super schedule CSV saved at:
    bart_files/{season}_super_sked.csv
  where `season = 2025` for the 2024–25 season, etc.
- Your existing functions get_all_stats(row_df) and get_stats(row_df)
  work as before and produce the same features you used to build
  sports.training_data / training_data.csv.
"""

from __future__ import annotations

import mysql.connector  # ⬅️ NEW

from datetime import datetime, timedelta
import datetime as _dt
from pathlib import Path
from typing import Tuple

import pandas as pd
import numpy as np

# This should be your existing connection setup
# (or whatever you use today in input_data.py)
from input_data import *  # noqa: F401,F403  (for mycursor / engine, etc.)
import hard_rock_converter
import os
from dotenv import load_dotenv
load_dotenv(Path(".env"))

# ---------------------------------------------------------------------------
# DB connection (matches input_data.py settings)
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("BBALL_DB_HOST", "localhost"),
    "user": os.getenv("BBALL_DB_USER", "root"),
    "password": os.getenv("BBALL_DB_PASS", ""),
    "database": os.getenv("BBALL_DB_NAME", "sports"),
    "port": int(os.getenv("BBALL_DB_PORT", "3306")),
}

cnx = mysql.connector.connect(**DB_CONFIG)
mycursor = cnx.cursor()

# ---------------------------------------------------------------------------
# Global date helpers (used by your existing SQL logic)
# ---------------------------------------------------------------------------

_now = _dt.datetime.now()
CURR_YEAR = _now.year
CURR_MONTH = _now.month
CURR_DAY = _now.day
yesterday = _now - timedelta(days=1)  # needed by get_todays_lines


# ---------------------------------------------------------------------------
# COPY YOUR EXISTING STAT FUNCTIONS HERE
# ---------------------------------------------------------------------------
def _ensure_row_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame([{col: None for col in df.columns}])
    return df

#
def get_all_stats(row_df):
    #print(row_df)
    print(row_df['away_team_name'])
    print(row_df['home_team_name'])
    sql = """SELECT eff_fg_pct as away_eff_fg_pct, ft_pct as away_ft_pct, ft_rate as away_ft_rate, 3pt_rate as away_3pt_rate, 3p_pct as away_3p_pct, off_rebound_pct as away_off_rebound_pct,def_rebound_pct as away_def_rebound_pct
    FROM sports.sub_offensive_averages
    where TeamName = '""" + row_df['away_team_name'].replace('\'','\'\'') + """'
    and Date < \'""" + str(row_df['date'].year) + """-""" + str(row_df['date'].month) + """-""" + str(row_df['date'].day) + """\'
    ORDER BY Date desc
    LIMIT 1
    """
    print(sql)

    mycursor.execute(sql)
    rows = mycursor.fetchall()
    field_names = [i[0] for i in mycursor.description] if mycursor.description else []
    df = pd.DataFrame(rows, columns=field_names)
    away_off_df = _ensure_row_df(df)

    sql = """SELECT def_eff_fg_pct as away_def_eff_fg_pct, def_ft_rate as away_def_ft_rate, def_3pt_rate as away_def_3pt_rate, def_3p_pct as away_def_3p_pct, def_off_rebound_pct as away_def_off_rebound_pct,def_def_rebound_pct as away_def_def_rebound_pct
    FROM sports.sub_defensive_averages
    where TeamName = '""" + row_df['away_team_name'].replace('\'','\'\'') + """'
    and Date < \'""" + str(row_df['date'].year) + """-""" + str(row_df['date'].month) + """-""" + str(row_df['date'].day) + """\'
    ORDER BY Date desc
    LIMIT 1
    """
    print(sql)
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    field_names = [i[0] for i in mycursor.description] if mycursor.description else []
    df = pd.DataFrame(rows, columns=field_names)
    away_def_df = _ensure_row_df(df)

    sql = """SELECT eff_fg_pct as home_eff_fg_pct, ft_pct as home_ft_pct, ft_rate as home_ft_rate, 3pt_rate as home_3pt_rate, 3p_pct as home_3p_pct, off_rebound_pct as home_off_rebound_pct,def_rebound_pct as home_def_rebound_pct
    FROM sports.sub_offensive_averages
    where TeamName = '""" + row_df['home_team_name'].replace('\'','\'\'') + """'
    and Date < \'""" + str(row_df['date'].year) + """-""" + str(row_df['date'].month) + """-""" + str(row_df['date'].day) + """\'
    ORDER BY Date desc
    LIMIT 1
    """
    print(sql)

    mycursor.execute(sql)
    rows = mycursor.fetchall()
    field_names = [i[0] for i in mycursor.description] if mycursor.description else []
    df = pd.DataFrame(rows, columns=field_names)
    home_off_df = _ensure_row_df(df)

    sql = """SELECT def_eff_fg_pct as home_def_eff_fg_pct, def_ft_rate as home_def_ft_rate, def_3pt_rate as home_def_3pt_rate, def_3p_pct as home_def_3p_pct, def_off_rebound_pct as home_def_off_rebound_pct,def_def_rebound_pct as home_def_def_rebound_pct
    FROM sports.sub_defensive_averages
    where TeamName = '""" + row_df['home_team_name'].replace('\'','\'\'') + """'
    and Date < \'""" + str(row_df['date'].year) + """-""" + str(row_df['date'].month) + """-""" + str(row_df['date'].day) + """\'
    ORDER BY Date desc
    LIMIT 1
    """
    print(sql)

    mycursor.execute(sql)
    rows = mycursor.fetchall()
    field_names = [i[0] for i in mycursor.description] if mycursor.description else []
    df = pd.DataFrame(rows, columns=field_names)
    home_def_df = _ensure_row_df(df)
    with open("log_output.txt", "a") as f:  # Append mode
        f.write(f"Processing: {row_df}\n")
    #print('hell yeah')
    #print(pd.concat([away_off_df.iloc[0], away_def_df.iloc[0],home_off_df.iloc[0],home_def_df.iloc[0]]))
    return pd.concat([away_off_df.iloc[0], away_def_df.iloc[0], home_off_df.iloc[0], home_def_df.iloc[0]])
#
def get_stats(row_df):
    team_name = row_df['away_team_name']
    if '\'' in row_df['away_team_name']:
        team_name = row_df['away_team_name'].replace('\'', '\'\'')
    sql = """SELECT adj_oe, adj_de, BARTHAG, adj_pace
    from sports.daily_data
    where team_name = \'""" + team_name + """\'
    and date >= \'""" + str(CURR_YEAR) + """-""" + str(CURR_MONTH) + """-""" + str(CURR_DAY) + """\';"""
    print(sql)
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    field_names = [i[0] for i in mycursor.description] if mycursor.description else []
    df = pd.DataFrame(rows, columns=field_names)
    away_adj_oe = df['adj_oe'][0]
    away_adj_de = df['adj_de'][0]
    away_BARTHAG = df['BARTHAG'][0]
    away_adj_pace = df['adj_pace'][0]
    team_name = row_df['home_team_name']
    if '\'' in row_df['home_team_name']:
        team_name = row_df['home_team_name'].replace('\'', '\'\'')
    sql = """SELECT adj_oe, adj_de, BARTHAG, adj_pace
    from sports.daily_data
    where team_name = \'""" + team_name + """\'
    and date >= \'""" + str(CURR_YEAR) + """-""" + str(CURR_MONTH) + """-""" + str(CURR_DAY) + """\';"""
    print(sql)
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    field_names = [i[0] for i in mycursor.description] if mycursor.description else []
    df = pd.DataFrame(rows, columns=field_names)
    home_adj_oe = df['adj_oe'][0]
    home_adj_de = df['adj_de'][0]
    home_BARTHAG = df['BARTHAG'][0]
    home_adj_pace = df['adj_pace'][0]
    home_team_home = True if row_df['neutral_site']==0 else False
    away_team_home = False
    return pd.Series([away_adj_oe, away_BARTHAG, away_adj_de, away_adj_pace, home_adj_oe, home_adj_de, home_adj_pace, home_BARTHAG, home_team_home, away_team_home])

def get_stats_past(row_df):
    team_name = row_df['away_team_name']
    if '\'' in row_df['away_team_name']:
        team_name = row_df['away_team_name'].replace('\'', '\'\'')
    sql = """SELECT adj_oe, adj_de, BARTHAG, adj_pace
    from sports.daily_data
    where team_name = \'""" + team_name + """\'
    ORDER BY date desc
    LIMIT 1"""
    print(sql)
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    field_names = [i[0] for i in mycursor.description] if mycursor.description else []
    df = pd.DataFrame(rows, columns=field_names)
    away_adj_oe = df['adj_oe'][0]
    away_adj_de = df['adj_de'][0]
    away_BARTHAG = df['BARTHAG'][0]
    away_adj_pace = df['adj_pace'][0]
    team_name = row_df['home_team_name']
    if '\'' in row_df['home_team_name']:
        team_name = row_df['home_team_name'].replace('\'', '\'\'')
    sql = """SELECT adj_oe, adj_de, BARTHAG, adj_pace
    from sports.daily_data
    where team_name = \'""" + team_name + """\'
    ORDER BY date desc
    LIMIT 1"""
    print(sql)
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    field_names = [i[0] for i in mycursor.description] if mycursor.description else []
    df = pd.DataFrame(rows, columns=field_names)
    home_adj_oe = df['adj_oe'][0]
    home_adj_de = df['adj_de'][0]
    home_BARTHAG = df['BARTHAG'][0]
    home_adj_pace = df['adj_pace'][0]
    home_team_home = True if row_df['neutral_site']==0 else False
    away_team_home = False
    return pd.Series([away_adj_oe, away_BARTHAG, away_adj_de, away_adj_pace, home_adj_oe, home_adj_de, home_adj_pace, home_BARTHAG, home_team_home, away_team_home])

def _format_sql_time(dt_value: _dt.datetime) -> str:
    return dt_value.strftime("%y/%m/%d %H:%M:%S")


def _coerce_date(value: object) -> _dt.date:
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, str):
        return _dt.datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError("target_date must be a date/datetime or YYYY-MM-DD string")


def _empty_hard_rock_row() -> pd.Series:
    mycursor.execute("SELECT * from sports.hard_rock_lines LIMIT 0")
    mycursor.fetchall()
    field_names = [i[0] for i in mycursor.description] if mycursor.description else []
    df = pd.DataFrame([{col: None for col in field_names}])
    if {"Time", "away_team_name", "home_team_name"}.issubset(df.columns):
        df = df.drop(["Time", "away_team_name", "home_team_name"], axis=1)
    df = _ensure_row_df(df)
    return df.iloc[0]


def get_todays_lines(away_team_name, home_team_name, target_date: object | None = None):
    if target_date is None:
        sql = """
        SELECT max(Time) from sports.hard_rock_lines
        """
        result = mycursor.execute(sql)
        max_date_time = mycursor.fetchall()[0][0]
        lower_bound = yesterday
        if max_date_time is None:
            return _empty_hard_rock_row()
    else:
        target = _coerce_date(target_date)
        day_start = _dt.datetime(target.year, target.month, target.day)
        day_end = day_start + timedelta(days=1)
        sql = """
        SELECT max(Time) from sports.hard_rock_lines
        where Time >= \'""" + _format_sql_time(day_start) + """\'
        and Time < \'""" + _format_sql_time(day_end) + """\'
        """
        result = mycursor.execute(sql)
        max_date_time = mycursor.fetchall()[0][0]
        if max_date_time is None:
            return _empty_hard_rock_row()
        lower_bound = day_start

    sql = """
    SELECT * from sports.hard_rock_lines
    where Time = \'""" + _format_sql_time(max_date_time) + """\'
    and away_team_name =\'""" + hard_rock_converter.convert_name(away_team_name).replace('\'','\'\'') + """\'
    and home_team_name =\'""" + hard_rock_converter.convert_name(home_team_name).replace('\'','\'\'') + """\';
    """
    print(sql)
    result = mycursor.execute(sql)
    df = pd.DataFrame(mycursor.fetchall())
    if df.empty:
        sql = """
        SELECT * from sports.hard_rock_lines
        where away_team_name =\'""" + hard_rock_converter.convert_name(away_team_name).replace('\'','\'\'') + """\'
        and home_team_name =\'""" + hard_rock_converter.convert_name(home_team_name).replace('\'','\'\'') + """\'
        and Time < \'""" + _format_sql_time(max_date_time) + """\'
        and Time >= \'""" + _format_sql_time(lower_bound) + """\'
        order by Time desc
        LIMIT 1;
        """
        print(sql)
        result = mycursor.execute(sql)
        df = pd.DataFrame(mycursor.fetchall())
    if df.empty:
        sql = """
        SELECT * from sports.hard_rock_lines
        where Time = \'""" + _format_sql_time(max_date_time) + """\'
        and away_team_name =\'""" + hard_rock_converter.convert_name(home_team_name).replace('\'','\'\'') + """\'
        and home_team_name =\'""" + hard_rock_converter.convert_name(away_team_name).replace('\'','\'\'') + """\';
        """
        print(sql)

        result = mycursor.execute(sql)
        df = pd.DataFrame(mycursor.fetchall())
        if not df.empty:
            time = df[0]
            away_team = df[1]
            home_team = df[2]
            away_spread = df[[3,4]]
            home_spread = df[[5,6]]
            over_unders = df[[7,8,9,10]]
            away_ml = df[11]
            home_ml = df[12]
            df = pd.concat([time, home_team, away_team,home_spread, away_spread, over_unders, home_ml, away_ml], axis=1)
    if df.empty:
        sql = """
        SELECT * from sports.hard_rock_lines
        where away_team_name =\'""" + hard_rock_converter.convert_name(home_team_name).replace('\'','\'\'') + """\'
        and home_team_name =\'""" + hard_rock_converter.convert_name(away_team_name).replace('\'','\'\'') + """\'
        and Time < \'""" + _format_sql_time(max_date_time) + """\'
        and Time >= \'""" + _format_sql_time(lower_bound) + """\'
        order by Time desc
        LIMIT 1;
        """
        print(sql)

        result = mycursor.execute(sql)
        df = pd.DataFrame(mycursor.fetchall())
        if not df.empty:
            time = df[0]
            away_team = df[1]
            home_team = df[2]
            away_spread = df[[3,4]]
            home_spread = df[[5,6]]
            over_unders = df[[7,8,9,10]]
            away_ml = df[11]
            home_ml = df[12]
            df = pd.concat([time, home_team, away_team,home_spread, away_spread, over_unders, home_ml, away_ml], axis=1)
    if df.empty:
        return _empty_hard_rock_row()
    field_names = [i[0] for i in mycursor.description]
    df.columns = field_names
    df = df.drop(['Time', 'away_team_name', 'home_team_name'], axis =1)
    df = _ensure_row_df(df)
    return df.iloc[0]

def get_diffs(df_row):
    def _is_missing(value):
        return pd.isna(value)

    spread_home = df_row['spread_home']
    home_spread_num = df_row['home_spread_num']
    if _is_missing(spread_home) or _is_missing(home_spread_num):
        spread_diff = np.nan
    else:
        spread_diff = spread_home - home_spread_num

    away_winner_odds = df_row['away_winner_odds']
    away_win_odds = df_row['away_win_odds']
    if _is_missing(away_winner_odds) or _is_missing(away_win_odds):
        away_winner_diff = np.nan
    elif away_winner_odds > 0 and away_win_odds < 0:
        away_winner_diff = (away_winner_odds - 100) + (100 + away_win_odds)
    elif away_winner_odds < 0 and away_win_odds > 0:
        away_winner_diff = (away_winner_odds + 100) + (away_win_odds - 100)
    else:
        away_winner_diff = away_winner_odds - away_win_odds

    home_winner_odds = df_row['home_winner_odds']
    home_win_odds = df_row['home_win_odds']
    if _is_missing(home_winner_odds) or _is_missing(home_win_odds):
        home_winner_diff = np.nan
    elif home_winner_odds > 0 and home_win_odds < 0:
        home_winner_diff = (home_winner_odds - 100) + (100 + home_win_odds)
    elif home_winner_odds < 0 and home_win_odds > 0:
        home_winner_diff = (home_winner_odds + 100) + (home_win_odds - 100)
    else:
        home_winner_diff = home_winner_odds - home_win_odds

    return(pd.Series([spread_diff, away_winner_diff, home_winner_diff]))


# ---------------------------------------------------------------------------
# New: pull today's games from Bart Torvik super sked
# ---------------------------------------------------------------------------

def get_games_for_today(season_year: int, bart_dir: str = "bart_files") -> pd.DataFrame:
    """
    Read bart_files/{season_year}_super_sked.csv and return *today's* games,
    excluding D2 games (where column 6 == 99).
    """
    csv_path = Path(bart_dir) / f"{season_year}_super_sked.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Super sked not found at {csv_path}. "
            "Make sure you’ve downloaded it into bart_files/."
        )

    games_df = pd.read_csv(csv_path, header=None)

    # 1 = game datetime
    games_df[1] = pd.to_datetime(games_df[1])

    today_start = _dt.datetime(CURR_YEAR, CURR_MONTH, CURR_DAY)
    today_end = today_start + timedelta(days=1)

    # only today's games
    mask_date = (games_df[1] >= today_start) & (games_df[1] < today_end)

    # drop D2 games: column 6 == 99 (numeric or string)
    mask_div = (games_df[6] != 99) & (games_df[6].astype(str) != "99")

    # apply both filters
    todays_games = games_df.loc[mask_date & mask_div, [7, 8, 14]].copy()

    # 7 = neutral_site, 8 = away_team_name, 14 = home_team_name
    todays_games.columns = ["neutral_site", "away_team_name", "home_team_name"]
    todays_games = todays_games.reset_index(drop=True)
    todays_games = todays_games[todays_games.home_team_name != 'Western New Mexico']
    todays_games = todays_games[todays_games.away_team_name != 'Western New Mexico']
    todays_games = todays_games[todays_games.home_team_name != 'Middle Ga. St.']
    todays_games = todays_games[todays_games.away_team_name != 'Middle Ga. St.']
    todays_games = todays_games[todays_games.home_team_name != 'Northern New Mexico']
    todays_games = todays_games[todays_games.away_team_name != 'Northern New Mexico']
    todays_games = todays_games[todays_games.home_team_name != 'Pacific Oregon']
    todays_games = todays_games[todays_games.away_team_name != 'Pacific Oregon']
    todays_games = todays_games[todays_games.home_team_name != 'UMass Boston']
    todays_games = todays_games[todays_games.away_team_name != 'UMass Boston']
    todays_games = todays_games[todays_games.home_team_name != 'Virginia-Lynchburg']
    todays_games = todays_games[todays_games.away_team_name != 'Virginia-Lynchburg']
    todays_games = todays_games[todays_games.home_team_name != 'Hardin-Simmons']
    todays_games = todays_games[todays_games.away_team_name != 'Hardin-Simmons']
    todays_games = todays_games[todays_games.home_team_name != 'Northwest Indian']
    todays_games = todays_games[todays_games.away_team_name != 'Northwest Indian']
    todays_games = todays_games[todays_games.home_team_name != 'Johnson (TN)']
    todays_games = todays_games[todays_games.away_team_name != 'Johnson (TN)']
    todays_games = todays_games[todays_games.home_team_name != 'Brescia']
    todays_games = todays_games[todays_games.away_team_name != 'Brescia']
    todays_games = todays_games[todays_games.home_team_name != 'SUNY Delhi']
    todays_games = todays_games[todays_games.away_team_name != 'SUNY Delhi']
    todays_games = todays_games[todays_games.home_team_name != 'Walla Walla']
    todays_games = todays_games[todays_games.away_team_name != 'Walla Walla']
    todays_games = todays_games[todays_games.home_team_name != 'Bryn Athyn']
    todays_games = todays_games[todays_games.away_team_name != 'Bryn Athyn']
    todays_games = todays_games[todays_games.home_team_name != 'Crown MN']
    todays_games = todays_games[todays_games.away_team_name != 'Crown MN']
    todays_games = todays_games[todays_games.home_team_name != 'Bethany (WV)']
    todays_games = todays_games[todays_games.away_team_name != 'Bethany (WV)']
    todays_games = todays_games[todays_games.home_team_name != 'St. Francis (IL)']
    todays_games = todays_games[todays_games.away_team_name != 'St. Francis (IL)']
    todays_games = todays_games[todays_games.home_team_name != 'St. Mary-Woods']
    todays_games = todays_games[todays_games.away_team_name != 'St. Mary-Woods']
    todays_games = todays_games[todays_games.home_team_name != 'Eureka']
    todays_games = todays_games[todays_games.away_team_name != 'Eureka']
    todays_games = todays_games[todays_games.home_team_name != 'Shawnee St.']
    todays_games = todays_games[todays_games.away_team_name != 'Shawnee St.']
    todays_games = todays_games[todays_games.home_team_name != 'Ky. Christian']
    todays_games = todays_games[todays_games.away_team_name != 'Ky. Christian']
    todays_games = todays_games[todays_games.home_team_name != 'La Sierra']
    todays_games = todays_games[todays_games.away_team_name != 'La Sierra']
    todays_games = todays_games[todays_games.home_team_name != 'Our Lady of the Lake']
    todays_games = todays_games[todays_games.away_team_name != 'Our Lady of the Lake']
    todays_games = todays_games[todays_games.home_team_name != 'Champion Chris.']
    todays_games = todays_games[todays_games.away_team_name != 'Champion Chris.']
    todays_games = todays_games[todays_games.home_team_name != 'Lancaster Bible']
    todays_games = todays_games[todays_games.away_team_name != 'Lancaster Bible']
    todays_games = todays_games[todays_games.home_team_name != 'South Alabama']
    todays_games = todays_games[todays_games.away_team_name != 'South Alabama']
    todays_games = todays_games[todays_games.home_team_name != 'Stonehill']
    todays_games = todays_games[todays_games.away_team_name != 'Stonehill']
    todays_games = todays_games[todays_games.away_team_name != 'Truett-McConnell']
    todays_games = todays_games[todays_games.home_team_name != 'Truett-McConnell']
    todays_games = todays_games[todays_games.away_team_name != 'St. Thomas Houston']
    todays_games = todays_games[todays_games.home_team_name != 'St. Thomas Houston']
    todays_games = todays_games[todays_games.away_team_name != 'West Va. Wesleyan']
    todays_games = todays_games[todays_games.home_team_name != 'West Va. Wesleyan']
    todays_games = todays_games[todays_games.away_team_name != 'Willamette']
    todays_games = todays_games[todays_games.home_team_name != 'Willamette']
    todays_games = todays_games[todays_games.away_team_name != 'Whittier']
    todays_games = todays_games[todays_games.home_team_name != 'Whittier']
    todays_games = todays_games[todays_games.away_team_name != 'Cairn']
    todays_games = todays_games[todays_games.home_team_name != 'Cairn']
    todays_games = todays_games[todays_games.away_team_name != 'St. Andrews']
    todays_games = todays_games[todays_games.home_team_name != 'St. Andrews']
    todays_games = todays_games[todays_games.away_team_name != 'LaGrange']
    todays_games = todays_games[todays_games.home_team_name != 'LaGrange']
    todays_games = todays_games[todays_games.away_team_name != 'CarolinaU']
    todays_games = todays_games[todays_games.home_team_name != 'CarolinaU']
    todays_games = todays_games[todays_games.away_team_name != 'Misericordia']
    todays_games = todays_games[todays_games.home_team_name != 'Misericordia']
    todays_games = todays_games[todays_games.away_team_name != 'William Peace']
    todays_games = todays_games[todays_games.home_team_name != 'William Peace']
    todays_games = todays_games[todays_games.away_team_name != 'Mid-Atlantic Christ.']
    todays_games = todays_games[todays_games.home_team_name != 'Mid-Atlantic Christ.']
    todays_games = todays_games[todays_games.away_team_name != 'Ecclesia']
    todays_games = todays_games[todays_games.home_team_name != 'Ecclesia']
    todays_games = todays_games[todays_games.away_team_name != 'S\'western Adventist']
    todays_games = todays_games[todays_games.home_team_name != 'S\'western Adventist']
    todays_games = todays_games[todays_games.away_team_name != 'Montreat']
    todays_games = todays_games[todays_games.home_team_name != 'Montreat']
    todays_games = todays_games[todays_games.away_team_name != 'Tennessee Wesleyan']
    todays_games = todays_games[todays_games.home_team_name != 'Tennessee Wesleyan']
    todays_games = todays_games[todays_games.away_team_name != 'Lincoln (MO)']
    todays_games = todays_games[todays_games.home_team_name != 'Lincoln (MO)']
    todays_games = todays_games[todays_games.away_team_name != 'Lincoln (CA)']
    todays_games = todays_games[todays_games.home_team_name != 'Lincoln (CA)']
    todays_games = todays_games[todays_games.away_team_name != 'Michigan Tech']
    todays_games = todays_games[todays_games.home_team_name != 'Michigan Tech']
    todays_games = todays_games[todays_games.away_team_name != 'Regent']
    todays_games = todays_games[todays_games.home_team_name != 'Regent']
    todays_games = todays_games[todays_games.away_team_name != 'Chadron St.']
    todays_games = todays_games[todays_games.home_team_name != 'Chadron St.']
    todays_games = todays_games[todays_games.away_team_name != 'UNT Dallas']
    todays_games = todays_games[todays_games.home_team_name != 'UNT Dallas']
    todays_games = todays_games[todays_games.away_team_name != 'Milligan']
    todays_games = todays_games[todays_games.home_team_name != 'Milligan']
    todays_games = todays_games[todays_games.away_team_name != 'Schreiner']
    todays_games = todays_games[todays_games.home_team_name != 'Schreiner']
    todays_games = todays_games[todays_games.away_team_name != 'Franciscan']
    todays_games = todays_games[todays_games.home_team_name != 'Franciscan']
    todays_games = todays_games[todays_games.away_team_name != 'Defiance']
    todays_games = todays_games[todays_games.home_team_name != 'Defiance']
    todays_games = todays_games[todays_games.away_team_name != 'Dillard']
    todays_games = todays_games[todays_games.home_team_name != 'Dillard']
    todays_games = todays_games[todays_games.away_team_name != 'Loyola LA']
    todays_games = todays_games[todays_games.home_team_name != 'Loyola LA']
    todays_games = todays_games[todays_games.away_team_name != 'Texas Wesleyan']
    todays_games = todays_games[todays_games.home_team_name != 'Texas Wesleyan']
    todays_games = todays_games[todays_games.away_team_name != 'South Dakota Mines']
    todays_games = todays_games[todays_games.home_team_name != 'South Dakota Mines']
    todays_games = todays_games[todays_games.away_team_name != 'Southern-N.O.']
    todays_games = todays_games[todays_games.home_team_name != 'Southern-N.O.']
    todays_games = todays_games[todays_games.away_team_name != 'Ky. Wesleyan']
    todays_games = todays_games[todays_games.home_team_name != 'Ky. Wesleyan']
    todays_games = todays_games[todays_games.away_team_name != 'Rust']
    todays_games = todays_games[todays_games.home_team_name != 'Rust']
    todays_games = todays_games[todays_games.away_team_name != 'Immaculata']
    todays_games = todays_games[todays_games.home_team_name != 'Immaculata']
    todays_games = todays_games[todays_games.away_team_name != 'UHSP']
    todays_games = todays_games[todays_games.home_team_name != 'UHSP']
    todays_games = todays_games[todays_games.away_team_name != 'Biblical Stud. (TX)']
    todays_games = todays_games[todays_games.home_team_name != 'Biblical Stud. (TX)']
    todays_games = todays_games[todays_games.away_team_name != 'Howard Payne']
    todays_games = todays_games[todays_games.home_team_name != 'Howard Payne']
    todays_games = todays_games[todays_games.away_team_name != 'Brewton-Parker']
    todays_games = todays_games[todays_games.home_team_name != 'Brewton-Parker']
    todays_games = todays_games[todays_games.away_team_name != 'Penn St.-Fayette']
    todays_games = todays_games[todays_games.home_team_name != 'Penn St.-Fayette']
    todays_games = todays_games[todays_games.away_team_name != 'North Central (IL)']
    todays_games = todays_games[todays_games.home_team_name != 'North Central (IL)']
    todays_games = todays_games[todays_games.away_team_name != 'Non-DI']
    todays_games = todays_games[todays_games.home_team_name != 'Non-DI']
    todays_games = todays_games[todays_games.away_team_name != 'Florida Tech']
    todays_games = todays_games[todays_games.home_team_name != 'Florida Tech']
    todays_games = todays_games[todays_games.away_team_name != 'Calumet Col.']
    todays_games = todays_games[todays_games.home_team_name != 'Calumet Col.']
    todays_games = todays_games[todays_games.away_team_name != 'Midway']
    todays_games = todays_games[todays_games.home_team_name != 'Midway']
    todays_games = todays_games[todays_games.away_team_name != 'Fort Lauderdale']
    todays_games = todays_games[todays_games.home_team_name != 'Fort Lauderdale']
    todays_games = todays_games[todays_games.away_team_name != 'Stanislaus St.']
    todays_games = todays_games[todays_games.home_team_name != 'Stanislaus St.']
    todays_games = todays_games[todays_games.away_team_name != 'Occidental']
    todays_games = todays_games[todays_games.home_team_name != 'Occidental']
    todays_games = todays_games[todays_games.away_team_name != 'Spartanburg Meth.']
    todays_games = todays_games[todays_games.home_team_name != 'Spartanburg Meth.']
    todays_games = todays_games[todays_games.away_team_name != 'Dallas']
    todays_games = todays_games[todays_games.home_team_name != 'Dallas']
    todays_games = todays_games[todays_games.away_team_name != 'Bethesda (CA)']
    todays_games = todays_games[todays_games.home_team_name != 'Bethesda (CA)']
    todays_games = todays_games[todays_games.away_team_name != 'Webber International']
    todays_games = todays_games[todays_games.home_team_name != 'Webber International']
    todays_games = todays_games[todays_games.away_team_name != 'William Woods']
    todays_games = todays_games[todays_games.home_team_name != 'William Woods']
    todays_games = todays_games[todays_games.away_team_name != 'Cal Maritime']
    todays_games = todays_games[todays_games.home_team_name != 'Cal Maritime']
    todays_games = todays_games[todays_games.away_team_name != 'Westcliff']
    todays_games = todays_games[todays_games.home_team_name != 'Westcliff']
    todays_games = todays_games[todays_games.away_team_name != 'Southwest (NM)']
    todays_games = todays_games[todays_games.home_team_name != 'Southwest (NM)']
    todays_games = todays_games[todays_games.away_team_name != 'Southwestern Christ.']
    todays_games = todays_games[todays_games.home_team_name != 'Southwestern Christ.']
    todays_games = todays_games[todays_games.away_team_name != 'Mt. Marty']
    todays_games = todays_games[todays_games.home_team_name != 'Mt. Marty']
    todays_games = todays_games[todays_games.away_team_name != 'Columbia Int\'l']
    todays_games = todays_games[todays_games.home_team_name != 'Columbia Int\'l']
    todays_games = todays_games[todays_games.away_team_name != 'Va. Wesleyan']
    todays_games = todays_games[todays_games.home_team_name != 'Va. Wesleyan']
    todays_games = todays_games[todays_games.away_team_name != 'Blackburn']
    todays_games = todays_games[todays_games.home_team_name != 'Blackburn']
    todays_games = todays_games[todays_games.away_team_name != 'Alice Lloyd']
    todays_games = todays_games[todays_games.home_team_name != 'Alice Lloyd']
    todays_games = todays_games[todays_games.away_team_name != 'Nobel']
    todays_games = todays_games[todays_games.home_team_name != 'Nobel']
    todays_games = todays_games[todays_games.away_team_name != 'Friends']
    todays_games = todays_games[todays_games.home_team_name != 'Friends']
    todays_games = todays_games[todays_games.away_team_name != 'Asbury']
    todays_games = todays_games[todays_games.home_team_name != 'Asbury']
    todays_games = todays_games[todays_games.away_team_name != 'Heidelberg']
    todays_games = todays_games[todays_games.home_team_name != 'Heidelberg']
    todays_games = todays_games[todays_games.away_team_name != 'Westminster (MO)']
    todays_games = todays_games[todays_games.home_team_name != 'Westminster (MO)']
    todays_games = todays_games[todays_games.away_team_name != 'Manhattanville']
    todays_games = todays_games[todays_games.home_team_name != 'Manhattanville']
    todays_games = todays_games[todays_games.away_team_name != 'JWU (Providence)']
    todays_games = todays_games[todays_games.home_team_name != 'JWU (Providence)']
    todays_games = todays_games[todays_games.away_team_name != 'Neumann']
    todays_games = todays_games[todays_games.home_team_name != 'Neumann']
    todays_games = todays_games[todays_games.away_team_name != 'Waldorf']
    todays_games = todays_games[todays_games.home_team_name != 'Waldorf']
    todays_games = todays_games[todays_games.away_team_name != 'Texas Lutheran']
    todays_games = todays_games[todays_games.home_team_name != 'Texas Lutheran']
    todays_games = todays_games[todays_games.away_team_name != 'Warner']
    todays_games = todays_games[todays_games.home_team_name != 'Warner']
    todays_games = todays_games[todays_games.away_team_name != 'Regis (MA)']
    todays_games = todays_games[todays_games.home_team_name != 'Regis (MA)']
    todays_games = todays_games[todays_games.away_team_name != 'Muskingum']
    todays_games = todays_games[todays_games.home_team_name != 'Muskingum']
    todays_games = todays_games[todays_games.away_team_name != 'Dallas Christian']
    todays_games = todays_games[todays_games.home_team_name != 'Dallas Christian']
    todays_games = todays_games[todays_games.away_team_name != 'Emerson']
    todays_games = todays_games[todays_games.home_team_name != 'Emerson']
    todays_games = todays_games[todays_games.away_team_name != 'Fort Valley St.']
    todays_games = todays_games[todays_games.home_team_name != 'Fort Valley St.']
    todays_games = todays_games[todays_games.away_team_name != 'Cleary']
    todays_games = todays_games[todays_games.home_team_name != 'Cleary']
    todays_games = todays_games[todays_games.away_team_name != 'TAMU-San Antonio']
    todays_games = todays_games[todays_games.home_team_name != 'TAMU-San Antonio']
    todays_games = todays_games[todays_games.away_team_name != 'Eastern Oregon']
    todays_games = todays_games[todays_games.home_team_name != 'Eastern Oregon']
    todays_games = todays_games[todays_games.away_team_name != 'Benedictine Mesa']
    todays_games = todays_games[todays_games.home_team_name != 'Benedictine Mesa']
    todays_games = todays_games[todays_games.away_team_name != 'Tex. A&M-Texarkana']
    todays_games = todays_games[todays_games.home_team_name != 'Tex. A&M-Texarkana']
    todays_games = todays_games[todays_games.away_team_name != 'Elms']
    todays_games = todays_games[todays_games.home_team_name != 'Elms']
    todays_games = todays_games[todays_games.away_team_name != 'William Carey']
    todays_games = todays_games[todays_games.home_team_name != 'William Carey']
    todays_games = todays_games[todays_games.away_team_name != 'East-West U.']
    todays_games = todays_games[todays_games.home_team_name != 'East-West U.']
    todays_games = todays_games[todays_games.away_team_name != 'Anderson (IN)']
    todays_games = todays_games[todays_games.home_team_name != 'Anderson (IN)']
    todays_games = todays_games[todays_games.away_team_name != 'Kean']
    todays_games = todays_games[todays_games.home_team_name != 'Kean']
    todays_games = todays_games[todays_games.away_team_name != 'Medgar Evers']
    todays_games = todays_games[todays_games.home_team_name != 'Medgar Evers']
    todays_games = todays_games[todays_games.away_team_name != 'Penn St.-Schuylkill']
    todays_games = todays_games[todays_games.home_team_name != 'Penn St.-Schuylkill']
    todays_games = todays_games[todays_games.away_team_name != 'Bowdoin']
    todays_games = todays_games[todays_games.home_team_name != 'Bowdoin']
    todays_games = todays_games[todays_games.away_team_name != 'Bowie St.']
    todays_games = todays_games[todays_games.home_team_name != 'Bowie St.']
    todays_games = todays_games[todays_games.away_team_name != 'Cheyney']
    todays_games = todays_games[todays_games.home_team_name != 'Cheyney']
    todays_games = todays_games[todays_games.away_team_name != 'Washington Adventist']
    todays_games = todays_games[todays_games.home_team_name != 'Washington Adventist']
    todays_games = todays_games[todays_games.away_team_name != 'Morehouse']
    todays_games = todays_games[todays_games.home_team_name != 'Morehouse']
    todays_games = todays_games[todays_games.away_team_name != 'Penn St.-Shenango']
    todays_games = todays_games[todays_games.home_team_name != 'Penn St.-Shenango']
    # If you still want to hard-filter certain non-D1 teams,
    # you can add those filters here (same pattern as your old script).
    #
    # Example:
    # bad = {"Western New Mexico", "Middle Ga. St."}
    # todays_games = todays_games[
    #     ~todays_games.home_team_name.isin(bad)
    #     & ~todays_games.away_team_name.isin(bad)
    # ]

    return todays_games

def attach_hard_rock_lines(
    df: pd.DataFrame,
    pred_col: str = "pred_margin",
    target_date: object | None = None,
) -> pd.DataFrame:
    """
    Given a DataFrame with at least:
        - away_team_name
        - home_team_name
        - pred_col (e.g. 'pred_margin')
        - pred_home_win_prob (for model-implied ML)

    Attach:
        - Hard Rock line data from sports.hard_rock_lines
          (including away_winner_odds/home_winner_odds = BOOK ML)
        - Model-implied moneylines: away_win_odds / home_win_odds
        - Edge columns via get_diffs:
              spread_diff, away_winner_diff, home_winner_diff
    """

    # 1️⃣ Pull lines for each game using your existing SQL logic
    def _fetch_lines(row):
        return get_todays_lines(
            row["away_team_name"],
            row["home_team_name"],
            target_date=target_date,
        )

    lines_df = df.apply(_fetch_lines, axis=1).reset_index(drop=True)

    out = df.reset_index(drop=True).copy()
    out = pd.concat([out, lines_df], axis=1)

    # At this point out has columns like:
    # home_spread_num, away_spread_num, away_winner_odds, home_winner_odds, etc.
    # We do NOT touch away_winner_odds/home_winner_odds (they are the book ML).

    # 2️⃣ Compute MODEL-implied American odds from predicted win prob
    #     -> home_win_odds / away_win_odds (what get_diffs expects)
    if "pred_home_win_prob" in out.columns:
        ph = out["pred_home_win_prob"].astype(float).clip(1e-6, 1.0 - 1e-6)
        pa = 1.0 - ph

        def prob_to_american(p: pd.Series) -> pd.Series:
            p = p.astype(float)
            fav_mask = p >= 0.5
            odds = np.empty_like(p, dtype=float)

            # Favorites: negative odds
            odds[fav_mask] = -np.round(100 * p[fav_mask] / (1.0 - p[fav_mask]))
            # Dogs: positive odds
            odds[~fav_mask] = np.round(100 * (1.0 - p[~fav_mask]) / p[~fav_mask])
            return odds

        out["home_win_odds"] = prob_to_american(ph)
        out["away_win_odds"] = prob_to_american(pa)

    out["model_home_spread"] = -out[pred_col]

    # get_diffs expects 'spread_home' in the same sign convention as home_spread_num
    out["spread_home"] = out["model_home_spread"]

    # 4️⃣ Finally, compute diff columns using your existing get_diffs logic
    diffs_df = out.apply(get_diffs, axis=1).reset_index(drop=True)
    diffs_df.columns = ["spread_diff", "away_winner_diff", "home_winner_diff"]

    out = pd.concat([out, diffs_df], axis=1)
    return out


# ---------------------------------------------------------------------------
# New: build feature matrix for today’s games in training_data format
# ---------------------------------------------------------------------------

FEATURE_ORDER_PATH = Path("artifacts") / "feature_order.json"


def build_today_feature_frame(
    season_year: int,
    bart_dir: str = "bart_files",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build (info_df, X_df) for today's games.

    info_df
        Human-readable cols (date, neutral_site, away_team_name, home_team_name).
    X_df
        Pure numeric model inputs, columns matching artifacts/feature_order.json.
    """
    games = get_games_for_today(season_year=season_year, bart_dir=bart_dir)

    if games.empty:
        # No games today – return empty frames with correct columns
        if FEATURE_ORDER_PATH.exists():
            import json

            feat_cols = json.load(open(FEATURE_ORDER_PATH))
        else:
            feat_cols = []  # fallback

        info_cols = ["date", "neutral_site", "away_team_name", "home_team_name"]
        return pd.DataFrame(columns=info_cols), pd.DataFrame(columns=feat_cols)

    # Attach a "date" column used by your SQL (get_all_stats / get_stats)
    games = games.copy()
    games["date"] = _dt.datetime(CURR_YEAR, CURR_MONTH, CURR_DAY)

    # 1) advanced team strength (adj_oe, adj_de, BARTHAG, pace, home flags)
    stats_basic = games.apply(get_stats, axis=1)
    stats_basic = stats_basic.rename(
        columns={
            0: "away_team_adj_oe",
            1: "away_team_BARTHAG",
            2: "away_team_adj_de",
            3: "away_team_adj_pace",
            4: "home_team_adj_oe",
            5: "home_team_adj_de",
            6: "home_team_adj_pace",
            7: "home_team_BARTHAG",
            8: "home_team_home",
            9: "away_team_home",
        }
    )

    # 2) four-factor style stats from your rolling averages
    stats_factors = games.apply(get_all_stats, axis=1)

    # Combine into a single feature frame
    features_raw = pd.concat(
        [
            games[["neutral_site"]].reset_index(drop=True),
            stats_basic.reset_index(drop=True),
            stats_factors.reset_index(drop=True),
        ],
        axis=1,
    )

    # Load feature order used during training (this is what the Torch models expect)
    import json

    feat_cols = json.load(open(FEATURE_ORDER_PATH))
    # Reindex to that order; any missing columns become NaN → fill with 0
    X_df = features_raw.reindex(columns=feat_cols)
    X_df = X_df.apply(pd.to_numeric, errors="coerce").fillna(0.0)

    # Info frame for output / CSV
    info_df = games[["away_team_name", "home_team_name", "neutral_site"]].copy()
    info_df.insert(0, "date", games["date"])

    return info_df, X_df
