from datetime import datetime, timedelta
import requests
import re
import pandas as pd
import datetime
from input_data import *
import csv
from sklearn.neighbors import KNeighborsRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error,r2_score,mean_squared_error
from xgboost import XGBRegressor
from sklearn.linear_model import ElasticNetCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.utils import shuffle
from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import GridSearchCV
from sklearn.decomposition import PCA
from do_models import*
import random
from predict_games import*
from sklearn.utils import shuffle
from more_stats import*
import trying_tensors
import inference
import os

import warnings
warnings.filterwarnings('ignore')

#from visualize_data import*


#random.seed(3220416)

def convert_ml_odds_before(df):
    away_win_odds = df['away_win_odds']
    home_win_odds = df['home_win_odds']
    print(away_win_odds)
    print(home_win_odds)
    if away_win_odds > 0:
        away_win_odds -= 100
    else:
        away_win_odds += 100
    if home_win_odds > 0:
        home_win_odds -= 100
    else:
        home_win_odds += 100
    print(away_win_odds)
    print(home_win_odds)

    return pd.Series({'away_win_odds': away_win_odds, 'home_win_odds': home_win_odds})

def convert_ml_odds_after(df):

    away_win_odds = df['away_win_odds']
    home_win_odds = df['home_win_odds']
    print(away_win_odds)
    print(home_win_odds)

    if away_win_odds > 0:
        away_win_odds += 100
    else:
        away_win_odds -= 100
    if home_win_odds > 0:
        home_win_odds += 100
    else:
        home_win_odds -= 100
    print(away_win_odds)
    print(home_win_odds)
    return pd.Series({'away_win_odds': away_win_odds, 'home_win_odds': home_win_odds})

def get_all_stats(row_df):
    print(row_df)

    sql = """SELECT fifteen_game_decreasing_avg_eff_fg_pct_avg as away_eff_fg_pct, fifteen_game_decreasing_avg_ft_pct_avg as away_ft_pct, fifteen_game_decreasing_avg_ft_rate_avg as away_ft_rate, fifteen_game_decreasing_avg_3pt_rate_avg as away_3pt_rate, fifteen_game_decreasing_avg_3p_pct_avg as away_3p_pct, fifteen_game_decreasing_avg_off_rebound_pct_avg as away_off_rebound_pct,fifteen_game_decreasing_avg_def_rebound_pct_avg as away_def_rebound_pct
    FROM sports.offensive_averages
    where TeamName = '""" + row_df['away_team_name'].replace('\'','\'\'') + """'
    and Date < \'""" + str(row_df['date'].year) + """-""" + str(row_df['date'].month) + """-""" + str(row_df['date'].day) + """\'
    and Date > \'""" + str(row_df['date'].year) + """-01-01'
    ORDER BY Date desc
    LIMIT 1
    """
    print(sql)
    result = mycursor.execute(sql)
    df = pd.DataFrame(mycursor.fetchall())
    field_names = [i[0] for i in mycursor.description]
    df.columns = field_names
    away_off_df = df

    sql = """SELECT fifteen_game_decreasing_avg_opp_eff_fg_pct_avg as away_def_eff_fg_pct, fifteen_game_decreasing_avg_opp_ft_rate_avg as away_opp_ft_rate, fifteen_game_decreasing_avg_opp_3pt_rate_avg as away_def_3pt_rate, fifteen_game_decreasing_avg_opp_3p_pct_avg as away_def_3p_pct, fifteen_game_decreasing_avg_opp_off_rebound_pct_avg as away_def_off_rebound_pct,fifteen_game_decreasing_avg_opp_def_rebound_pct_avg as away_def_def_rebound_pct
    FROM sports.defensive_averages
    where TeamName = '""" + row_df['away_team_name'].replace('\'','\'\'') + """'
    and Date < \'""" + str(row_df['date'].year) + """-""" + str(row_df['date'].month) + """-""" + str(row_df['date'].day) + """\'
    and Date > \'""" + str(row_df['date'].year) + """-01-01'
    ORDER BY Date desc
    LIMIT 1
    """
    print(sql)

    result = mycursor.execute(sql)
    df = pd.DataFrame(mycursor.fetchall())
    field_names = [i[0] for i in mycursor.description]
    df.columns = field_names
    away_def_df = df

    sql = """SELECT fifteen_game_decreasing_avg_eff_fg_pct_avg as home_eff_fg_pct, fifteen_game_decreasing_avg_ft_pct_avg as home_ft_pct, fifteen_game_decreasing_avg_ft_rate_avg as home_ft_rate, fifteen_game_decreasing_avg_3pt_rate_avg as home_3pt_rate, fifteen_game_decreasing_avg_3p_pct_avg as home_3p_pct, fifteen_game_decreasing_avg_off_rebound_pct_avg as home_off_rebound_pct,fifteen_game_decreasing_avg_def_rebound_pct_avg as home_def_rebound_pct
    FROM sports.offensive_averages
    where TeamName = '""" + row_df['home_team_name'].replace('\'','\'\'') + """'
    and Date < \'""" + str(row_df['date'].year) + """-""" + str(row_df['date'].month) + """-""" + str(row_df['date'].day) + """\'
    and Date > \'""" + str(row_df['date'].year) + """-01-01'
    ORDER BY Date desc
    LIMIT 1
    """
    print(sql)

    result = mycursor.execute(sql)
    df = pd.DataFrame(mycursor.fetchall())
    field_names = [i[0] for i in mycursor.description]
    df.columns = field_names
    home_off_df = df

    sql = """SELECT fifteen_game_decreasing_avg_opp_eff_fg_pct_avg as home_def_eff_fg_pct, fifteen_game_decreasing_avg_opp_ft_rate_avg as home_opp_ft_rate, fifteen_game_decreasing_avg_opp_3pt_rate_avg as home_def_3pt_rate, fifteen_game_decreasing_avg_opp_3p_pct_avg as home_def_3p_pct, fifteen_game_decreasing_avg_opp_off_rebound_pct_avg as home_def_off_rebound_pct,fifteen_game_decreasing_avg_opp_def_rebound_pct_avg as home_def_def_rebound_pct
    FROM sports.defensive_averages
    where TeamName = '""" + row_df['home_team_name'].replace('\'','\'\'') + """'
    and Date < \'""" + str(row_df['date'].year) + """-""" + str(row_df['date'].month) + """-""" + str(row_df['date'].day) + """\'
    and Date > \'""" + str(row_df['date'].year) + """-01-01'
    ORDER BY Date desc
    LIMIT 1
    """
    print(sql)

    result = mycursor.execute(sql)
    df = pd.DataFrame(mycursor.fetchall())
    field_names = [i[0] for i in mycursor.description]
    df.columns = field_names
    home_def_df = df


    return [away_off_df, away_def_df,home_off_df,home_def_df]

def do_that_shit(iter):
    i=0
    iter = int(iter)
    host=os.getenv("BBALL_DB_HOST")
    user=os.getenv("BBALL_DB_USER")
    password=os.getenv("BBALL_DB_PASS")
    database="sports"
    conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
    )
    mycursor = conn.cursor()
    sql = """SELECT * FROM sports.training_data"""

    result = mycursor.execute(sql)
    df = pd.DataFrame(mycursor.fetchall())
    field_names = [i[0] for i in mycursor.description]
    df.columns = field_names

    df['MOV'] = df['away_team_pts'] - df['home_team_pts']
    df['total_pts'] = df['away_team_pts'] + df['home_team_pts']
    df['home_team_home'] = df.apply(lambda x: True if x['neutral_site']==0 else False, axis=1)
    df['away_team_home'] = False
    return_dfs_total = []
    while i < iter:



    #sql = """SELECT tb.date, tb.neutral_site, tb.away_team_name, tb.away_team_pts, tb.home_team_name, tb.home_team_pts, dd.adj_oe as away_team_adj_oe, dd.BARTHAG as away_team_BARTHAG ,dd.adj_de as away_team_adj_de, dd.adj_pace as away_team_adj_pace, dd2.adj_oe as home_team_adj_oe,  dd2.adj_de as home_team_adj_de, dd2.adj_pace as home_team_adj_pace, dd2.BARTHAG as home_team_BARTHAG
    #FROM tot_boxscores as tb
    #JOIN daily_data as dd on (tb.away_team_name=dd.team_name and tb.date=dd.date)
    #JOIN daily_data as dd2 on (tb.home_team_name=dd2.team_name and tb.date=dd2.date)
    #where tb.date >= '"""2012-02-01"""'
    #and MONTH(tb.date) != 11"""

        df_shuffled = shuffle(df).reset_index(drop=True)
        #visualize_data(df)
        spread_y = df_shuffled['MOV']
        total_y = df_shuffled['total_pts']
        df_x = df_shuffled.drop(['MOV', 'total_pts', 'date', 'away_team_name', 'home_team_name', 'home_team_pts', 'away_team_pts'], axis =1)
        #print(df_x.columns.to_list())


        X_train, X_test, y_train, y_test = train_test_split(df_x, spread_y,test_size=.2)
        scaler = StandardScaler()
        X_train[['away_team_adj_oe', 'away_team_BARTHAG', 'away_team_adj_de', 'away_team_adj_pace', 'home_team_adj_oe', 'home_team_adj_de', 'home_team_adj_pace', 'home_team_BARTHAG', 'away_eff_fg_pct', 'away_ft_pct',
               'away_ft_rate', 'away_3pt_rate', 'away_3p_pct', 'away_off_rebound_pct',
               'away_def_rebound_pct', 'away_def_eff_fg_pct', 'away_def_ft_rate',
               'away_def_3pt_rate', 'away_def_3p_pct', 'away_def_off_rebound_pct',
               'away_def_def_rebound_pct', 'home_eff_fg_pct', 'home_ft_pct',
               'home_ft_rate', 'home_3pt_rate', 'home_3p_pct', 'home_off_rebound_pct',
               'home_def_rebound_pct', 'home_def_eff_fg_pct', 'home_opp_ft_rate',
               'home_def_3pt_rate', 'home_def_3p_pct', 'home_def_off_rebound_pct',
               'home_def_def_rebound_pct']]=scaler.fit_transform(X_train[['away_team_adj_oe', 'away_team_BARTHAG', 'away_team_adj_de', 'away_team_adj_pace', 'home_team_adj_oe', 'home_team_adj_de', 'home_team_adj_pace', 'home_team_BARTHAG', 'away_eff_fg_pct', 'away_ft_pct',
               'away_ft_rate', 'away_3pt_rate', 'away_3p_pct', 'away_off_rebound_pct',
               'away_def_rebound_pct', 'away_def_eff_fg_pct', 'away_def_ft_rate',
               'away_def_3pt_rate', 'away_def_3p_pct', 'away_def_off_rebound_pct',
               'away_def_def_rebound_pct', 'home_eff_fg_pct', 'home_ft_pct',
               'home_ft_rate', 'home_3pt_rate', 'home_3p_pct', 'home_off_rebound_pct',
               'home_def_rebound_pct', 'home_def_eff_fg_pct', 'home_opp_ft_rate',
               'home_def_3pt_rate', 'home_def_3p_pct', 'home_def_off_rebound_pct',
               'home_def_def_rebound_pct']])
        X_test[['away_team_adj_oe', 'away_team_BARTHAG', 'away_team_adj_de', 'away_team_adj_pace', 'home_team_adj_oe', 'home_team_adj_de', 'home_team_adj_pace', 'home_team_BARTHAG', 'away_eff_fg_pct', 'away_ft_pct',
               'away_ft_rate', 'away_3pt_rate', 'away_3p_pct', 'away_off_rebound_pct',
               'away_def_rebound_pct', 'away_def_eff_fg_pct', 'away_def_ft_rate',
               'away_def_3pt_rate', 'away_def_3p_pct', 'away_def_off_rebound_pct',
               'away_def_def_rebound_pct', 'home_eff_fg_pct', 'home_ft_pct',
               'home_ft_rate', 'home_3pt_rate', 'home_3p_pct', 'home_off_rebound_pct',
               'home_def_rebound_pct', 'home_def_eff_fg_pct', 'home_opp_ft_rate',
               'home_def_3pt_rate', 'home_def_3p_pct', 'home_def_off_rebound_pct',
               'home_def_def_rebound_pct']]=scaler.transform(X_test[['away_team_adj_oe', 'away_team_BARTHAG', 'away_team_adj_de', 'away_team_adj_pace', 'home_team_adj_oe', 'home_team_adj_de', 'home_team_adj_pace', 'home_team_BARTHAG', 'away_eff_fg_pct', 'away_ft_pct',
               'away_ft_rate', 'away_3pt_rate', 'away_3p_pct', 'away_off_rebound_pct',
               'away_def_rebound_pct', 'away_def_eff_fg_pct', 'away_def_ft_rate',
               'away_def_3pt_rate', 'away_def_3p_pct', 'away_def_off_rebound_pct',
               'away_def_def_rebound_pct', 'home_eff_fg_pct', 'home_ft_pct',
               'home_ft_rate', 'home_3pt_rate', 'home_3p_pct', 'home_off_rebound_pct',
               'home_def_rebound_pct', 'home_def_eff_fg_pct', 'home_opp_ft_rate',
               'home_def_3pt_rate', 'home_def_3p_pct', 'home_def_off_rebound_pct',
               'home_def_def_rebound_pct']])
        #model_nn = trying_tensors.tunechi_reg(X_train, X_test, y_train, y_test)
        #model_nn = tune_model(X_train, X_test, y_train, y_test,'nn')
        #y_pred_nn = test_model(model_nn,X_test)
        model_nn, model_nn_class = inference.load_models(X_train.shape[1])

        #model_nn_class = tune_model_class(X_train, X_test, y_train, y_test,'nn')
        #y_pred_nn_class = test_model_class(model_nn_class, X_test)
        #y_pred_nn = inference.run_inference_reg(X_test, model_nn)

        #y_pred_nn_class = inference.run_inference_class(X_test, model_nn_class)
        #y_pred_nn_class = trying_tensors.test_model_class(model_nn_class, X_test)
        #print(y_pred_test)
        #input()
        #input()
        #model_xgb = tune_model(X_train, X_test, y_train, y_test,'xgb')
        #y_pred_xgb = test_model(model_xgb, X_test)



        #temp_df_xgb = print_results(y_pred_xgb, y_test)
        #temp_df_nn = print_results(y_pred_nn, y_test)
        #y_pred_nn_class = y_pred_nn_class.reshape(-1, 1)

        #temp_df_nn_class = print_results_class(y_pred_nn_class, y_test)

        return_df = predict_todays_games(model_nn, model_nn_class, scaler, "pytorch")
        return_dfs_total.append(return_df)
        #predict_todays_games(model_nn, "xgb")

        #return_future_df = predict_future_games(model_nn,model_nn_class,scaler, "nn")
        i+=1
        #input()
    averaged_df = return_dfs_total[0]
    i = 1
    while i < len(return_dfs_total):
        averaged_df = pd.concat([averaged_df, return_dfs_total[i]])
        i+=1

    averaged_df[['away_win_odds','home_win_odds']] = averaged_df.apply(lambda x: convert_ml_odds_before(x), axis=1)
    #print(averaged_df.columns)
    averaged_df_averaged = averaged_df.groupby(by=['away_team_name','home_team_name']).mean()
    #print(averaged_df_averaged)
    averaged_df_averaged[['away_win_odds','home_win_odds']] = averaged_df_averaged.apply(lambda x: convert_ml_odds_after(x), axis=1)
    #print(averaged_df_averaged)
    str_date = datetime.datetime.now().strftime("%m_%d_%y_%H_%M_%S")
    averaged_df_averaged=averaged_df_averaged.sort_values(by=['spread_diff'], key=abs, ascending = [False])
    averaged_df_averaged.to_csv('predictions/averaged_'+str_date+'_predictions_total.csv')
    print('done')
    input()



if __name__ == '__main__':

    do_that_shit(sys.argv[1])
