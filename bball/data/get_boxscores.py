from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import numpy as np
import datetime
from input_data import *
import csv
from to_datetime import*



year = 2025
url = 'https://barttorvik.com/' + str(year) + '_super_sked.csv'
df = pd.read_csv(url)
df.to_csv('bart_files/'+str(year)+'_super_sked.csv', index=False)


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


sql = """SELECT MAX(date)
FROM tot_boxscores
"""

result = mycursor.execute(sql)
df = pd.DataFrame(mycursor.fetchall())
max_date = to_datetime(df.values[0][0])



with open('bart_files/'+str(year)+'_super_sked.csv') as file_obj:

    # Create reader object by passing the file
    # object to reader method
    reader_obj = csv.reader(file_obj)

    # Iterate over each row in the csv
    # file using reader object
    i=0
    for row in reader_obj:

        if row[24] == '':
            i+=1
            continue
        if row[6] == '99' or row[6]==99:
            print('see ya d2')
            i+=1
            continue

        date = datetime.datetime(int("20"+row[1].split("/")[2]), int(row[1].split("/")[0]), int(row[1].split("/")[1]))
        if date <= max_date:
            i+=1
            continue

        neutral_site = int(row[7])
        away_team_name = row[8]
        away_team_oe = float(row[9])
        away_team_de = float(row[10])
        away_team_pythag = float(row[11])
        away_team_proj_points = float(row[13])
        home_team_name = row[14]
        home_team_oe = float(row[15])
        home_team_de = float(row[16])
        home_team_pythag = float(row[17])
        home_team_proj_points = float(row[19])
        tot_project_posessions = float(row[20])

        away_team_t_rank = int(row[46])
        home_team_t_rank = int(row[47])
        boxscore = row[50]
        overtimes = row[51]
        if overtimes == '' or overtimes == 'Unnamed: 51':
            overtimes = 0
        else:
            overtimes = int(float(overtimes))
        game_tempo = float(row[25])
        away_team_adj_tempo = float(row[31])
        home_team_adj_tempo = float(row[32])
        away_team_adj_off = float(row[33])
        away_team_adj_def = float(row[34])
        home_team_adj_off = float(row[35])
        home_team_adj_def = float(row[36])
        away_team_ppp = float(row[43])
        home_team_ppp = float(row[44])
        boxscore = boxscore.split(",")

        away_team_pts = int(boxscore[18].replace("u'", "").replace(".0","").replace("'","").strip())
        away_made_shots = int(boxscore[4].replace("u'", "").replace(".0","").replace("'","").strip())

        away_tot_shots = int(boxscore[5].replace("u'", "").replace(".0","").replace("'","").strip())
        away_made_3pts = int(boxscore[6].replace("u'", "").replace(".0","").replace("'","").strip())
        away_tot_3pts = int(boxscore[7].replace("u'", "").replace(".0","").replace("'","").strip())
        away_made_ft = int(boxscore[8].replace("u'", "").replace(".0","").replace("'","").strip())
        away_tot_ft = int(boxscore[9].replace("u'", "").replace(".0","").replace("'","").strip())
        away_off_reb = int(boxscore[10].replace("u'", "").replace(".0","").replace("'","").strip())
        away_def_reb = int(boxscore[11].replace("u'", "").replace(".0","").replace("'","").strip())
        away_tot_reb = int(boxscore[12].replace("u'", "").replace(".0","").replace("'","").strip())
        away_assists = int(boxscore[13].replace("u'", "").replace(".0","").replace("'","").strip())
        away_steals = int(boxscore[14].replace("u'", "").replace(".0","").replace("'","").strip())
        away_blocks = int(boxscore[15].replace("u'", "").replace(".0","").replace("'","").strip())
        away_turnovers = int(boxscore[16].replace("u'", "").replace(".0","").replace("'","").strip())
        away_personal_fouls = int(boxscore[17].replace("u'", "").replace(".0","").replace("'","").strip())


        home_team_pts = int(boxscore[33].replace("u'", "").replace(".0","").replace("'","").strip())
        home_made_shots = int(boxscore[19].replace("u'", "").replace(".0","").replace("'","").strip())
        home_tot_shots = int(boxscore[20].replace("u'", "").replace(".0","").replace("'","").strip())
        home_made_3pts = int(boxscore[21].replace("u'", "").replace(".0","").replace("'","").strip())
        home_tot_3pts = int(boxscore[22].replace("u'", "").replace(".0","").replace("'","").strip())
        home_made_ft = int(boxscore[23].replace("u'", "").replace(".0","").replace("'","").strip())
        home_tot_ft = int(boxscore[24].replace("u'", "").replace(".0","").replace("'","").strip())
        home_off_reb = int(boxscore[25].replace("u'", "").replace(".0","").replace("'","").strip())
        home_def_reb = int(boxscore[26].replace("u'", "").replace(".0","").replace("'","").strip())
        home_tot_reb = int(boxscore[27].replace("u'", "").replace(".0","").replace("'","").strip())
        home_assists = int(boxscore[28].replace("u'", "").replace(".0","").replace("'","").strip())
        home_steals = int(boxscore[29].replace("u'", "").replace(".0","").replace("'","").strip())
        home_blocks = int(boxscore[30].replace("u'", "").replace(".0","").replace("'","").strip())
        home_turnovers = int(boxscore[31].replace("u'", "").replace(".0","").replace("'","").strip())
        home_personal_fouls = int(boxscore[32].replace("u'", "").replace(".0","").replace("'","").strip())

        data = [date, neutral_site,away_team_name,away_team_oe,away_team_de,away_team_pythag,away_team_proj_points,home_team_name,home_team_oe,home_team_de,home_team_pythag,home_team_proj_points,tot_project_posessions,away_team_pts,home_team_pts,away_team_t_rank,home_team_t_rank,overtimes,game_tempo,away_team_adj_tempo,home_team_adj_tempo,away_team_adj_off,away_team_adj_def,home_team_adj_off,home_team_adj_def,away_team_ppp,home_team_ppp,away_made_shots,away_tot_shots,away_made_3pts,away_tot_3pts,away_made_ft,away_tot_ft,away_off_reb,away_def_reb,away_tot_reb,away_assists,away_steals,away_blocks,away_turnovers,away_personal_fouls,home_made_shots,home_tot_shots,home_made_3pts,home_tot_3pts,home_made_ft,home_tot_ft,home_off_reb,home_def_reb,home_tot_reb,home_assists,home_steals,home_blocks,home_turnovers,home_personal_fouls]

        columns = ['date','neutral_site','away_team_name','away_team_oe','away_team_de','away_team_pythag','away_team_proj_points','home_team_name','home_team_oe','home_team_de','home_team_pythag','home_team_proj_points','tot_project_posessions','away_team_pts','home_team_pts','away_team_t_rank','home_team_t_rank','overtimes','game_tempo','away_team_adj_tempo','home_team_adj_tempo','away_team_adj_off','away_team_adj_def','home_team_adj_off','home_team_adj_def','away_team_ppp','home_team_ppp','away_made_shots','away_tot_shots','away_made_3pts','away_tot_3pts','away_made_ft','away_tot_ft','away_off_reb','away_def_reb','away_tot_reb','away_assists','away_steals','away_blocks','away_turnovers','away_personal_fouls','home_made_shots','home_tot_shots','home_made_3pts','home_tot_3pts','home_made_ft','home_tot_ft','home_off_reb','home_def_reb','home_tot_reb','home_assists','home_steals','home_blocks','home_turnovers','home_personal_fouls']

        df = pd.DataFrame(data=[data],columns=columns)
        input_data(df, 'tot_boxscores')
        i+=1
        if i % 100 == 0:
            print(i)
