from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import datetime
from input_data import *
import csv
from pandas.io import sql
from sqlalchemy import create_engine
from to_datetime import*


def get_url_date(date):
    year = str(date.year)
    day = date.day
    month = date.month

    if day < 10:
        day = '0'+str(day)
    else:
        day = str(day)
    if month < 10:
        month = '0'+str(month)
    else:
        month = str(month)
    return year+month+day


host="localhost"
user="root"
password="jake3241"
database="sports"

conn = mysql.connector.connect(
  host=host,
  user=user,
  password=password,
  database=database
)
mycursor = conn.cursor()


sql = """SELECT MAX(date)
FROM daily_data;
"""

result = mycursor.execute(sql)
df = pd.DataFrame(mycursor.fetchall())
max_date = to_datetime(df.values[0][0])
print(max_date)
year = max_date.year
day = max_date.day
month = max_date.month


# 3/17/2013 data is weird

d = datetime.datetime(year = int(year), month = int(month), day = int(day))
columns = 'team_name','conference','record','adj_oe','rank_adj_oe','adj_de','rank_adj_de','BARTHAG','rank_BARTHAG','proj_wins','proj_losses','proj_conf_wins','proj_conf_losses','conference_record','idk_1','idk_2','idk_3','idk_4','idk_5','idk_6','idk_7','idk_8','idk_9','idk_10','idk_11','idk_12','conference_adj_oe','conference_adj_de','idk_15','idk_16','idk_17','idk_18','idk_19','idk_20','idk_21','idk_22','idk_23','idk_24','idk_25','idk_26','idk_27','idk_28','idk_29','adj_pace','date'
while d < datetime.datetime.now():
    print(d)
    year = d.year
    day = d.day
    month = d.month
    url = 'https://barttorvik.com/timemachine/team_results/'+ get_url_date(d)+'_team_results.json.gz'

    data = requests.get(url, headers = {'User-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'})
    #print(data.text)
    try:
        df = pd.read_json(data.text)

    except Exception as e:
        if month == 5:
            month =10
            day=15
            d = datetime.datetime(year = year, month = month, day = day)
        d+=datetime.timedelta(days=1)
        continue
    df['date'] = datetime.datetime(year = year, month = month, day = day) + timedelta(1)
    df = df.drop(list(df.columns[[0]]), axis = 1)
    df.columns = columns
    #df.to_csv('daily_data.csv')
    input_data(df, 'daily_data')

    d+=datetime.timedelta(days=1)
