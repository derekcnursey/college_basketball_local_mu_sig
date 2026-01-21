import mysql.connector
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import datetime
from pandas.io import sql
from sqlalchemy import create_engine



def input_data(df, table):
    #print(df)
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

    str = "mysql+mysqlconnector://"+ user +":"+password+"@"+host+"/"+database
    engine = create_engine(str)
    try:
        df.to_sql(table, con=engine, if_exists='append', index=False)
        #print('data added')
        return True
    except Exception as e:
        print(e)
        input()
        return False

    conn.close()
