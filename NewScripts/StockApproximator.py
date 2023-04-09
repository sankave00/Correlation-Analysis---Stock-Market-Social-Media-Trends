import os
import datetime as dt
from datetime import date, datetime
import json
import csv
import pandas as pd
import numpy as np
import tqdm

from tqdm.notebook import tqdm

def approximate_values(open_date_map, close_date_map, high_date_map, low_date_map, existing_dates):
    starting_date = existing_dates[0]

    for i in range(1, len(existing_dates)):
        cur_date = existing_dates[i]

        while starting_date + dt.timedelta(days = 1) < cur_date:
            str_start_date, str_cur_date = starting_date.strftime('%Y-%m-%d'), cur_date.strftime('%Y-%m-%d')
            
            next_date = starting_date + dt.timedelta(days = 1)
            
            str_next_date = next_date.strftime('%Y-%m-%d')

            start_open, start_close = open_date_map[str_start_date], close_date_map[str_start_date]
            end_open, end_close = open_date_map[str_cur_date], close_date_map[str_cur_date]

            start_high, start_low = high_date_map[str_start_date], low_date_map[str_start_date]
            end_high, end_low = high_date_map[str_cur_date], low_date_map[str_cur_date]

            next_open = round(((start_open + end_open) / 2),2)
            next_close = round(((start_close + end_close) / 2),2)
            
            next_high = round(((start_high + end_high) / 2),2)
            next_low = round(((start_low + end_low) / 2),2)

            open_date_map[str_next_date] = next_open
            close_date_map[str_next_date] = next_close
            high_date_map[str_next_date] = next_high
            low_date_map[str_next_date] = next_low

            starting_date = next_date

        starting_date = cur_date
        
        
    open_date_map = dict(sorted(open_date_map.items()))
    close_date_map = dict(sorted(close_date_map.items()))
    high_date_map = dict(sorted(high_date_map.items()))
    low_date_map = dict(sorted(low_date_map.items()))
        
    return open_date_map, close_date_map, high_date_map, low_date_map

def write_to_csv(file,category, ticker, open_date_map, close_date_map, high_date_map, low_date_map):
    """Writes the content with category to csv file to make the dataset."""

    filepath = os.path.join(os.path.dirname(__file__),f"../New Datasets/{file}")
    if os.path.exists(filepath):
        if os.stat(filepath).st_size != 0:
            filemode = "a+"
        else:
            filemode = "w+"
    else:
        filemode = "w+"

    contents = []
    
    for date in open_date_map.keys():
        contents.append((category, ticker, date, open_date_map[date], close_date_map[date], high_date_map[date], low_date_map[date]))
    
    with open(filepath, filemode) as data_csv:
    
        csv_writer = csv.writer(data_csv, delimiter='\t')
        
        if(filemode == "w+"):
            csv_writer.writerow(["Category", "Ticker", "Date", "Open", "Close", "High", "Low"])
        
        for row in contents:
          csv_writer.writerow(row)
        print('Succesfully created new file')
        data_csv.close()

file = 'ApproximatedStockDataFinal.csv'
filepath = os.path.join(os.path.dirname(__file__),f"../New Datasets/FinalStockData.csv")
df = pd.read_csv(filepath,sep='\t')
g_df = df.groupby('Ticker')
print(g_df.head())
print("unique",df.Ticker.unique())

progress_bar = enumerate(df.Ticker.unique())

for i, ticker in progress_bar:
    temp_df = g_df.get_group(ticker)
    
    category = temp_df['Category'].unique()[0]
    
    data_dict_list = temp_df.to_dict('records')
    existing_dates = temp_df['Date'].to_list()
    
    existing_dates = [datetime.strptime(date, '%Y-%m-%d') for date in existing_dates]
    
    sorted(existing_dates)
    open_date_map = dict(zip(temp_df.Date, temp_df.Open))
    close_date_map = dict(zip(temp_df.Date, temp_df.Close))
    high_date_map = dict(zip(temp_df.Date, temp_df.High))
    low_date_map = dict(zip(temp_df.Date, temp_df.Low))

    open_date_map, close_date_map, high_date_map, low_date_map = approximate_values(open_date_map, close_date_map, high_date_map, low_date_map, existing_dates)
    write_to_csv(file,category, ticker, open_date_map, close_date_map, high_date_map, low_date_map)
