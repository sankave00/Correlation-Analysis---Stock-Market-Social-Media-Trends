import os
import csv
from time import sleep
from dotenv import load_dotenv
import datetime
import time
import pandas as pd

def write_to_csv(data, file="stockdataset.csv", category = "None",ticker="None"):
    """Writes the content with category to csv file to make the dataset."""
    #print("Data", data)
    data.insert(0,'Category',category)
    data.insert(1,'Ticker',ticker)
    data.drop(columns=['Adj Close', 'Volume'], inplace=True)
    col = data.pop('Close')

    data.insert(4, col.name, col)
    filepath = os.path.join(os.path.dirname(__file__),f"../New Datasets/{file}")
    if os.path.exists(filepath):
        if os.stat(filepath).st_size != 0:
            filemode = "a+"
            with open(filepath, filemode, newline='') as data_csv:
              csv_writer = csv.writer(data_csv, delimiter='\t')
              for index, row in data.iterrows():
                  #print(row,sep="\n")
                  csv_writer.writerow(row)
            data_csv.close()
        else:
            data.to_csv(filepath, index=False, sep='\t')
    else:
        data.to_csv(filepath, index=False, sep='\t')
  

if __name__ == "__main__":

    file = "FinalStockData.csv"

    load_dotenv()

    keywords = {
        "Health": ["vaccine", "hospital", "doctor"],
        "Finance": ["shares", "stock price","bank"],
        "EVs": ["tesla motors", "hybrid vechicles", "EVs"],
        "Telecom": ["5G", "broadband", "mobile network"],
        "Tech": ["Apple", "Facebook", "Google", "Amazon"]
    }

    ticker_list = {
        "Health": ["UNH"],
        "Finance": ["MS"],
        "EVs": ["TSLA"],
        "Tech": ["GOOGL"]   
    }
    
    # cur_date = datetime.datetime.now()
    # x = cur_date.year - 2
    # start_date = datetime.datetime(x, cur_date.month, cur_date.day)
    # next_date = start_date + datetime.timedelta(days=5)
    start_date = int(time.mktime(datetime.datetime(2021, 4, 1, 0, 1).timetuple()))
    end_date = int(time.mktime(datetime.datetime(2023, 3, 31, 23, 59).timetuple()))
    interval = '1d' # 1d, 1m
    # while(next_date <= end_date):
        
    #     print(f"Date on process: {next_date}")
        
    for category in ticker_list.keys():
        print(f"\t ** {category}")
        tickers = ticker_list[category]
        
        for ticker in tickers:
            print(f"\t\t ** Fetching {ticker} Stock deets")
            query_string = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={start_date}&period2={end_date}&interval={interval}&events=history&includeAdjustedClose=true'
            print("Data", query_string)
            df = pd.read_csv(query_string)
            write_to_csv(data=df, file=file, category=category,ticker=ticker)
            sleep(10)


