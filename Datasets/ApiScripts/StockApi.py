import os
import csv
import string
from time import sleep
import requests
from dotenv import load_dotenv
import datetime


def get_stock_data(ticker, date):
    """Returns the current news data fetched from NewsData.io API."""

    STOCKDATA_API_KEY = os.getenv("POLYGON_STOCK_API_KEY")

    url = f"https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&apiKey={STOCKDATA_API_KEY}"
    params = {"language" : "en"}

    req = requests.get(url, params)
    response = req.json()

    return response


def write_to_csv(data, file="stockdataset.csv", category = "None"):
    """Writes the content with category to csv file to make the dataset."""

    filepath = os.path.join(os.path.dirname(__file__),f"../{file}")
    if os.path.exists(filepath):
        if os.stat(filepath).st_size != 0:
            filemode = "a+"
        else:
            filemode = "w+"
    else:
        filemode = "w+"
    
    with open(filepath, filemode) as data_csv:
    
        csv_writer = csv.writer(data_csv, delimiter='\t')
        
        if(filemode == "w+"):
            csv_writer.writerow(["CATEGORY", "TICKER", "DATE", "OPEN", "CLOSE", "PRE_MARKET", "AFTER_HOURS", "HIGH", "LOW"])
        
        if(data['status'] == "OK"):
            csv_writer.writerow([category, data["symbol"], data["from"], data["open"], data["close"], data["preMarket"], data["afterHours"], data["high"], data["low"]])
        else:
            print("NOT OK")
        data_csv.close()

if __name__ == "__main__":

    file = "StockData.csv"

    load_dotenv()

    keywords = {
        "Health": ["vaccine", "hospital", "pharmaceuticals"],
        "Finance": ["share", "stock price","bank"],
        "EVs": ["tesla motors", "ather", "EVs"],
        "Telecom": ["5G", "Jio", "Airtel"],
        "Tech": ["Apple", "Facebook", "Google", "Amazon"]
    }

    ticker_list = {
        "Health": ["UNH"],
        "Finance": ["MS"],
        "EVs": ["TSLA", "NIO"],
        "Telecom":["AMT"],
        "Tech": ["INTC", "META"]
        
    }
    
    # cur_date = datetime.datetime.now()
    # x = cur_date.year - 2
    # start_date = datetime.datetime(x, cur_date.month, cur_date.day)
    # next_date = start_date + datetime.timedelta(days=5)
    next_date = datetime.datetime(2023, 1, 30)
    
    while(next_date <= datetime.datetime.now()):
        
        print(f"Date on process: {next_date}")
        
        for category in ticker_list.keys():
            print(f"\t ** {category}")
            tickers = ticker_list[category]
            
            for ticker in tickers:
                print(f"\t\t ** Fetching {ticker} Stock deets")
                data = get_stock_data(ticker, next_date.strftime("%Y-%m-%d"))
                write_to_csv(data=data, file=file, category=category)
                sleep(12)
        
        next_date = next_date + datetime.timedelta(days=1)
