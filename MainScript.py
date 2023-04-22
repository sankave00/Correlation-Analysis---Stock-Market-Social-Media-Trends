import time
import os
import pandas as pd
from dotenv import load_dotenv
import datetime
import csv

import NewScripts.TweetFetcher as TweetFetcher
import NewScripts.NewStockFetcher as StockFetcher
import NewScripts.RedditFetcher as RedditFetcher
from NewScripts.TweetFetcher import *
from NewScripts.NewStockFetcher import *
from NewScripts.NewStockFetcher import *

twitter_keywords = {
    "Finance": ["Morgan Stanley", "investment","#MS", "$MS" ],
    "EVs": ["tesla","EVs","#TSLA", "$TSLA"],
    "Tech": ["Google","#GOOGL","#GOOG","$GOOGL","$GOOG"]
}
reddit_keywords = {
    "Finance": ["'Morgan Stanley'","'stock price'", "investment","MS" ],
    "EVs": ["tesla","EVs","'electric vehicle'","TSLA"],
    "Tech": ["Google","'Alphabet Inc.'","GOOGL"]
}
ticker_list = {
    "Finance": ["MS"],
    "EVs": ["TSLA"],
    "Tech": ["GOOGL"]   
}
load_dotenv()

start_date = int(time.mktime(datetime.datetime(2023, 4, 20, 0, 1).timetuple()))
end_date = int(time.mktime(datetime.datetime(2023, 4, 20, 23, 59).timetuple()))
one_day = datetime.timedelta(hours=24)
start_date2 = datetime.datetime(2023, 4, 20)
after = str(int(start_date2.timestamp()))
before_date = start_date2 + one_day
before = str(int(before_date.timestamp()))
interval = '1d' # 1d, 1m

for category in ticker_list.keys():
    print(f"\t ** {category}")
    tickers = ticker_list[category]
    
    for ticker in tickers:
        print(f"\t\t ** Fetching {ticker} Stock deets")
        query_string = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={start_date}&period2={end_date}&interval={interval}&events=history&includeAdjustedClose=true'
        df = pd.read_csv(query_string)
        StockFetcher.write_to_csv(data=df, file='live-stock.csv', category=category,ticker=ticker)

    query_params = {}
    query_params['tweet.fields'] = 'created_at,lang,source,public_metrics'
    query_params['start_time'] = f'{start_date2.strftime("%Y-%m-%d")}T00:00:00Z'
    query_params['end_time'] = f'{start_date2.strftime("%Y-%m-%d")}T23:59:59Z'
    query_params['max_results'] = 100

    keywords_list = twitter_keywords[category]

    print(f'\tCollecting {category} tweets')
    print(f"&&&Fetching data for {start_date2.strftime('%Y-%m-%d')}")

    for keyword in keywords_list:
        responses = set()
        response_dict = dict()

        print(f'\t\tCollecting {keyword} tweets')

        query_params['query'] = f"( -is:retweet {keyword} -has:links)"
        print(query_params)
        response = TweetFetcher.get_tweet_data(query_params, 5)

        for resp in response:  # add to set to prevent duplicates
            formatted_resp = TweetFetcher.format_response(resp[0])
            if formatted_resp not in responses:
                responses.add(formatted_resp)
                response_dict[formatted_resp] = resp[1]
            else:
                response_dict[formatted_resp] = resp[1]

        TweetFetcher.write_to_csv(response_dict, 'live-tweets.csv', category,start_date2)

    keywords_list = reddit_keywords[category]  
    for keyword in keywords_list:  
      sub_count = 0
      sub_stats = {}
      sub_reddit = keyword
      key_word = keyword

      data = RedditFetcher.get_pushshift_data(key_word, after, before, sub_reddit)
      #print(data)
      max_count = 3
      count = 0
      while len(data) > 0 and count < max_count:
          print('count ', count)
          for submission in data:
              RedditFetcher.collect_sub_data(submission,sub_stats)
              sub_count += 1

          #print(len(data))
          print(str(datetime.datetime.fromtimestamp(data[-1]['created_utc'])))
          after = data[-1]['created_utc']
          data = RedditFetcher.get_pushshift_data(key_word, after, before, sub_reddit)
          # print(data)
          # print(data['data'][0]['author'])
          count = count + 1

      # keep saving data collected in each iteration
      RedditFetcher.write_subs_to_file(category,'live-reddits.csv',sub_stats)
