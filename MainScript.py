import time
import os
import pandas as pd
from dotenv import load_dotenv
import datetime
import csv
import catboost
from catboost import CatBoostRegressor
import joblib
import numpy as np

import NewScripts.TweetFetcher as TweetFetcher
import NewScripts.NewStockFetcher as StockFetcher
import NewScripts.RedditFetcher as RedditFetcher
import NewScripts.Liveaggregate as Liveaggregate
# import Prediction.catboost_final as catboost_final

from NewScripts.TweetFetcher import *
from NewScripts.NewStockFetcher import *
from NewScripts.RedditFetcher import *
from NewScripts.Liveaggregate import *
from sklearn.preprocessing import MinMaxScaler

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

start_date = int(time.mktime(datetime.datetime(2023, 4, 20, 6,0,0).timetuple()))
end_date = int(time.mktime(datetime.datetime(2023, 4, 20, 23, 59,00).timetuple()))
one_day = datetime.timedelta(hours=24)
next_start_date = int(time.mktime(datetime.datetime(2023, 4, 21, 6,0,0).timetuple()))
next_end_date = int(time.mktime(datetime.datetime(2023, 4, 21, 23, 59,00).timetuple()))
start_date2 = datetime.datetime(2023, 4, 20)
after = str(int(start_date2.timestamp()))
before_date = start_date2 + one_day
before = str(int(before_date.timestamp()))
interval = '1d' # 1d, 1m
next_open = {}

for category in ticker_list.keys():
    print(f"\t ** {category}")
    tickers = ticker_list[category]
    
    for ticker in tickers:
        print(f"\t\t ** Fetching {ticker} Stock deets")
        query_string = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={start_date}&period2={end_date}&interval={interval}&events=history&includeAdjustedClose=true'
        df = pd.read_csv(query_string)
        query_string = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={next_start_date}&period2={next_end_date}&interval={interval}&events=history&includeAdjustedClose=true'
        next_df = pd.read_csv(query_string)
        print(f"%%%%%%%%%%{ticker}Close value :    ",next_df['Close'][0])
        print(f"%%%%%%%%%%{ticker}High value :    ",next_df['High'][0])
        print(f"%%%%%%%%%%{ticker}Low value :    ",next_df['Low'][0])
        next_open[category] = next_df['Open'][0]
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

Liveaggregate.initModel()
d=dict()
for i in ticker_list:
    print(i)
    tfile=i+'_live-tweets.csv'
    rfile=i+'_live-reddits.csv'
    df=Liveaggregate.readDf(tfile,rfile)
    df=Liveaggregate.computeSentiment(df)
    df=Liveaggregate.stringformat(df)
    df=Liveaggregate.aggregation(df)
    sdf=Liveaggregate.readSdf()
    sdf=sdf[sdf['CATEGORY']==i]
    
    mdf=Liveaggregate.mergerops(df,sdf)
    mdf = mdf.rename(columns={'DATE': 'date','CLOSE':'close','HIGH':'high','LOW':'low'})
    # print(mdf)
    mdf['open'] = next_open[i]
    d[ticker_list[i][0]]=mdf
#print(d)


maxvalues = dict()
minvalues = dict()
maxvalues['TSLA'] = {
    'open':311.666656,
    'prev_open':311.666656,
    'prev_wt_neg':-0.063934,
    'prev_wt_neu':-0.796949,
    'prev_wt_pos':-0.405471,
    'close':309.320007,
    'high':314.666656,
    'low':305.579987
}
maxvalues['MS'] = {
    'open':99.779999,
    'prev_open':99.779999,
    'prev_wt_neg':-8.218171,
    'prev_wt_neu':-3.026643,
    'prev_wt_pos':-9.230039,
    'close':100.830002,
    'high':100.989998,
    'low':99.5
}
maxvalues['GOOGL'] = {
    'open':121.519997,
    'prev_open':121.519997,
    'prev_wt_neg':96.811424,
    'prev_wt_neu':3065.08019,
    'prev_wt_pos':1384.068814,
    'close':122.080002,
    'high':122.43,
    'low':120.639999
}
minvalues['TSLA'] = {
    'open':103.0,
    'prev_open':103.0,
    'prev_wt_neg':-324.438132,
    'prev_wt_neu':-1738.56976,
    'prev_wt_pos':-1954.754501,
    'close':108.099998,
    'high':111.75,
    'low':101.809998
}
minvalues['MS'] = {
    'open':74.980003,
    'prev_open':74.980003,
    'prev_wt_neg':-634.398994,
    'prev_wt_neu':-7354.101371,
    'prev_wt_pos':-14779.116384,
    'close':75.300003,
    'high':77.800003,
    'low':74.669998
}
minvalues['GOOGL'] = {
    'open':85.400002,
    'prev_open':85.400002,
    'prev_wt_neg':0.154121,
    'prev_wt_neu':0.075774,
    'prev_wt_pos':1.074724,
    'close':83.43,
    'high':86.519997,
    'low':83.339996
}

def normalize():
    norm_dfs = dict()
                
    for category in ticker_list.keys():
        
        ticker = ticker_list[category][0]
        temp_norm_df = d[ticker].copy(deep=True)
            
        temp_norm_df = temp_norm_df.drop(columns=['date'])

        temp_norm_df.drop(columns=['close', 'high', 'low'], inplace=True)

        
        columns = ['open', 'prev_open','prev_wt_neg', 'prev_wt_neu', 'prev_wt_pos']
        temp_norm_df = pd.DataFrame(temp_norm_df, columns = columns)
        #print("Temp df3",temp_norm_df,sep="\n")
            
        norm_dfs[ticker] = temp_norm_df.copy(deep=True)
    return norm_dfs



norms_df = normalize()
print("Norms df\n",norms_df)
for category in ticker_list.keys():
    ticker = ticker_list[category][0]
    for output_label in ['close', 'high', 'low']:
      model = joblib.load(f'Models/Trained Models/Catboost/catboost_model_{ticker}-{output_label}')
      #print("Norms df ticker\n",norms_df[ticker])
      for col in norms_df[ticker].columns:
        #   print("****************",col)
          norms_df[ticker][col] = norms_df[ticker][col].astype(float)

        #   print(norms_df[ticker].loc[0,col])
        #   print(minvalues[ticker][col])
        #   print(maxvalues[ticker][col])
        #   print(type(norms_df[ticker].loc[0,col]))
        #   print(type(minvalues[ticker][col]))
        #   print(type(maxvalues[ticker][col]))
          norms_df[ticker].loc[0,col] = (norms_df[ticker].loc[0,col] - minvalues[ticker][col]) / (maxvalues[ticker][col] - minvalues[ticker][col])
      #print(norms_df[ticker][0:1])
      y_pred = model.predict(norms_df[ticker][0:1])
      #print(type(y_pred))
      #y_pred[0] = (y_pred[0] - minvalues[ticker][output_label]) / (maxvalues[ticker][output_label] - minvalues[ticker][output_label])
      val = y_pred[0] * (maxvalues[ticker][output_label] - minvalues[ticker][output_label]) + minvalues[ticker][output_label]
      print(f"PREDICTION OF {output_label} OF {ticker} : ",val)




