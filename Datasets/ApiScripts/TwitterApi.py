import re
from urllib import response
from numpy import extract
import requests
import os
import json
from dotenv import load_dotenv
import csv
import string
import requests
from urllib import response
import datetime

bearer_token = ''
url = ''

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def get_tweet_data(query_params, page_count = 10):
    extracted_tweets = []
    response = requests.request("GET", url, auth=bearer_oauth, params=query_params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    json_response = response.json()
     
    while page_count:
        page_count -= 1
        
        if 'data' in json_response:
            for tweet in json_response['data']:
                if tweet['lang'] == 'en':
                    extracted_tweets.append(tweet['text'])
    
    print(f'\t\t *** Extracted {len(extracted_tweets)} tweets ***')
    return extracted_tweets

def write_to_csv(data, file="tweetdataset.csv", category = "None", date=datetime.datetime.now()):
    """Writes the content with category to csv file to make the dataset."""

    filepath = os.path.join(os.path.dirname(__file__),f"../{file}")
    if os.path.exists(filepath):
        filemode = "a+"
    else:
        filemode = "w+"
    
    with open(filepath, filemode) as data_csv:
    
        csv_writer = csv.writer(data_csv, delimiter='\t')
        
        if(filemode == "w+"):
            csv_writer.writerow(["CATEGORY", "DATE", "TWEET"])
        
        for data_point in data:
            #remove non-printable characters
            data_point = ''.join(filter(lambda x: x in string.printable, data_point)).replace('\n', ' ').replace('\r', '').replace('\t', ' ').replace('&gt', ' ').replace('&lt', ' ')
            data_point = re.sub(r'\bhttps://t.co/[^ ]*\b',' ', data_point)
            data_point = ' '.join(data_point.split())
            #append to csv
            csv_writer.writerow([category, date.strftime("%Y-%m-%d"), data_point])

        data_csv.close()

if __name__ == "__main__":

    load_dotenv()
    file = "tweetdataset.csv"
    page_count = 10
      
    bearer_token = os.getenv("TWITTER_API_BEARER_TOKEN")

    url = "https://api.twitter.com/2/tweets/search/recent"

    keywords = {
        "Health": ["vaccine", "hospital", "pharmaceuticals"],
        "Finance": ["share", "stock price","bank"],
        "EVs": ["tesla motors", "ather", "EVs"],
        "Telecom": ["5G", "Jio", "Airtel"],
        "Tech": ["Apple", "Facebook", "Google", "Amazon"]
    }
    
    
    next_date = datetime.datetime(2023, 2, 3)
    
    end_date = datetime.datetime(2023, 2, 5)
    
    query_params = {}
    query_params['tweet.fields'] = 'created_at,lang,source'
    
    while(next_date < end_date):
        
        query_params['start_time'] = f'{next_date.strftime("%Y-%m-%d")}T00:00:00Z'
        query_params['end_time'] = f'{next_date.strftime("%Y-%m-%d")}T23:59:59Z'
        print(query_params)

        print(f"Fetching data for {next_date.strftime('%Y-%m-%d')}")
        
        for category in keywords.keys():
            responses = set()
            keywords_list = keywords[category]

            print(f'\tCollecting {category} tweets')
            
            for keyword in keywords_list:
                print(f'\t\tCollecting {keyword} tweets')

                query_params['query'] = f"( -is:retweet #{keyword} -has:links)"
                    
                response = get_tweet_data(query_params, page_count)
                
                for resp in response:   # to prevent duplicates
                    responses.add(resp)
                
                write_to_csv(responses, file, category, next_date)
            print()
                
        next_date = next_date + datetime.timedelta(days=1)

