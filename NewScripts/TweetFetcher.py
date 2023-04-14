import re
from urllib import response
from numpy import extract
import requests
import os
from dotenv import load_dotenv
import csv
import string
import requests
import datetime
from time import sleep, time

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields

bearer_token = None
search_url = None
countApiCalls = 0
start_time = time()


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FullArchiveSearchPython"
    return r


def connect_to_endpoint(url, params):
    response = requests.request(
        "GET", search_url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def check_api_call_count():
    global countApiCalls, start_time
    if countApiCalls >= 1:
        sleep_time = max(1, 3 - (time() - start_time))

        print("\t\t\t  -- Sleeping for 3 secs")
        sleep(sleep_time)
        countApiCalls = 0
        start_time = time()


def get_tweet_data(query_params, page_count=5):
    global countApiCalls
    extracted_tweets = []

    check_api_call_count()

    json_response = connect_to_endpoint(search_url, query_params)
    countApiCalls += 1

    while page_count:
        page_count -= 1

        if 'data' in json_response:
            for tweet in json_response['data']:
                if tweet['lang'] == 'en':
                    extracted_tweets.append(
                        (tweet['text'], tweet['public_metrics']['retweet_count'] + 1))

        if 'meta' in json_response:
            if 'next_token' in json_response['meta']:
                query_params['next_token'] = json_response['meta']['next_token']

                check_api_call_count()

                json_response = connect_to_endpoint(search_url, query_params)
                countApiCalls += 1

    print(f'\t\t -- Extracted {len(extracted_tweets)} tweets')
    return extracted_tweets


def write_to_csv(data, file, category="None", date=datetime.datetime.now()):
    """Writes the content with category to csv file to make the dataset."""

    filepath = os.path.join(os.path.dirname(__file__), f"../../{category+file}")
    if os.path.exists(filepath):
        filemode = "a+"
    else:
        filemode = "w+"

    # with open(f"../Datasets/{file}", "a") as data_csv:
    with open(filepath, filemode) as data_csv:

        csv_writer = csv.writer(data_csv, delimiter='\t')

        if(filemode == "w+"):
            # CONT = RT_CONT + 1
            csv_writer.writerow(["CATEGORY", "DATE","TITLE", "COUNT"])

        for data_point, count in data.items():
            # append to csv
            csv_writer.writerow(
                [category, date.strftime("%Y-%m-%d"),data_point, count])

        data_csv.close()


def format_response(data_point):
    # remove non-printable characters
    data_point = ''.join(filter(lambda x: x in string.printable, data_point))

    temp = data_point


    data_point = temp

    data_point = data_point.replace('\n', ' ').replace('\r', '').replace('\t', ' ').replace(
        '&gt', ' > ').replace('&lt', ' < ').replace('&amp', ' & ').replace('&quot', ' ').replace('&apos', ' ')
    data_point = re.sub(r'\bhttps://t.co/[^ ]*\b', ' ', data_point)
    data_point = ' '.join(data_point.split())

    return data_point

def main(file, page_count=5):
    load_dotenv()

    global bearer_token
    bearer_token = os.getenv("TWITTER_API_ACAD_BEARER_TOKEN")
    global search_url
    search_url = "https://api.twitter.com/2/tweets/search/all"

    old_keywords = {
        "Crypto": ["bitcoin", "dogecoin", "crypto"],
        "Oil": ["crude oil", "oil prices"],
        "EVs": ["tesla motors", "electric vehicle", "EVs"],
        "Gaming": ["XBox", "Playstation", "Video Games"],
        "Tech": ["Apple", "Facebook", "Google", "Amazon"]}

    semi_ok_keywords = {
        "Crypto": ["BTC", "ETH"],
        "Oil": ["CVX", "XOM"],  # , "COP"],
        "EVs": ["TSLA", "LCID"],
        "Gaming": ["EA", "ATVI"],
        "Tech": ["AAPL", "GOOGL", "INTC", "FB"]
    }

    used_keywords = {
        "Tech": ["#GOOGL", "#GOOG", "#AAPL", "#INTC", "$GOOGL", "$GOOG", "$AAPL", "$INTC", "#MSFT", "$MSFT"],
        "Gaming": ["$EA", "$ATVI", "#ATVI"],
        "Food": ["$MCD", "$MDLZ", "#PepsiCo", "$PEP"],
        "Pharma": ["$PFE", "$AZN", "$MRNA", "#Moderna", "#Astrazeneca", "#Pfizer"],
        "Oil": ["#XOM #Oil", "#COP #Oil", "#DVN #Oil", "#XOM", "#ENB #Oil", "$XOM", "$COP", "$DVN", "$ENB"]
    }

    keywords = {
        "Health": ["United Health", "hospital","heath insurance","UNH"],
        "Finance": ["share","Morgan Stanley","stock price", "investment" ],
        "EVs": ["tesla","EVs","electric vehicle"],
        "Telecom": ["5G", "broadband", "mobile network","American Tower Corp"],
        "Tech": ["Google","Alphabet"]
    }

    twitter_keywords = {
       "Health": ["United Health" ,"health insurance","#UNH","$UNH"],
        "Finance": ["Morgan Stanley", "investment","#MS", "$MS" ],
        "EVs": ["tesla","EVs","#TSLA", "$TSLA"],
        "Tech": ["Google","#GOOGL","#GOOG","$GOOGL","$GOOG"]
    }

    #next_date = datetime.datetime(2020, 4, 7)
    next_date = datetime.datetime(2022, 6, 6)

    #Uncomment below line to fetch data from another date in case of failure
    #next_date = datetime.datetime(2022, 2, 16)

    #end_date = datetime.datetime(2022, 3, 30)
    end_date = datetime.datetime(2022, 7, 31)

    while(next_date <= end_date):
        # print(type(next_date))
        query_params = {}
        query_params['tweet.fields'] = 'created_at,lang,source,public_metrics'
        query_params['start_time'] = f'{next_date.strftime("%Y-%m-%d")}T00:00:00Z'
        query_params['end_time'] = f'{next_date.strftime("%Y-%m-%d")}T23:59:59Z'
        query_params['max_results'] = 100

        print(f"Fetching data for {next_date.strftime('%Y-%m-%d')}")

        for category in twitter_keywords.keys():

            keywords_list = twitter_keywords[category]

            print(f'\tCollecting {category} tweets')

            for keyword in keywords_list:
                responses = set()
                response_dict = dict()

                print(f'\t\tCollecting {keyword} tweets')

                query_params['query'] = f"( -is:retweet {keyword} -has:links)"
                print(query_params)
                response = get_tweet_data(query_params, page_count)

                for resp in response:  # add to set to prevent duplicates
                    formatted_resp = format_response(resp[0])
                    if formatted_resp not in responses:
                        responses.add(formatted_resp)
                        response_dict[formatted_resp] = resp[1]
                    else:
                        response_dict[formatted_resp] = resp[1]

                write_to_csv(response_dict, file, category, next_date)

            print()

        next_date = next_date + datetime.timedelta(days=1)
        # print(type(next_date))


if __name__ == "__main__":
    filename = "_tweetdataprev.csv"
    page_count = 5
    main(filename, page_count)