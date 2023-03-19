import requests
import os
from dotenv import load_dotenv
import csv
import string
import requests
from urllib import response
import re

import requests
import os
import json

def setHeaders():
    CLIENT_ID = os.getenv('REDDIT_PERSONAL_SCRIPT')
    SECRET_TOKEN = os.getenv('REDDIT_SECRET_KEY')
    PW =os.getenv('REDDIT_PW')
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, SECRET_TOKEN)

    # here we pass our login method (password), username, and password
    data = {'grant_type': 'password',
            'username': 'san_kave',
            'password': PW}

    # setup our header info, which gives reddit a brief description of our app
    headers = {'User-Agent': 'Myapp'}

    # send our request for an OAuth token
    res = requests.post('https://www.reddit.com/api/v1/access_token',
                        auth=auth, data=data, headers=headers)

    TOKEN = res.json()['access_token']
    print('Token generated')

    # add authorization to our headers dictionary
    headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}
    return headers

def write_to_csv(data, file="redditdataset.csv", category = "None"):
    """Writes the content with category to csv file to make the dataset."""

    filepath = os.path.join(os.path.dirname(__file__),f"../{file}")
    if os.path.exists(filepath):
        filemode = "a+"
    else:
        filemode = "w+"
    
    with open(filepath, filemode) as data_csv:
    
        csv_writer = csv.writer(data_csv, delimiter='\t')
        
        if(filemode == "w+"):
            csv_writer.writerow(["CATEGORY", "POST"])
        
        for data_point in data:
            #remove non-printable characters
            data_point = ''.join(filter(lambda x: x in string.printable, data_point)).replace('\n', ' ').replace('\r', '').replace('\t', ' ').replace('&gt', ' ').replace('&lt', ' ')
            data_point = re.sub(r'\bhttps://t.co/[^ ]*\b',' ', data_point)
            data_point = ' '.join(data_point.split())
            #append to csv
            csv_writer.writerow([category, data_point])

        data_csv.close()



if __name__ == "__main__":

    load_dotenv()
    file = "redditdataset.csv"
    page_count = 10

    headers = setHeaders()

    keywords = {
        "Health": ["vaccine", "hospital", "pharmaceuticals"],
        "Finance": ["share", "stock price","bank"],
        "EVs": ["tesla motors", "ather", "EVs"],
        "Telecom": ["5G", "Jio", "Airtel"],
        "Tech": ["Apple", "Facebook", "Google", "Amazon"]
    }

    keywords = {
        "Health": ["vaccine"]
    }

    for category in keywords.keys():
        responses = set()
        keywords_list = keywords[category]

        print(f'\tCollecting {category} Reddit posts')
        
        for keyword in keywords_list:
            print(f'\t\tCollecting {keyword} Reddit posts')

            params = {
                'limit' : 20,
                'q' : keyword,
                'sort': 'hot'
            }
            res = requests.get("https://oauth.reddit.com/r/subreddit/search",
                        headers=headers,params=params)
            cnt = 0
            for post in res.json()['data']['children']:
                if(cnt==0):
                    print(post['data'])
                    responses.add(post['data']['title'])
                    cnt = cnt + 1
               
        write_to_csv(responses, file, category)
        print()