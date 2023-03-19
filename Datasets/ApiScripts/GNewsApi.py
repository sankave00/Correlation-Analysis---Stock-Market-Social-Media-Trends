import os
import csv
import string
from time import sleep
import requests
from urllib import response
from pprint import PrettyPrinter
from dotenv import load_dotenv

def get_current_news_data(query, extra_params):
    """Returns the current news data fetched from NewsAPI."""


    GNEWSAPI_API_KEY = os.getenv("GNEWS_API_KEY")
    print(GNEWSAPI_API_KEY)
    url = "https://gnews.io/api/v4/search?"

    querystring = {"apikey":GNEWSAPI_API_KEY,"q":query,"from":"2021-03-04", "to" : "2021-03-06", "sort_by":"relevance", "lang":"en"}


    extracted_news = []

    while True:
        req = requests.get(url, querystring)
        response = req.json()
        tot_response = min(100,response['totalArticles'])
        print(response['totalArticles'])
        cnt = 0
        for i in response['articles']:
            cnt = cnt +1
            print(i)
            extracted_news.append(i["title"])
        else:
            print(cnt)
            break
    
    # print(extracted_news)
    return extracted_news


def write_to_csv(data, category = "None"):
    """Writes the content with category to csv file to make the dataset."""

    print("\n  ***Currently writing the '{}' category***".format(category))

    with open(os.path.join(os.path.dirname(__file__),"../news_dataset.csv"), "a") as data_csv:
        csv_writer = csv.writer(data_csv)

        for data_point in data:
            #remove non-printable characters
            data_point = ''.join(filter(lambda x: x in string.printable, data_point))
            #append to csv
            csv_writer.writerow([category, data_point])

        data_csv.close()

    print("\n  ***Done writing '{}' category***".format(category))


if __name__ == "__main__":

    load_dotenv()

    keywords = {
        "Health": ["vaccine", "hospital", "pharmaceuticals"],
        "Finance": ["share", "stock price","bank"],
        "EVs": ["tesla motors", "ather", "EVs"],
        "Telecom": ["5G", "Jio", "Airtel"],
        "Tech": ["Apple", "Facebook", "Google", "Amazon"]
    }


    print("Started collecting data from the APIs...")

    response = get_current_news_data("vaccine",extra_params={})
    write_to_csv(response, "Health")


    # for category in keywords.keys():
    #     responses = set()
    #     extra_params = {}

    #     print("\n***Currently parsing the '{}' category***".format(category))
    #     keywords_list = keywords[category]

    #     for keyword in keywords_list:
    #         print("  ***Currently parsing the '{}' keyword***".format(keyword))
    #         response = get_current_news_data(keyword, extra_params)

    #         for resp in response:   # To prevent duplicates
    #             responses.add(resp)

    #     write_to_csv(responses, category)

    print("Finished collecting data from the API.")