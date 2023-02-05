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


    NEWSAPI_API_KEY = os.getenv("NEWSAPI_API_KEY")
    url = "https://newsapi.org/v2/everything?"

    querystring = {"apiKey":NEWSAPI_API_KEY,"q":query,"sort_by":"relevancy"}


    extracted_news = []

    while True:
        req = requests.get(url, querystring)
        response = req.json()
        tot_response = min(100,response['totalResults'])
        for i in range(tot_response):
            extracted_news.append(response['articles'][i]["title"])
        else:
            break
    
    # print(extracted_news)
    return extracted_news


def write_to_csv(data, category = "None"):
    """Writes the content with category to csv file to make the dataset."""

    print("\n  ***Currently writing the '{}' category***".format(category))

    with open(os.path.join(os.path.dirname(__file__),"../newsdataset.csv"), "a") as data_csv:
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

    for category in keywords.keys():
        responses = set()
        extra_params = {}

        print("\n***Currently parsing the '{}' category***".format(category))
        keywords_list = keywords[category]

        for keyword in keywords_list:
            print("  ***Currently parsing the '{}' keyword***".format(keyword))
            response = get_current_news_data(keyword, extra_params)

            for resp in response:   # To prevent duplicates
                responses.add(resp)

        write_to_csv(responses, category)

    print("Finished collecting data from the API.")