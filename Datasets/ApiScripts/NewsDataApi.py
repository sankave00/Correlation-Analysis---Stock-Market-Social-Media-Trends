import os
import csv
import string
import requests
from urllib import response
from pprint import PrettyPrinter
from dotenv import load_dotenv



def get_current_news_data(query):
    """Returns the current news data fetched from NewsData.io API."""

    #Load API keys
    NEWSDATA_API_KEY = os.getenv('NEWSDATA_API_KEY')

    #NewsData.io URL
    url = "https://newsdata.io/api/1/news?apikey=" + NEWSDATA_API_KEY
    params = {"q" : query, "language" : "en"}


    extracted_news = []

    while True:
        req = requests.get(url, params)
        response = req.json()

        for result in response["results"]:
            extracted_news.append(result["title"])

        else:
            break
    
    # print(extracted_news)
    return extracted_news


def write_to_csv(data, category = "None"):
    """Writes the content with category to csv file to make the dataset."""

    with open(os.path.join(os.path.dirname(__file__),"../newsdataset.csv"), "a") as data_csv:

        csv_writer = csv.writer(data_csv)

        for data_point in data:
            #remove non-printable characters
            data_point = ''.join(filter(lambda x: x in string.printable, data_point))
            #append to csv
            csv_writer.writerow([category, data_point])

        data_csv.close()


if __name__ == "__main__":

    load_dotenv()

    keywords = {
        "Health": ["vaccine", "hospital", "pharmaceuticals"],
        "Finance": ["share", "stock price","bank"],
        "EVs": ["tesla motors", "ather", "EVs"],
        "Telecom": ["5G", "Jio", "Airtel"],
        "Tech": ["Apple", "Facebook", "Google", "Amazon"]
    }


    print("Started collecting data from the API")

    for category in keywords.keys():
        responses = set()

        print("\n\t***Currently parsing the '{}' category***".format(category))
        keywords_list = keywords[category]

        for keyword in keywords_list:
            print("\t\t***Currently parsing the '{}' keyword***".format(keyword))
            response = get_current_news_data(keyword)

            for resp in response:   # to prevent duplicates
                responses.add(resp)

        write_to_csv(response,category)

    print("Finished collecting data from the API.")