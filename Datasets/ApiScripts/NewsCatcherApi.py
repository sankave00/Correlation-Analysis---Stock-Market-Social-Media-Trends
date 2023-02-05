import os
import csv
import string
from time import sleep
import requests
from urllib import response
from pprint import PrettyPrinter
from dotenv import load_dotenv

def get_current_news_data(query, extra_params):
    """Returns the current news data fetched from NewsCatcher API."""

    NEWSCATCHER_API_KEY = os.getenv("NEWSCATCHER_API_KEY")

    url = "https://api.newscatcherapi.com/v2/search"

    querystring = {"q":query,"lang":"en","sort_by":"relevancy","page":"1"}

    headers = {
        "x-api-key": NEWSCATCHER_API_KEY
        }

    extracted_news = []
    page = 1
    total_pages = 0

    response = requests.request("GET", url, headers=headers, params=querystring)

    if response.json().get("total_pages", None):
        total_pages = response.json()["total_pages"]


    print("   ",query,":- Total pages- ",total_pages)

    while True:
        querystring["page"] = str(page)
        response = requests.request("GET", url, headers=headers, params=querystring)
        response = response.json()


        if(page > min(total_pages,2)):
            break

        print("      Page- ",page)
        page += 1

        if response.get("articles", None):
            for result in response["articles"]:
                extracted_news.append(result["title"])
        else:
            break

        sleep(1)


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


    print("Started collecting data from the API")

    for category in keywords.keys():
        responses = set()
        extra_params = {}

        if category == "Tech":  #to restrict tech news to only tech articles
            extra_params["category"] = "technology"

        print("\n***Currently parsing the '{}' category***".format(category))
        keywords_list = keywords[category]

        for keyword in keywords_list:
            print("  ---Currently parsing the '{}' keyword...---".format(keyword))
            response = get_current_news_data(keyword, extra_params)

            for resp in response:   # to prevent duplicates
                responses.add(resp)

        write_to_csv(responses, category)

    print("Finished collecting data from the API.")