import os
import datetime as dt
from datetime import date, datetime
import json
import csv
import pandas as pd
import numpy as np

def write_to_csv(data, file, category="None", date=datetime.datetime.now()):
    """Writes the content with category to csv file to make the dataset."""

    filepath = os.path.join(os.path.dirname(__file__), f"../New Datasets/{category+file}")
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

file = '_dataset.csv'
categories = ["Health","Finance","EVs","Tech"]

for category in categories:
    filepath1 = os.path.join(os.path.dirname(__file__),f"../New Datasets/{category}_tweetdata.csv")
    filepath2 = os.path.join(os.path.dirname(__file__),f"../New Datasets/{category}_redditdata.csv")