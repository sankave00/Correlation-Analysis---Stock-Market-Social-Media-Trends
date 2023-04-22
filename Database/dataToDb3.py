import csv
import sqlite3
import os
import pandas as pd

# Connecting to the FYP database
connection = sqlite3.connect(os.path.join(os.path.dirname(__file__), f'fypdbmain.sqlite'))

# Creating a cursor object to execute SQL queries on a database table
cursor = connection.cursor()

FILENAME1 = "FinanceAggrFinal.csv"
TABLENAME = "financeDataAggr"

# Table Definition
#category	date	neg_count	neu_count	pos_count	neg_score	neu_score	pos_score	wt_neg	wt_neu	wt_pos	count
create_table = f'''CREATE TABLE IF NOT EXISTS {TABLENAME}(
				category TEXT,
				date DATE,
        neg_count	INT,
        neu_count	INT,
        pos_count	INT,
        neg_score	REAL,
        neu_score	REAL,
        pos_score	REAL,
        wt_neg	REAL,
        wt_neu	REAL,
        wt_pos	REAL,
        count INT);
				'''
# Creating the table into our
# database
cursor.execute(create_table)

file = open(os.path.join(os.path.dirname(__file__),f"../New Datasets/{FILENAME1}"))
# Reading the contents of the NewTweetData.csv file
tweetcontents = csv.reader(file, delimiter='\t')

# SQL query to insert data into the tweets table
try:
	insert_records = f"INSERT INTO {TABLENAME} VALUES(?, ?, ?, ?,?, ?, ?, ?,?, ?, ?, ?)"
 	# Importing the contents of the file into our tweets table
	cursor.executemany(insert_records, tweetcontents)
except sqlite3.Error as error:
    print({error})
    pass

try:
	delete_header = f"DELETE from {TABLENAME} where category = 'category'"
	cursor.execute(delete_header)
except sqlite3.Error as error:
    print({error})
    pass


# SQL query to retrieve all data from the tweets table to verify that the
# data of the csv file has been successfully inserted into the table
select_all = f"SELECT * FROM {TABLENAME}"
rows = cursor.execute(select_all).fetchall()

# Output to the console screen
print({"Rows written" : len(rows)})

# Committing the changes
connection.commit()

# # closing the database connection
# connection.close()