import csv
import sqlite3
import os

# Connecting to the FYP database
connection = sqlite3.connect(os.path.join(os.path.dirname(__file__), f'fypdbmain.sqlite'))

# Creating a cursor object to execute SQL queries on a database table
cursor = connection.cursor()

FILENAME1 = "Health_tweetdata.csv"
FILENAME2 = "Health_redditdata.csv"
TABLENAME = "healthdata"

# Table Definition
create_table = f'''CREATE TABLE IF NOT EXISTS {TABLENAME}(
				category TEXT,
				date DATE,
				title TEXT NOT NULL,
        count INTEGER,
				CONSTRAINT uniq_twt PRIMARY KEY (category, date, title)
       			);
				'''
# Creating the table into our
# database
cursor.execute(create_table)

file = open(os.path.join(os.path.dirname(__file__),f"../New Datasets/{FILENAME1}"))
# Reading the contents of the NewTweetData.csv file
tweetcontents = csv.reader(file, delimiter='\t')
file = open(os.path.join(os.path.dirname(__file__),f"../New Datasets/{FILENAME2}"))
redditcontents = csv.reader(file, delimiter='\t')
# SQL query to insert data into the tweets table
try:
	insert_records = f"INSERT INTO {TABLENAME} (category, date,title,count) VALUES(?, ?, ?, ?) ON CONFLICT(category, date, title) DO NOTHING"
 	# Importing the contents of the file into our tweets table
	cursor.executemany(insert_records, tweetcontents)
except sqlite3.Error as error:
    print({error})
    pass
try:
	insert_records = f"INSERT INTO {TABLENAME} (category, date,title,count) VALUES(?, ?, ?, ?) ON CONFLICT(category, date, title) DO NOTHING"
 	# Importing the contents of the file into our tweets table
	cursor.executemany(insert_records, redditcontents)
except sqlite3.Error as error:
    print({error})
    pass
try:
	delete_header = "DELETE from stockData where category = 'CATEGORY'"
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

# closing the database connection
connection.close()