import csv
import sqlite3
import os

# Connecting to the FYP database
connection = sqlite3.connect(os.path.join(os.path.dirname(__file__), f'fypdbmain.sqlite'))

# Creating a cursor object to execute SQL queries on a database table
cursor = connection.cursor()

FILENAME = "output.csv"
TABLENAME = "stockDataAggr"

# Table Definition
create_table = f'''CREATE TABLE IF NOT EXISTS {TABLENAME}(
				category TEXT,
				ticker TEXT,
				stockDate DATE,
				open REAL,
				close REAL,
				high REAL,
				low REAL,
        aggrPercent REAL);
				'''
# Creating the table into our
# database
cursor.execute(create_table)

file = open(os.path.join(os.path.dirname(__file__),f"../New Datasets/{FILENAME}"))
# Reading the contents of the NewTweetData.csv file
contents = csv.reader(file, delimiter='\t')
# SQL query to insert data into the tweets table
try:
	insert_records = f"INSERT INTO {TABLENAME} (category, ticker, stockDate, open, close, high, low, aggrPercent) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
 	# Importing the contents of the file into our tweets table
	cursor.executemany(insert_records, contents)
except sqlite3.Error as error:
    print({error})
    pass
try:
	delete_header = f"DELETE from {TABLENAME} where category = 'Category'"
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