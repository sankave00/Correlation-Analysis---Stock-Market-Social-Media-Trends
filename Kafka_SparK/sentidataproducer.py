import pprint
import sqlite3
import json
import time
from kafka import KafkaProducer
import os

TABLENAME = "financedata"
d="date"
def json_serializer(data):
    """Returns a JSON serialized dump of the given data."""

    return json.dumps(data).encode("utf-8")


if __name__ == "__main__":
    batch_size, timeout = 10000, 1

    print("-----Tweet Data Producer Stream-----")

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=json_serializer
    )

    print("Kafka Producer started.")

    try:
        conn = sqlite3.connect(os.path.join(os.path.dirname(__file__),f"../Database/fypdbmain.sqlite"))
        print("Connected to FYPDB Database.")
        cursor = conn.cursor()
        query = f"SELECT * FROM {TABLENAME} ORDER BY {d}"

        cursor.execute(query)

        while True:
            records = cursor.fetchmany(batch_size)

            if not records: #no more tweets to read
                break

            for record in records:
                data = dict()
                data['category'] = record[0]
                data['date'] = record[1]
                data['title'] = record[2]
                data['count'] = record[3]
                
                # record = ','.join(str(x) for x in record)
                producer.send("financetopicV6", json.dumps(data)) #topic: "techtopicV1"
                #print(record)
                # producer.send("tweets", json.dumps(data)) #topic: "tweets"
                pprint.pprint(json.dumps(data))
                
            time.sleep(timeout)  #wait for timeout before next send

        cursor.close()

    
    except sqlite3.Error as error:
        print("Failed to connect to FYPDB Database.")

    
    finally:
        if conn:
            time.sleep(10)
            conn.close()
            print("Disconnected from FYPDB Database.")