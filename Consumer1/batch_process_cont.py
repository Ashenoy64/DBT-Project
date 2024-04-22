from kafka import KafkaConsumer
import sqlite3
from json import loads
from time import time, sleep
from consumer import consumer  # Importing consumer module for sentiment analysis
from sys import argv

DB_CON = None

def init_db():
    global DB_CON
    DB_CON = sqlite3.connect("app.db")
    print("[LOG] Connected to database")

def cleanup():
    global DB_CON
    print("[LOG] Cleaning up...")
    DB_CON.close()
    print("[LOG] Goodbye!")

def get_all_data():
    global DB_CON
    cur = DB_CON.cursor()
    q = """
        SELECT * FROM reddit_post;
    """
    cur.execute(q)
    try:
        records = cur.fetchall()
    except Exception as e:
        print("[ERROR] Error fetching batch data from db:", e)
        records = None
        
    cur.close()
    return records

def process_batch(delay):
    while True:
        records = get_all_data()
        if not records:
            print("[LOG] No more records to process. Exiting.")
            # break
        
        start = time()
        # Run sentiment analysis on this batch
        for i in records:
            print("[LOG] Sentiment =>", i[0], ":", consumer.analyzer_function(i[2]))
        
        end = time()
        print(f"[LOG] Elapsed time: {end - start} seconds")

        truncate_table()
        # Add a delay before processing the next batch
        sleep(delay)  # Adjust the delay as needed

        # Truncate the table after processing

def truncate_table():
    global DB_CON
    cur = DB_CON.cursor()
    q = """
        DELETE FROM reddit_post;
    """
    cur.execute(q)
    DB_CON.commit()
    print("[LOG] Table truncated.")


def main():
    init_db()
    try:
        delay = int(argv[1])
        process_batch(delay)

    except KeyboardInterrupt:
        cleanup()


if __name__ == "__main__":
    main()