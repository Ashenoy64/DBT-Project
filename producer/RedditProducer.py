# from confluent_kafka import Producer
from kafka import KafkaProducer
import praw
import os
from dotenv import load_dotenv
import time
import json
import tqdm

load_dotenv()



KAFAKA_SERVER = os.getenv('KAFKA_CLUSTER_BOOTSTRAP_SERVERS')
CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
USER_AGENT = os.getenv('REDDIT_USER_AGENT')
SUBREDDIT = os.getenv('REDDIT_SUBREDDIT')
INTERVAL = os.getenv('INTERVAL')


print(KAFAKA_SERVER)
POST_DATA = []

def get_data():
    global POST_DATA
    reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT)
    HOT_POSTS = reddit.subreddit(SUBREDDIT).hot()
    for post in tqdm.tqdm(list(HOT_POSTS),total=len(list(HOT_POSTS)),desc="Getting Data"):
        _comments =[]
        for comment in post.comments:
            comment_data = {
                "body": comment.body,
                "score": comment.score,
                "created_utc": comment.created_utc,
                "id": comment.id,
                "permalink": comment.permalink,
                "ups": comment.ups,
                "downs": comment.downs,
                "author": comment.author.name if comment.author else "Deleted",

            }
            _comments.append(comment_data)
        post_data = {
            "title": post.title,
            "selftext": "Hello world this is a test post",
            "url": post.url,
            "score": post.score,
            "authorName": post.author.name,
            "id": post.id,
            "created_utc": post.created_utc,
            "permalink": post.permalink,
            "ups": post.ups,
            "downs": post.downs,
            "num_comments": post.num_comments,
            "comments": _comments,
        }
        POST_DATA.append(post_data)

    
def start_streaming():
    global POST_DATA
    producer = KafkaProducer(bootstrap_servers=KAFAKA_SERVER.split(","),value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    print("Starting Streaming")
    for i in range(len(POST_DATA)):
        time.sleep(int(INTERVAL))
        print("Posting data to Kafka")
        producer.send("reddit",POST_DATA[i])
        print("Data Posted")

if __name__ == "__main__":
    get_data()
    print("Done Getting Data")
    start_streaming()
    pass


