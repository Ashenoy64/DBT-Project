from confluent_kafka import Producer
import praw
import os
from dotenv import load_dotenv

load_dotenv()



KAFAKA_SERVER = os.getenv('KAFKA_CLUSTER_BOOTSTRAP_SERVERS')
CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
USER_AGENT = os.getenv('REDDIT_USER_AGENT')    





"""
reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT)
HOT_POSTS = reddit.subreddit("indian").hot()

for post in HOT_POSTS:
    print(post.title)
    print(post.selftext)
    print(post.url)
    print(post.score)
    print(post.id)
    print(post.subreddit)
    print(post.created_utc)
    print(post.author)
    print(post.comments)
    print(post.permalink)
    print(post.upvote_ratio)
    print(post.ups)
    print(post.downs)
    print(post.num_comments)
    print(post)
    break
"""


producer = Producer({'bootstrap.servers': "100.88.89.141:9092,100.88.89.141:9093,100.88.89.141:9094"})
producer.produce('reddit', 'Hello, Kafka!'.encode('utf-8'))
producer.flush()


"""
producer = KafkaProducer(bootstrap_servers=KAFAKA_SERVER.split(','))

producer.send('reddit', b'Hello, Kafka!')
producer.flush()
"""
