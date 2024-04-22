from configparser import ConfigParser
import praw
from time import sleep
from json import dumps
from kafka import KafkaProducer
import json
from sys import argv
import threading

# Read configuration from config file
configParser = ConfigParser()
configParser.read("config")
CLIENT_ID = configParser["DATA_PRODUCER"].get("client_id")
CLIENT_SECRET = configParser["DATA_PRODUCER"].get("client_secret")
USER_AGENT = configParser["DATA_PRODUCER"].get("user_agent")

# Initialize Reddit instance
reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT)

# Specify Kafka broker and topic
kafka_broker = 'localhost:9092'
kafka_topic = argv[1]

# Set batch size
interval = 50


# Initialize Kafka producer
my_producer = KafkaProducer(bootstrap_servers=kafka_broker)

# Function to send data to Kafka in batches
def myPeriodicFunction(posts):
    global kafka_topic
    print(f"[LOG] SENDING DATA to BROKER from subreddit=r/{kafka_topic}!")

    try:
        for post in posts:
            my_producer.send(kafka_topic, json.dumps(post).encode('utf-8'))
            sleep(1)
        my_producer.flush()
    except:
        pass

# Function to periodically send posts
def startTimer():
    global interval
    threading.Timer(5, startTimer).start()  # Send posts every 5 seconds

    # Fetch new posts
    NEW_POSTS = reddit.subreddit(argv[1]).new()

    # Prepare data for Kafka
    l = []
    for x in NEW_POSTS:
        try:
            l.append({
                "id": str(x.id),
                "selftext": str(x.selftext),
                "title": str(x.title),
                "created": str(x.created_utc)
            })
        except:
            pass
    # print(len(l))

    # Split posts into batches and send them
    for i in range(len(l)-100, len(l), interval):
        myPeriodicFunction(l[i:i+interval])

# Start sending posts
startTimer()



'''
['STR_FIELD', '__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattr__', 
'__getattribute__', '__gt__', '__hash__', '__init__', '__init_subclass__', '__le__', '__lt__', '__module__', '__ne__', '__new__', 
'__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_chunk', 
'_comments_by_id', '_fetch', '_fetch_data', '_fetch_info', '_fetched', '_kind', '_reddit', '_reset_attributes', '_safely_add_arguments', 
'_url_parts', '_vote', 'all_awardings', 'allow_live_comments', 'approved_at_utc', 'approved_by', 'archived', 'author', 
'author_flair_background_color', 'author_flair_css_class', 'author_flair_richtext', 'author_flair_template_id', 'author_flair_text', 
'author_flair_text_color', 'author_flair_type', 'author_fullname', 'author_is_blocked', 'author_patreon_flair', 'author_premium', 'award', 
'awarders', 'banned_at_utc', 'banned_by', 'can_gild', 'can_mod_post', 'category', 'clear_vote', 'clicked', 'comment_limit', 'comment_sort', 
'comments', 'content_categories', 'contest_mode', 'created', 'created_utc', 'crosspost', 'delete', 'disable_inbox_replies', 'discussion_type', 
'distinguished', 'domain', 'downs', 'downvote', 'duplicates', 'edit', 'edited', 'enable_inbox_replies', 'flair', 'fullname', 'gild', 'gilded', 
'gildings', 'hidden', 'hide', 'hide_score', 'id', 'id_from_url', 'is_created_from_ads_ui', 'is_crosspostable', 'is_meta', 'is_original_content', 
'is_reddit_media_domain', 'is_robot_indexable', 'is_self', 'is_video', 'likes', 'link_flair_background_color', 'link_flair_css_class', 
'link_flair_richtext', 'link_flair_text', 'link_flair_text_color', 'link_flair_type', 'locked', 'mark_visited', 'media', 'media_embed', 
'media_only', 'mod', 'mod_note', 'mod_reason_by', 'mod_reason_title', 'mod_reports', 'name', 'no_follow', 'num_comments', 'num_crossposts', 
'num_reports', 'over_18', 'parent_whitelist_status', 'parse', 'permalink', 'pinned', 'pwls', 'quarantine', 'removal_reason', 'removed_by', 
'removed_by_category', 'reply', 'report', 'report_reasons', 'save', 'saved', 'score', 'secure_media', 'secure_media_embed', 'selftext', 
'selftext_html', 'send_replies', 'shortlink', 'spoiler', 'stickied', 'subreddit', 'subreddit_id', 'subreddit_name_prefixed', 
'subreddit_subscribers', 'subreddit_type', 'suggested_sort', 'thumbnail', 'thumbnail_height', 'thumbnail_width', 'title', 'top_awarded_type', 
'total_awards_received', 'treatment_tags', 'unhide', 'unsave', 'ups', 'upvote', 'upvote_ratio', 'url', 'user_reports', 'view_count', 'visited', 
'whitelist_status', 'wls']
'''
