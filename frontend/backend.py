import os
from dotenv import load_dotenv



def start_producers(subredits:list, sort:str, keywords:list):

    for subredit in subredits:
        print("running",subredit)
        # os.system("docker run -d  --network=scrape-net -e SUBREDDIT={} -e KAFKA_SERVER={} reddit-producer ".format(subredit))
        os.system("docker run -d --network=scrape-net -e SUBREDDIT={} -e KAFKA_SERVER={} -e CLIENT_ID={} -e USER_AGENT={} -e INTERVAL={} reddit-producer ".format(subredit,os.getenv('KAFKA_CLUSTER_BOOTSTRAP_SERVERS'),os.getenv('REDDIT_CLIENT_ID'),os.getenv('REDDIT_USER_AGENT'),os.getenv('INTERVAL')))

    os.system("docker-compose -f ../consumer/docker-compose.yaml up -d")

if __name__=="__main__":
    load_dotenv()
    start_producers()
