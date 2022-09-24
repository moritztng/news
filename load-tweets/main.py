import tweepy
from google.cloud import bigquery
from os import environ
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

tweepy_client = tweepy.Client(environ['BEARER_TOKEN'])

bigquery_client = bigquery.Client()

def load_tweets(event_data, context):
    tweets = tweepy.Paginator(tweepy_client.search_recent_tweets,
                              event_data['attributes']['query'],
                              start_time=datetime.now() - timedelta(days=1), 
                              tweet_fields=None, 
                              user_fields=None, 
                              max_results=100).flatten()
    bigquery_client.insert_rows_json('tweets.tweets', [tweet.data for tweet in tweets])
    return ''
