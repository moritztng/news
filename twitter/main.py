import tweepy
from google.cloud import bigquery
from os import environ
from dotenv import load_dotenv

load_dotenv()

bigquery_client = bigquery.Client()
query = f"""
    SELECT owner, twitter, name, stargazerCount,
    stargazerCount - LAG(stargazerCount) OVER (PARTITION BY owner, name ORDER BY time) AS stargazerGrowth
    FROM `github_repositories.repositories`
    WHERE time > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 2 DAY)
    ORDER BY stargazerGrowth DESC
    LIMIT {environ['NUMBER_REPOSITORIES']}
"""

tweepy_client = tweepy.Client(
    consumer_key=environ['CONSUMER_KEY'],
    consumer_secret=environ['CONSUMER_SECRET'],
    access_token=environ['ACCESS_TOKEN'],
    access_token_secret=environ['ACCESS_TOKEN_SECRET'],
)

def tweet(event_data, context):
    repositories = bigquery_client.query(query)
    tweet_id = None
    for repository in repositories:
        twitter_username_text = '@' + repository['twitter'] + '\n' if repository['twitter'] else '' 
        tweet_text = (
            f"{repository['owner']}/{repository['name']} 🎉\n"
            f"{repository['stargazerGrowth']} 🌟 today\n"
            f"{repository['stargazerCount']} 🌟 total\n"
            f"{twitter_username_text}"
            f"https://github.com/{repository['owner']}/{repository['name']}"
        )
        tweet_id = tweepy_client.create_tweet(text=tweet_text, in_reply_to_tweet_id=tweet_id).data['id']
    return ''
