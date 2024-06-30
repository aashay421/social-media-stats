import tweepy
import os
import yaml
import dateutil.parser as dateparser
import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

def initialize_twitter_client(api_key, api_key_secret, bearer_token, access_token, access_token_secret, username):
    client = tweepy.Client(bearer_token=bearer_token, consumer_key=api_key, consumer_secret=api_key_secret, access_token=access_token, access_token_secret=access_token_secret, return_type = dict)
    return client


def get_twitter_profile_id(client, username):
    user = client.get_user(username=username)
    id = user.get('data').get('id')
    return id

def get_tweets_data(client, id):
    tweets = client.get_users_tweets(id = twitter_profile_id, max_results=100)
    all_tweets = tweets.get('data')
    tweet_ids = []
    tweets_data = []
    for tweet in all_tweets:
        tweet_ids.append(tweet.get('id'))
    
    for id in tweet_ids:
        data = client.get_tweet(id=id, tweet_fields=['public_metrics','created_at'], media_fields=['type', 'public_metrics'], expansions='attachments.media_keys')
        media_type = "post"
        media_views = 0
        if 'includes' in data:
            if data.get('includes').get('media')[0].get('type') != 'video':
                temp_dict = {'tweet_id': id, 'tweet_caption':data.get('data').get('text'), 'tweet_created_at':dateparser.parse(data.get('data').get('created_at')), 'like_count':data.get('data').get('public_metrics').get('like_count'), 'retweet_count':data.get('data').get('public_metrics').get('retweet_count'), 'reply_count':data.get('data').get('public_metrics').get('reply_count'), 'quote_count':data.get('data').get('public_metrics').get('quote_count'), 
                'media_type':data.get('includes').get('media')[0].get('type'), 'media_views':media_views}
            else:
                temp_dict = {'tweet_id': id, 'tweet_caption':data.get('data').get('text'), 'tweet_created_at':dateparser.parse(data.get('data').get('created_at')), 'like_count':data.get('data').get('public_metrics').get('like_count'), 'retweet_count':data.get('data').get('public_metrics').get('retweet_count'), 'reply_count':data.get('data').get('public_metrics').get('reply_count'), 'quote_count':data.get('data').get('public_metrics').get('quote_count'), 
                'media_type':data.get('includes').get('media')[0].get('type'), 'media_views':data.get('includes').get('media')[0].get('public_metrics').get('view_count')}
        else:
            temp_dict = {'tweet_id': id, 'tweet_caption':data.get('data').get('text'), 'tweet_created_at':dateparser.parse(data.get('data').get('created_at')), 'like_count':data.get('data').get('public_metrics').get('like_count'), 'retweet_count':data.get('data').get('public_metrics').get('retweet_count'), 'reply_count':data.get('data').get('public_metrics').get('reply_count'), 'quote_count':data.get('data').get('public_metrics').get('quote_count'), 
                'media_type':media_type, 'media_views':media_views}
    
        tweets_data.append(temp_dict)
    
    return tweet_ids, tweets_data


def initialize_snowflake_connection(username, password, account):
    conn = snow.connect(
    user=username,
    password=password,
    account=account
    )
    cs = conn.cursor()
    return cs, conn

def upload_data(profile_data, tweets_data, mentions_data, cursor, conn):
    sql = "USE ROLE SYSADMIN"
    cursor.execute(sql)


    sql = """CREATE WAREHOUSE IF NOT EXISTS test 
             WITH WAREHOUSE_SIZE = XSMALL"""
    cursor.execute(sql)


    sql = "USE WAREHOUSE test"
    cursor.execute(sql)

    sql = "CREATE DATABASE IF NOT EXISTS test_db"
    cursor.execute(sql)

    sql = "USE DATABASE test_db"
    cursor.execute(sql)

    sql = "CREATE SCHEMA IF NOT EXISTS test_schema"
    cursor.execute(sql)

    sql = "USE SCHEMA test_schema"
    cursor.execute(sql)

    profile_data_sql = """CREATE OR REPLACE TABLE twitter_user_stats (id VARCHAR(20), name VARCHAR(200), description VARCHAR(300), created_at DATETIME,
    username VARCHAR(20), tweet_count VARCHAR(20), followers_count VARCHAR(20), following_count VARCHAR(20), listed_count VARCHAR(20), verified VARCHAR(10))"""
    cursor.execute(profile_data_sql)

    success, nchunks, nrows, _ = write_pandas(conn, profile_data, "twitter_user_stats", quote_identifiers=False)
    print("Twitter User Stats Table: ", success, " ",  nchunks, " ", nrows)

    tweets_data_sql = """CREATE OR REPLACE TABLE tweets_data (tweet_id VARCHAR(20), tweet_caption VARCHAR(500), tweet_created_at DATETIME, like_count VARCHAR(20),
    retweet_count VARCHAR(20), reply_count VARCHAR(20), quote_count VARCHAR(20), media_type VARCHAR(20), media_views VARCHAR(10))"""
    cursor.execute(tweets_data_sql)

    success, nchunks, nrows, _ = write_pandas(conn, tweets_data, "tweets_data", quote_identifiers=False)
    print("Tweets Data Table: ", success, " ",  nchunks, " ", nrows)

    mentions_data_sql = """CREATE OR REPLACE TABLE mentions (id VARCHAR(20), text VARCHAR(2000))"""
    cursor.execute(mentions_data_sql)

    success, nchunks, nrows, output = write_pandas(conn, mentions_data, "mentions", quote_identifiers=False)
    print("Mentions: ", success, " ",  nchunks, " ", nrows, " ", output)


def close_snowflake_connection(cursor, conn):
    cursor.close()
    conn.close()


def get_user_profile_data(client, id, username):
    data = client.get_user(username=username, user_fields=['created_at', 'description', 'public_metrics', 'verified'])
    profile_data = []
    profile_data.append({'id':data.get('data').get('id'), 'name':data.get('data').get('name'), 'description':data.get('data').get('description'),
    'created_at':dateparser.parse(data.get('data').get('created_at')), 'username':data.get('data').get('username'), 'tweet_count':data.get('data').get('public_metrics').get('tweet_count'),
    'followers_count':data.get('data').get('public_metrics').get('followers_count'), 'following_count':data.get('data').get('public_metrics').get('following_count'), 
    'listed_count':data.get('data').get('public_metrics').get('listed_count'), 'verified':data.get('data').get('verified')})
    return profile_data

def get_profile_mentions(client, id):
    profile_mentions = client.get_users_mentions(id=id)
    mentions = []
    for mention in profile_mentions.get('data'):
        mentions.append({'id': mention.get('id'), 'text': mention.get('text')})
    
    return mentions


if __name__ == "__main__":


    current_dir = os.path.dirname(os.path.realpath(__file__))
    CONFIG_FILE = os.path.join(current_dir, "config.yaml")

    with open(CONFIG_FILE, "r") as f:
        config = yaml.full_load(f)
    
    API_key = config['twitter']['api_key']
    API_key_secret = config['twitter']['api_key_secret']
    Bearer_token = config['twitter']['bearer_token']
    Access_token = config['twitter']['access_token']
    Access_token_secret = config['twitter']['access_token_secret']
    twitter_username = config['twitter']['username']
    """
    snowflake_username = config['snowflake']['username']
    snowflake_password = config['snowflake']['password']
    snowflake_account = config['snowflake']['account']
    """

    client = initialize_twitter_client(API_key, API_key_secret, Bearer_token, Access_token, Access_token_secret, twitter_username)
    twitter_profile_id = get_twitter_profile_id(client, twitter_username)
    mentions = get_profile_mentions(client, twitter_profile_id)

    profile_data = get_user_profile_data(client, twitter_profile_id, twitter_username)   
    tweet_ids, tweets_data = get_tweets_data(client, twitter_profile_id)

    profile_data_df = pd.DataFrame(profile_data)
    tweets_data_df = pd.DataFrame(tweets_data)
    mentions_data_df = pd.DataFrame(mentions)

    """
    db_cursor, conn = initialize_snowflake_connection(snowflake_username, snowflake_password, snowflake_account)
    upload_data(profile_data_df, tweets_data_df, mentions_data_df, db_cursor, conn)
    close_snowflake_connection(db_cursor, conn)
    """

#    limit = 10
#    api = tweepy.API(tweepy.OAuthHandler(API_key, API_key_secret))
#    tweets = tweepy.Cursor(api.search_tweets, q='#haryana', tweet_mode='extended').items(limit)
#    for tweet in tweets:
#        print(tweet)