import json
import facebook
import os
import yaml
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebookads.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
import dateutil.parser as dateparser 

def initialize_facebook_graph(access_token):
    graph = facebook.GraphAPI(access_token)
    return graph

def get_facebook_instagram_id(graph):
    profile = graph.get_object('me', fields='id, name, accounts{instagram_business_account}')
    profile_dump = json.dumps(profile, indent=4)
    profile_load = json.loads(profile_dump)
    facebook_id = profile_load['id']
    instagram_id = profile_load['accounts']['data'][0]['instagram_business_account']['id']
    return facebook_id, instagram_id

def get_instagram_data(graph, instagram_id):
    media = graph.get_connections(id=instagram_id, connection_name='media', fields='caption, comments_count, like_count, media_type, username, media_product_type, timestamp')
    personal_data = graph.get_object(id=instagram_id, fields ='followers_count, follows_count, media_count, name, username')
    instagram_user_stats = []
    instagram_stats_temp = {'profile_id':personal_data.get('id'), 'media_count':personal_data.get('media_count'),'followers_count':personal_data.get('followers_count'), 'follows_count':personal_data.get('follows_count')}
    instagram_user_stats.append(instagram_stats_temp)
    temp = []
    for data in media['data']:
        dict = {'media_id':data.get('id'), 'profile_id':personal_data.get('id'),'caption': data.get('caption'), 'comments_count': data.get('comments_count'), 'like_count':data.get('like_count'), 'media_type':data.get('media_type'), 'username':data.get('username'), 'media_product_type':data.get('media_product_type'), 'timestamp':dateparser.parse(data.get('timestamp'))}
        temp.append(dict)
    
    insight_dict = []
    
    for dict in temp:
        impressions_value = 0
        engagement_value = 0
        reach_value = 0
        saved_value = 0
        video_views_value = 0
        plays_value = 0
        shares_value = 0
        period = "lifetime"
        if dict['media_product_type'] == "FEED":
            if dict['media_type'] == "IMAGE":
                try:
                    insights = graph.get_connections(id=dict.get('media_id'), connection_name='insights', period='lifetime', metric='impressions, engagement, reach, saved')
                    for insight in insights['data']:
                        if "impression" in insight.get('name'):
                            impressions_value = insight.get('values')[0].get('value')
                        if "reach" in insight.get('name'):
                            reach_value = insight.get('values')[0].get('value')
                        if "engagement" in insight.get('name'):
                            engagement_value = insight.get('values')[0].get('value')
                        if "saved" in insight.get('name'):
                            saved_value = insight.get('values')[0].get('value')
                    temp_dict = {'media_id': dict.get('media_id'), 'period':period,'impressions': impressions_value, 'reach':reach_value, 'engagement': engagement_value, 'saved':saved_value, 'video_views':float("NAN"), 'plays':float("NAN"), 'shares':float("NAN")}
                    insight_dict.append(temp_dict)
                except:
                    temp_dict = {'media_id': dict.get('media_id'), 'period':float("NAN"),'impressions': float("NAN"), 'reach': float("NAN"), 'engagement':float("NAN"), 'saved':float("NAN"), 'video_views':float("NAN"), 'plays':float("NAN"), 'shares':float("NAN")}
                    insight_dict.append(temp_dict)

            elif dict['media_type'] == "VIDEO":
                try:
                    insights = graph.get_connections(id=dict.get('media_id'), connection_name='insights', period='lifetime', metric='impressions, engagement, reach, saved, video_views')
                    for insight in insights['data']:
                        if "impression" in insight.get('name'):
                            impressions_value = insight.get('values')[0].get('value')
                        if "reach" in insight.get('name'):
                            reach_value = insight.get('values')[0].get('value')
                        if "engagement" in insight.get('name'):
                            engagement_value = insight.get('values')[0].get('value')
                        if "saved" in insight.get('name'):
                            saved_value = insight.get('values')[0].get('value')
                        if "video_views" in insight.get('name'):
                            video_views_value = insight.get('values')[0].get('value')
                    temp_dict = {'media_id': dict.get('media_id'), 'period':period,'impressions': impressions_value, 'reach':reach_value, 'engagement': engagement_value, 'saved':saved_value, 'video_views':video_views_value, 'plays':float("NAN"), 'shares':float("NAN")}
                    insight_dict.append(temp_dict)
                except:
                    temp_dict = {'media_id': dict.get('media_id'), 'period':float("NAN"),'impressions': float("NAN"), 'reach': float("NAN"), 'engagement':float("NAN"), 'saved':float("NAN"), 'video_views':float("NAN"), 'plays':float("NAN"), 'shares':float("NAN")}
                    insight_dict.append(temp_dict)

        elif dict['media_product_type'] == "STORY":
            try:
                insights = graph.get_connections(id=dict.get('media_id'), connection_name='insights', period='lifetime', metric='impressions, reach')
                for insight in insights['data']:
                    if "impression" in insight.get('name'):
                        impressions_value = insight.get('values')[0].get('value')
                    if "reach" in insight.get('name'):
                        reach_value = insight.get('values')[0].get('value')
                temp_dict = {'media_id': dict.get('media_id'), 'period':period,'impressions': impressions_value, 'reach':reach_value, 'engagement': float("NAN"), 'saved': float("NAN"), 'video_views': float("NAN"), 'plays':float("NAN"), 'shares':float("NAN")}
                insight_dict.append(temp_dict)
            except:
                temp_dict = {'media_id': dict.get('media_id'), 'period':float("NAN"),'impressions': float("NAN"), 'reach':float("NAN"), 'engagement': float("NAN"), 'saved': float("NAN"), 'video_views': float("NAN"), 'plays':float("NAN"), 'shares':float("NAN")}
                insight_dict.append(temp_dict)

        elif dict['media_product_type'] == "REELS":
            try:
                insights = graph.get_connections(id=dict.get('media_id'), connection_name='insights', period='lifetime', metric='plays, reach, shares, saved')
                for insight in insights['data']:
                    if "plays" in insight.get('name'):
                        plays_value = insight.get('values')[0].get('value')
                    if "reach" in insight.get('name'):
                        reach_value = insight.get('values')[0].get('value')
                    if "shares" in insight.get('name'):
                        shares_value = insight.get('values')[0].get('value')
                    if "saved" in insight.get('name'):
                        saved_value = insight.get('values')[0].get('value')
                temp_dict = {'media_id': dict.get('media_id'), 'period':period,'impressions': float("NAN"), 'reach':reach_value, 'engagement': float("NAN"), 'saved': saved_value, 'video_views': float("NAN"), 'plays':plays_value, 'shares':shares_value}
                insight_dict.append(temp_dict)
            except:
                temp_dict = {'media_id': dict.get('media_id'), 'period':float("NAN"),'impressions': float("NAN"), 'reach': float("NAN"), 'engagement':float("NAN"), 'saved':float("NAN"), 'video_views':float("NAN"), 'plays':float("NAN"), 'shares':float("NAN")}
                insight_dict.append(temp_dict)
        else:
            pass
    
    return temp, insight_dict, instagram_user_stats
        
def get_ads_data(app_id, app_secret, access_token):
    FacebookAdsApi.init(app_id, app_secret, access_token) 
    my_account = User(fbid = 'me')
    ad_account = AdAccount(my_account.get_ad_accounts()[0]['id'])
    campaigns = ad_account.get_campaigns()
    campaign_ids = []
    for campaign in campaigns:
        campaign_ids.append(campaign.get('id'))

    temp = []
    for campaign_id in campaign_ids:
        campaign = Campaign(campaign_id)
        insights = campaign.get_insights(fields=[AdsInsights.Field.account_currency, AdsInsights.Field.account_id, AdsInsights.Field.account_name, AdsInsights.Field.ad_id,
            AdsInsights.Field.ad_name, AdsInsights.Field.adset_id, AdsInsights.Field.adset_name, AdsInsights.Field.campaign_id, AdsInsights.Field.campaign_name, AdsInsights.Field.canvas_avg_view_time,
            AdsInsights.Field.clicks, AdsInsights.Field.unique_clicks, AdsInsights.Field.date_start, AdsInsights.Field.date_stop, AdsInsights.Field.outbound_clicks, 
            AdsInsights.Field.impressions, AdsInsights.Field.outbound_clicks, AdsInsights.Field.spend, AdsInsights.Field.reach])
        for i in insights:
            if 'Instagram' not in i['campaign_name']:
                temp_dict = {'account_currency':i.get('account_currency'), 'account_id':i.get('account_id'),
                'account_name': i.get('account_name'), 'campaign_id':i.get('campaign_id'), 'campaign_name': i.get('campaign_name'), 
                'clicks': i.get('clicks'), 'unique_clicks': i.get('unique_clicks'), 'date_start':i.get('date_start'), 'date_stop':i.get('date_stop'),
                'outbound_clicks':i.get('outbound_clicks')[0].get('value'), 'impressions': i.get('impressions'), 'spend': i.get('spend'), 'reach':i.get('reach')}
                temp.append(temp_dict)
            else:
                temp_dict = {'account_currency':i.get('account_currency'), 'account_id':i.get('account_id'),
                'account_name': i.get('account_name'), 'campaign_id':i.get('campaign_id'), 'campaign_name': i.get('campaign_name'), 
                'clicks': i.get('clicks'), 'unique_clicks': i.get('unique_clicks'), 'date_start':i.get('date_start'), 'date_stop':i.get('date_stop'),
                'impressions': i.get('impressions'), 'spend': i.get('spend'), 'reach':i.get('reach')}
                temp.append(temp_dict)
    return temp
    

def get_facebook_posts(graph, facebook_id):
    user_posts = graph.get_connections(id=facebook_id, connection_name='posts', fields='id, message, created_time')
    post_ids = []
    temp = []
    caption_dict = {}
    created_time_dict = {}
    for post in user_posts['data']:
        post_ids.append(post.get('id'))
        caption_dict[post['id']] = {'caption':post.get('message')}
        created_time_dict[post['id']] = {'created_time': dateparser.parse(post.get('created_time'))}
        


    for id in post_ids:
        reactions = graph.get_connections(id = id, connection_name='reactions', fields='type, total_count, viewer_reaction', summary=True, period='lifetime')
        reactions_dump = json.dumps(reactions, indent=4)
        reactions_load = json.loads(reactions_dump)
        
        comments = graph.get_connections(id=id, connection_name='comments', fields='order, total_count', summary=True)
        comments_dump = json.dumps(comments, indent=4)
        comments_load = json.loads(comments_dump)
        
        insights = graph.get_connections(id=id, connection_name='insights', metric='post_reactions_anger_total,post_reactions_sorry_total,post_reactions_haha_total,post_reactions_wow_total,post_reactions_love_total,post_reactions_like_total')
        insights_dump = json.dumps(insights, indent=4)
        insights_load = json.loads(insights_dump)
        
        
        anger_value = 0
        sorry_value = 0
        haha_value = 0
        wow_value = 0
        love_value = 0
        like_value = 0
        for insight in insights_load['data']:
            if "anger" in insight.get('name'):
                anger_value = insight.get('values')[0].get('value')
            if "sorry" in insight.get('name'):
                sorry_value = insight.get('values')[0].get('value')
            if "haha" in insight.get('name'):
                haha_value = insight.get('values')[0].get('value')
            if "wow" in insight.get('name'):
                wow_value = insight.get('values')[0].get('value')
            if "love" in insight.get('name'):
                love_value = insight.get('values')[0].get('value')
            if "like" in insight.get('name'):
                like_value = insight.get('values')[0].get('value')
             
        temp_dict = {'id': id, 'caption':caption_dict.get(id).get('caption'),'reaction_count':reactions_load.get('summary').get('total_count'), 'comment_count':comments_load.get('summary').get('total_count'),
        'anger_reaction_count':anger_value, 'sorry_reaction_count':sorry_value, 'haha_reaction_count':haha_value, 'wow_reaction_count': wow_value, 
        'love_reaction_count': love_value, 'like_reaction_count':like_value, 'created_time':created_time_dict.get(id).get('created_time')}

        temp.append(temp_dict)

    return temp

def get_page_id_token(graph):
    profile = graph.get_connections('me', connection_name='accounts', fields='id, access_token')
    page_id = profile['data'][0]['id']
    page_access_token = profile['data'][0]['access_token']
    return page_id, page_access_token


def get_instagram_followers_by_city_gender_age(graph, id):
    city_data = graph.get_connections(id=id, connection_name='insights', metric='audience_city, audience_gender_age', period='lifetime')
    age_wise = []
    city_wise = []
    for insight in city_data['data']:
        if "audience_gender_age" in insight.get('name'):
            age_wise.append(insight.get('values')[0].get('value'))
        if "audience_city" in insight.get('name'):
            city_wise.append(insight.get('values')[0].get('value'))

    return city_wise, age_wise


def convert_to_df(data_city, data_age_gender):
    city_df = pd.DataFrame(list(data_city[0].items()), columns=['city','value'])
    age_df = pd.DataFrame(list(data_age_gender[0].items()), columns=['age', 'value'])


    city_df[['city', 'state']] = city_df.city.str.split(",", expand=True)
    age_df[['gender', 'age']] = age_df.age.str.split(".", expand=True)

    return city_df, age_df 


def get_page_impressions_by_city_gender_age(graph, id):
    city_data = graph.get_connections(id=id, connection_name='insights', metric='page_impressions_by_city_unique, page_impressions_by_age_gender_unique', period='days_28')
    likes_count = graph.get_object(id=id, fields = 'fan_count')
    fan_count = likes_count.get('fan_count')
    age_wise = []
    city_wise = []
    for insight in city_data['data']:
        if "age_gender" in insight.get('name'):
            age_wise.append(insight.get('values')[0].get('value'))
        if "city" in insight.get('name'):
            city_wise.append(insight.get('values')[0].get('value'))

    return city_wise, age_wise, fan_count

def page_data_convert_to_df(data_city, data_age_gender, fan_count):
    city_df = pd.DataFrame(list(data_city[0].items()), columns=['city','value'])
    age_df = pd.DataFrame(list(data_age_gender[0].items()), columns=['age', 'value'])

    city_df[['city', 'state', 'country']] = city_df.city.str.split(",", expand=True)
    age_df[['gender', 'age']] = age_df.age.str.split(".", expand=True)
    age_df['fan_count'] = fan_count

    return city_df, age_df 



def initialize_snowflake_connection(username, password, account):
    conn = snow.connect(
    user=username,
    password=password,
    account=account
    )
    cs = conn.cursor()
    return cs, conn

def upload_data(city_df, age_df, instagram_user_stats, instagram_data, instagram_insights, ads_data, user_post, page_post, cursor, conn):
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

    
    instagram_user_stats_sql = """CREATE OR REPLACE TABLE instagram_user_stats (profile_id VARCHAR(250), media_count VARCHAR(20), followers_count VARCHAR(20), follows_count VARCHAR(20))"""
    cursor.execute(instagram_user_stats_sql)

    success, nchunks, nrows, _ = write_pandas(conn, instagram_user_stats, "instagram_user_stats", quote_identifiers=False)
    print("Instagram User Stats Table: ", success, " ",  nchunks, " ", nrows)

    instagram_data_sql = """CREATE OR REPLACE TABLE instagram_data (media_id VARCHAR(250), profile_id VARCHAR(250), caption VARCHAR(500), comments_count VARCHAR(20),
    like_count VARCHAR(20), media_type VARCHAR(250), username VARCHAR(250), media_product_type VARCHAR(250), timestamp DATETIME)"""
    cursor.execute(instagram_data_sql)

    success, nchunks, nrows, _ = write_pandas(conn, instagram_data, "instagram_data", quote_identifiers=False)
    print("Instagram Data Table: ", success, " ",  nchunks, " ", nrows)

    instagram_insights_sql = """CREATE OR REPLACE TABLE instagram_insights (media_id VARCHAR(250), period VARCHAR(20), impressions VARCHAR(20), reach VARCHAR(20), engagement VARCHAR(20), saved VARCHAR(20), video_views VARCHAR(20), plays VARCHAR(20), shares VARCHAR(20))"""
    cursor.execute(instagram_insights_sql)

    success, nchunks, nrows, _ = write_pandas(conn, instagram_insights, "instagram_insights", quote_identifiers=False)
    print("Instagram Insights Table: ", success, " ",  nchunks, " ", nrows)

    
#    ads_data_sql = """CREATE OR REPLACE TABLE ads_data (account_currency VARCHAR(20), account_id VARCHAR(50), account_name VARCHAR(50), 
#    campaign_id VARCHAR(20), campaign_name VARCHAR(250), clicks VARCHAR(20), unique_clicks VARCHAR(20),
#    date_start DATETIME, date_stop DATETIME, outbound_clicks VARCHAR(20), impressions VARCHAR(20), spend VARCHAR(20), reach VARCHAR(20))"""
#    cursor.execute(ads_data_sql)

#    success, nchunks, nrows, _ = write_pandas(conn, ads_data, "ads_data", quote_identifiers=False)
#    print("Ads Data Table: ", success, " ",  nchunks, " ", nrows)
    
    user_post_sql = """CREATE OR REPLACE TABLE user_posts (id VARCHAR(50), caption VARCHAR(500), reaction_count VARCHAR(20), comment_count VARCHAR(20), 
    anger_reaction_count VARCHAR(20), sorry_reaction_count VARCHAR(20), haha_reaction_count VARCHAR(20), wow_reaction_count VARCHAR(20), 
    love_reaction_count VARCHAR(20), like_reaction_count VARCHAR(20), created_time DATETIME)"""
    cursor.execute(user_post_sql)

    success, nchunks, nrows, _ = write_pandas(conn, user_post, "user_posts", quote_identifiers=False)
    print("User Posts Table: ", success, " ",  nchunks, " ", nrows)

    page_post_sql = """CREATE OR REPLACE TABLE page_posts (id VARCHAR(50), caption VARCHAR(500), reaction_count VARCHAR(20), comment_count VARCHAR(20),
    anger_reaction_count VARCHAR(20), sorry_reaction_count VARCHAR(20), haha_reaction_count VARCHAR(20), wow_reaction_count VARCHAR(20), love_reaction_count VARCHAR(20), like_reaction_count VARCHAR(20), created_time DATETIME)"""
    cursor.execute(page_post_sql)

    success, nchunks, nrows, _ = write_pandas(conn, page_post, "page_posts", quote_identifiers=False)
    print("Page Posts Table: ", success, " ",  nchunks, " ", nrows)


def upload_instagram_city_df(cursor, conn, city_df):

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

    city_sql = """CREATE OR REPLACE TABLE instagram_followers_by_city (city VARCHAR(200), value VARCHAR(20), state VARCHAR(200))"""
    cursor.execute(city_sql)

    success, nchunks, nrows, _ = write_pandas(conn, city_df, "instagram_followers_by_city", quote_identifiers=False)
    print("Instagram Followers by City Table: ", success, " ",  nchunks, " ", nrows)


def upload_instagram_age_gender_df(cursor, conn, age_df):
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

    age_sql = """CREATE OR REPLACE TABLE instagram_followers_by_gender_age (age VARCHAR(20),value VARCHAR(20),gender VARCHAR(10))"""
    cursor.execute(age_sql)

    success, nchunks, nrows, _ = write_pandas(conn, age_df, "instagram_followers_by_gender_age", quote_identifiers=False)
    print("Instagram Followers by gender and age Table: ", success, " ",  nchunks, " ", nrows)


def upload_page_impressions_by_city(city_df, cursor, conn):
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

    city_sql = """CREATE OR REPLACE TABLE page_impressions_by_city (city VARCHAR(200), value VARCHAR(20), state VARCHAR(200), country VARCHAR(200))"""
    cursor.execute(city_sql)

    success, nchunks, nrows, _ = write_pandas(conn, city_df, "page_impressions_by_city", quote_identifiers=False)
    print("Page Impressions by City Table: ", success, " ",  nchunks, " ", nrows)

def upload_page_impressions_by_age_gender(age_df, cursor, conn):
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

    age_sql = """CREATE OR REPLACE TABLE page_impressions_by_gender_age (age VARCHAR(20),value VARCHAR(20),gender VARCHAR(10), fan_count VARCHAR(10))"""
    cursor.execute(age_sql)

    success, nchunks, nrows, _ = write_pandas(conn, age_df, "page_impressions_by_gender_age", quote_identifiers=False)
    print("Page Impressions by gender and age Table: ", success, " ",  nchunks, " ", nrows)

def close_snowflake_connection(cursor, conn):
    cursor.close()
    conn.close()


if __name__ == "__main__":

    current_dir = os.path.dirname(os.path.realpath(__file__))
    CONFIG_FILE = os.path.join(current_dir, "config.yaml")

    with open(CONFIG_FILE, "r") as f:
        config = yaml.full_load(f)
    
    facebook_access_token = config['facebook']['access_token']
    facebook_app_id = config['facebook']['app_id']
    facebook_app_secret = config['facebook']['app_secret']

    snowflake_username = config['snowflake']['username']
    snowflake_password = config['snowflake']['password']
    snowflake_account = config['snowflake']['account']


    graph = initialize_facebook_graph(facebook_access_token)
    facebook_id, instagram_id = get_facebook_instagram_id(graph)
    instagram_data, instagram_insight_dict, instagram_user_stats = get_instagram_data(graph, instagram_id) 
    ads_data = get_ads_data(facebook_app_id, facebook_app_secret, facebook_access_token)
    facebook_user_posts = get_facebook_posts(graph, facebook_id)
    page_id, page_access_token = get_page_id_token(graph)
    page_graph = initialize_facebook_graph(page_access_token)
    page_posts = get_facebook_posts(page_graph, page_id)

    instagram_user_stats_df = pd.DataFrame(instagram_user_stats)
    instagram_data_df = pd.DataFrame(instagram_data)    
    instagram_insight_df = pd.DataFrame(instagram_insight_dict)
    ads_data_df = pd.DataFrame(ads_data)
    facebook_user_posts_df = pd.DataFrame(facebook_user_posts)
    page_posts_df = pd.DataFrame(page_posts)
    followers_city, followers_age_gender = get_instagram_followers_by_city_gender_age(graph, instagram_id)
    followers_city_df, followers_age_gender_df = convert_to_df(followers_city, followers_age_gender)

#    impressions_city, impressions_age_gender, fan_count = get_page_impressions_by_city_gender_age(page_graph, page_id)
#    impressions_city_df, impressions_age_gender_df = page_data_convert_to_df(impressions_city, impressions_age_gender, fan_count)
    

    
    db_cursor, conn = initialize_snowflake_connection(snowflake_username, snowflake_password, snowflake_account)

    upload_data(followers_city_df, followers_age_gender_df, instagram_user_stats_df, instagram_data_df, instagram_insight_df, ads_data_df,facebook_user_posts_df, page_posts_df, db_cursor, conn)
    upload_instagram_city_df(db_cursor, conn, followers_city_df)
    upload_instagram_age_gender_df(db_cursor, conn, followers_age_gender_df)
#    upload_page_impressions_by_city(impressions_city_df, db_cursor, conn)
#    upload_page_impressions_by_age_gender(impressions_age_gender_df, db_cursor, conn)
    close_snowflake_connection(db_cursor, conn)
    