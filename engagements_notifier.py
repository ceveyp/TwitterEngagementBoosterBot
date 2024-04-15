from discord import SyncWebhook
import discord
import requests
import time
from datetime import datetime
from db import *
from config import *

twitter_base_uri = "https://api.twitter.com/2"
tweet_url = 'https://twitter.com/tweet/status/{}'
headers = {"Authorization": "Bearer {}".format(twitter_bearer_token),
           "User-Agent": "v2UserTweetsPython"}

embed_color = 0x00ff00
points_breakdown = "Like: 10 points\n\nRetweet: 20 points\n\nComment: 30 points"
rewards_distributed_text = "Today's rewards have been distributed for Twitter likes/comments/retweets. Check the leaderboard in the in the <#{}> channel".format(commands_channel_id)


def send_discord_tweet_notification(tweet_id: str, tweet_content: str) -> None:
    webhook = SyncWebhook.from_url(tweets_webhook)
    this_tweet_url = tweet_url.format(tweet_id)
    register_embed = discord.Embed(title="New Tweet Notification",
                                   description=tweet_content,
                                   url=this_tweet_url,
                                   color=embed_color)
    register_embed.set_footer(text=points_breakdown)
    register_embed.add_field(name='Link', value=this_tweet_url)
    webhook.send(embed=register_embed)


def twitter_get_single_user_id(username: str) -> str:
    sql_query = "SELECT user_id FROM {}.accounts WHERE username=%s".format(mysql_db_name)
    params = (username, )
    ret = mysql_query(sql_query, params)
    if ret:
        return ret.pop().get("user_id")
    twitter_get_user_id_uri = twitter_base_uri + "/users/by/username/{}"
    api_data = twitter_api_call(url=twitter_get_user_id_uri.format(username))
    user_id = api_data.get('data').get('id')
    sql_query = "INSERT INTO {}.accounts (username, user_id) VALUES (%s, %s)".format(mysql_db_name)
    params = (username, user_id)
    mysql_exec(sql_query, params)
    return user_id


def twitter_get_timeline_tweets(username: str) -> list:
    tweets_timeline_url = twitter_base_uri + "/users/{}/tweets"
    user_id = twitter_get_single_user_id(username)
    params = {"exclude": "retweets,replies"}
    api_data = twitter_api_call(url=tweets_timeline_url.format(user_id), params=params)
    tweets = api_data.get('data')
    return tweets


def twitter_get_tweet_liking_users(tweet_id: str) -> list:
    tweet_likes_url = twitter_base_uri + "/tweets/{}/liking_users"
    api_data = twitter_api_call(url=tweet_likes_url.format(tweet_id))
    likes = api_data.get('data')
    if not likes:
        return []
    liking_users = []
    for like in likes:
        liking_users.append(like.get('username').lower())
    return liking_users


def twitter_get_tweet_retweeting_users(tweet_id: str) -> list:
    tweet_retweets_url = twitter_base_uri + "/tweets/{}/retweeted_by"
    api_data = twitter_api_call(url=tweet_retweets_url.format(tweet_id))
    retweets = api_data.get('data')
    if not retweets:
        return []
    retweeting_users = []
    for retweet in retweets:
        retweeting_users.append(retweet.get('username').lower())
    return retweeting_users


def twitter_get_tweet_comments(tweet_id: str) -> list:
    tweet_comments_url = twitter_base_uri + "/tweets/search/recent"
    params = {"query": "conversation_id:{}".format(tweet_id),
              "expansions": "author_id"}
    api_data = twitter_api_call(url=tweet_comments_url, params=params)
    commenting_users = api_data.get('data')
    return commenting_users


def twitter_get_tweet_commenting_users(tweet_id: str) -> list:
    commenting_users = twitter_get_tweet_comments(tweet_id)
    if not commenting_users:
        return []
    commenting_user_ids = ""
    commenting_users_usernames = []
    for commenting_user in commenting_users:
        commenting_user_ids = commenting_user_ids + commenting_user.get('author_id') + ','
    if commenting_user_ids.endswith(','):
        commenting_user_ids = commenting_user_ids[0:len(commenting_user_ids) - 1]
    if commenting_user_ids:
        twitter_user_id_lookup_url = twitter_base_uri + "/users"
        params = {"ids": commenting_user_ids}
        api_data = twitter_api_call(url=twitter_user_id_lookup_url, params=params)
        commenting_users = api_data.get('data')
        for commenting_user in commenting_users:
            commenting_username = commenting_user.get('username')
            commenting_users_usernames.append(commenting_username.lower())
    return commenting_users_usernames


def tweet_exists(tweet_id: str) -> bool:
    sql_query = "SELECT * FROM {}.tweets WHERE tweet_id=%s".format(mysql_db_name)
    params = (tweet_id,)
    return bool(mysql_query(sql_query, params))


def insert_tweet(tweet_id: str, username: str) -> None:
    sql_query = "INSERT INTO {}.tweets VALUES (%s, %s)".format(mysql_db_name)
    params = (tweet_id, username)
    mysql_exec(sql_query, params)


def twitter_api_call(url: str, method: str = 'GET', params: dict = None, data: dict = None) -> any:
    attempts = 0
    while True:
        if attempts == 50:
            raise Exception("Could not complete API call after 50 attempts")
        resp = requests.request(method=method,
                                url=url,
                                params=params,
                                json=data,
                                headers=headers,
                                timeout=30)
        if resp.status_code == 429:
            attempts += 1
            print("API timeout")
            time.sleep(60)
            continue
        if not resp.ok:
            raise Exception("Twitter API error: " + resp.text)
        return resp.json()


def twitter_scrape_engagements(username: str) -> dict:
    engagements = {"username": username,
                   "tweets": []}
    tweets = twitter_get_timeline_tweets(username)
    for tweet in tweets:
        tweet_id = tweet.get('id')
        tweet_text = tweet.get('text')
        if not tweet_exists(tweet_id):
            insert_tweet(tweet_id, username)
            send_discord_tweet_notification(tweet_id, tweet_text)
        tweet_likes = twitter_get_tweet_liking_users(tweet_id)
        tweet_retweets = twitter_get_tweet_retweeting_users(tweet_id)
        tweet_comments = twitter_get_tweet_commenting_users(tweet_id)
        this_tweet = {"tweet_id": tweet_id,
                      "tweet_likes": tweet_likes,
                      "tweet_retweets": tweet_retweets,
                      "tweet_comments": tweet_comments}
        engagements["tweets"].append(this_tweet)
    return engagements


def get_registered_users() -> list:
    sql_query = "SELECT DISTINCT twitter_handle FROM {}.users".format(mysql_db_name)
    users = [x.get('twitter_handle').replace('@', '').lower().strip() for x in mysql_query(sql_query)]
    return users


def add_user_points(engagements: dict) -> None:
    try:
        registered_users = get_registered_users()
        username = engagements.get('username')
        tweets = engagements.get('tweets')
        for tweet in tweets:
            tweet_id = tweet.get('tweet_id')

            # Tweet likes
            try:
                tweet_likes = tweet.get('tweet_likes')
                sql_query = "SELECT * FROM {}.engagements WHERE tweet_id=%s AND type=1".format(mysql_db_name)
                params = (tweet_id, )
                db_tweet_likes = mysql_query(sql_query, params)
                db_tweet_liking_users = [x.get('username').lower().strip() for x in db_tweet_likes]
                for tweet_liker in tweet_likes:
                    try:
                        tweet_liker_formatted = tweet_liker.lower().strip()
                        if tweet_liker_formatted in registered_users:
                            if tweet_liker_formatted not in db_tweet_liking_users:
                                sql_query = """INSERT INTO {}.engagements (account, tweet_id, username, type)
                                                    VALUES (%s, %s, %s, %s)""".format(mysql_db_name)
                                params = (username, tweet_id, tweet_liker_formatted, 1)
                                mysql_exec(sql_query, params)
                                sql_query = "SELECT id, points FROM {}.users WHERE twitter_handle=%s".format(mysql_db_name)
                                params = (tweet_liker_formatted, )
                                db_user = mysql_query(sql_query, params).pop()
                                points = db_user.get('points') + like_points
                                sql_query = "UPDATE {}.users SET points=%s WHERE id=%s".format(mysql_db_name)
                                params = (points, db_user.get('id'))
                                mysql_exec(sql_query, params)
                    except Exception as e:
                        print(e)
            except Exception as e:
                print("Exception thrown in tweet likes")
                print(e)

            # Tweet retweets
            try:
                tweet_retweets = tweet.get('tweet_retweets')
                sql_query = "SELECT * FROM {}.engagements WHERE tweet_id=%s AND type=2".format(mysql_db_name)
                params = (tweet_id, )
                db_tweet_retweets = mysql_query(sql_query, params)
                db_tweet_retweeting_users = [x.get('username').lower().strip() for x in db_tweet_retweets]
                for tweet_retweeter in tweet_retweets:
                    try:
                        tweet_retweeter_formatted = tweet_retweeter.lower().strip()
                        if tweet_retweeter_formatted in registered_users:
                            if tweet_retweeter_formatted not in db_tweet_retweeting_users:
                                sql_query = """INSERT INTO {}.engagements (account, tweet_id, username, type)
                                                    VALUES (%s, %s, %s, %s)""".format(mysql_db_name)
                                params = (username, tweet_id, tweet_retweeter_formatted, 2)
                                mysql_exec(sql_query, params)
                                sql_query = "SELECT id, points FROM {}.users WHERE twitter_handle=%s".format(mysql_db_name)
                                params = (tweet_retweeter_formatted, )
                                db_user = mysql_query(sql_query, params).pop()
                                points = db_user.get('points') + retweet_points
                                sql_query = "UPDATE {}.users SET points=%s WHERE id=%s".format(mysql_db_name)
                                params = (points, db_user.get('id'))
                                mysql_exec(sql_query, params)
                    except Exception as e:
                        print(e)
            except Exception as e:
                print("Exception thrown in tweet retweets")
                print(e)

            # Tweet comments
            try:
                tweet_comments = tweet.get('tweet_comments')
                sql_query = "SELECT * FROM {}.engagements WHERE tweet_id=%s AND type=3".format(mysql_db_name)
                params = (tweet_id, )
                db_tweet_comments = mysql_query(sql_query, params)
                db_tweet_commenting_users = [x.get('username').lower().strip() for x in db_tweet_comments]
                for tweet_comment in tweet_comments:
                    try:
                        tweet_commenter_formatted = tweet_comment.lower().strip()
                        if tweet_commenter_formatted in registered_users:
                            if tweet_commenter_formatted not in db_tweet_commenting_users:
                                sql_query = """INSERT INTO {}.engagements (account, tweet_id, username, type)
                                                    VALUES (%s, %s, %s, %s)""".format(mysql_db_name)
                                params = (username, tweet_id, tweet_commenter_formatted, 3)
                                mysql_exec(sql_query, params)
                                sql_query = "SELECT id, points FROM {}.users WHERE twitter_handle=%s".format(mysql_db_name)
                                params = (tweet_commenter_formatted, )
                                db_user = mysql_query(sql_query, params).pop()
                                points = db_user.get('points') + comment_points
                                sql_query = "UPDATE {}.users SET points=%s WHERE id=%s".format(mysql_db_name)
                                params = (points, db_user.get('id'))
                                mysql_exec(sql_query, params)
                    except Exception as e:
                        print(e)
            except Exception as e:
                print("Exception thrown in tweet comments")
                print(e)
    except Exception as e:
        print("Exception in add user points")
        print(e)


def fetch_new_twitter_posts(username: str) -> None:
    tweets = twitter_get_timeline_tweets(username)
    for tweet in tweets:
        tweet_id = tweet.get('id')
        tweet_text = tweet.get('text')
        if not tweet_exists(tweet_id):
            insert_tweet(tweet_id, username)
            send_discord_tweet_notification(tweet_id, tweet_text)


def run_twitter_new_posts_notifier() -> None:
    while True:
        for username in usernames:
            try:
                print("Checking for new posts")
                fetch_new_twitter_posts(username)
            except Exception as e:
                print("Exception in twitter new posts notifier")
                print(e)
        time.sleep(120)


def run_twitter_engagements_notifier() -> None:
    while True:
        try:
            now = datetime.now()
            if now.hour == 22 and now.minute == 0:
                print("Running twitter engagements notifier")
                for username in usernames:
                    try:
                        engagements = twitter_scrape_engagements(username)
                        add_user_points(engagements)
                    except Exception as e:
                        print("Exception in twitter engagements notifier: " + username)
                        print(e)
                webhook = SyncWebhook.from_url(tweets_webhook)
                register_embed = discord.Embed(title="Rewards have been distributed!",
                                               description=rewards_distributed_text,
                                               color=embed_color)
                webhook.send(embed=register_embed)
                time.sleep(3 * 60)
        except Exception as e:
            print("Exception in twitter engagements notifier")
            print(e)
        time.sleep(5)
