from credentials import *
import tweepy
from textblob import TextBlob
import os
import json
from pymongo import MongoClient
import re
import dateutil.parser
from datetime import datetime, timedelta


# Options
today = datetime.now().strftime("%Y-%m-%d")
tags = ['ripple', 'tron', 'ethereum', 'XRP', 'ETH', 'TRX']


# Functions
def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())


# DB connect
BASE_PATH = os.path.dirname(__file__)
DB_CONFG_PATH = os.path.normpath(os.path.join(BASE_PATH, 'dbconfig/config.json'))

cfg = json.load(open(DB_CONFG_PATH))
db = 'mongodb://{host}:{port}'.format(**cfg)
with MongoClient(db) as client:
    cryptotweets = client[cfg['database']][cfg['collection']]


# Twitter connect
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


# Get tweets
for searchtag in tags:

    for tweet in tweepy.Cursor(api.search,
                           q=searchtag,
                           include_entities=True,
                           since=today).items():

        twt = tweet._json

        # essential entities
        created = dateutil.parser.parse(twt['created_at'])
        id = twt['id_str']
        text = twt['text']
        username = twt['user']['screen_name']
        usercreated = dateutil.parser.parse(twt['user']['created_at'])
        userfollowers = twt['user']['followers_count']
        userfriends = twt['user']['friends_count']
        userid = twt['user']['id_str']
        usertweets = twt['user']['statuses_count']
        userverified = twt['user']['verified']

        # optional entities
        try:
            hashtags = []
            for tag in twt['entities']['hashtags']:
                ht = tag['text']
                hashtags.append(ht)
        except:
            pass

        try:
            favourites = twt['favorite_count']
        except:
            pass

        try:
            retweets = twt['retweet_count']
        except:
            pass

        try:
            language = twt['lang']
        except:
            pass

        try:
            country = twt['place']['country']
        except:
            pass

        try:
            userloc = twt['user']['location']
        except:
            pass


        # sentiment
        blob = TextBlob(clean_tweet(text))
        sent = blob.sentiment
        polarity = sent.polarity
        subjectivity = sent.subjectivity

        # insert in db

        if not cryptotweets.find_one({'tweet_id': id}):

            cryptotweets.insert({'tweet_id': id,
                             'tag': searchtag,
                             'user': {
                                 'id': userid,
                                 'name': username,
                                 'created': usercreated,
                                 'followers': userfollowers,
                                 'friends': userfriends,
                                 'tweets': usertweets,
                                 'verified': userverified,
                             },
                             'tweet': {
                                 'text': text,
                                 'created': created,
                                 'polarity': polarity,
                                 'subjectivity': subjectivity
                             }})

            try:
                cryptotweets.update({'tweet_id': id}, {'$set': {'user.location': userloc}})
            except:
                pass

            try:
                cryptotweets.update({'tweet_id': id}, {'$set':  {'tweet.country': country}})
            except:
                pass

            try:
                cryptotweets.update({'tweet_id': id}, {'$set':  {'tweet.language': language}})
            except:
                pass

            try:
                if hashtags:
                    cryptotweets.update({'tweet_id': id}, {'$set':  {'tweet.hashtags': hashtags}})
            except:
                pass

            try:
                cryptotweets.update({'tweet_id': id}, {'$set':  {'tweet.retweets': retweets}})
            except:
                pass

            try:
                cryptotweets.update({'tweet_id': id}, {'$set': {'tweet.favourites': favourites}})
            except:
                pass
