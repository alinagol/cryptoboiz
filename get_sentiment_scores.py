import os
import json
from pymongo import MongoClient
from datetime import datetime, timedelta
from pprint import pprint
from credentials import *
import tweepy


# Options
today = datetime.now().strftime("%Y-%m-%d")


# DB connect
BASE_PATH = os.path.dirname(__file__)
DB_CONFG_PATH = os.path.normpath(os.path.join(BASE_PATH, 'dbconfig/config.json'))

cfg = json.load(open(DB_CONFG_PATH))
db = 'mongodb://{host}:{port}'.format(**cfg)
with MongoClient(db) as client:
    cryptotweets = client[cfg['database']][cfg['collection']]
    cryptoscores = client[cfg['database']]['scores']


# Twitter connect
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


# Sentiment scores db
if not cryptoscores.find_one({'currency': 'ripple'}):
    cryptoscores.insert({'currency': 'ripple'})

if not cryptoscores.find_one({'currency': 'tron'}):
    cryptoscores.insert({'currency': 'tron'})

if not cryptoscores.find_one({'currency': 'ethereum'}):
    cryptoscores.insert({'currency': 'ethereum'})


# Get scores
summary = cryptotweets.aggregate([{'$group': {'_id': {'currency': '$tag',
                                            'date': {'$dateToString':
                                                         {'format': "%Y-%m-%d",
                                                          'date': '$tweet.created'}}},
                                    'avg_sent': {'$avg': '$tweet.polarity'}}}])


for item in list(summary):
    message = 'Currency: %s, Sentiment: %s' % (item['_id']['currency'], item['avg_sent'])
    print(message)
    api.update_status(message)
