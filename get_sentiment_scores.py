import os
import json
from pymongo import MongoClient
from credentials import *
import tweepy
import dateutil.parser
from datetime import datetime
from coinmarketcap import Market


# Options
today = datetime.now().strftime("%Y-%m-%d")
today_mdgb = dateutil.parser.parse(today)

coins = []
crypto_names = json.load(open('crypto_names.json'))
for key, value in crypto_names.items():
    coins.append(key)

coinmarketcap = Market()


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


# Get scores for today
summary = cryptotweets.aggregate([{'$match': {'tweet.created':  {'$gt': today_mdgb}}},
                                  {'$group': {'_id': {'currency': '$tag',
                                                      'date': {'$dateToString':
                                                                   {'format': "%Y-%m-%d",
                                                                    'date': '$tweet.created'}}},
                                              'avg_sent': {'$avg': '$tweet.polarity'},
                                              'w_avg_sent': {'$avg': '$tweet.weighted_sentiment'},
                                              'number_of_tweets': {'$sum': 1}}}])


# Post to Twitter and save to db
for item in list(summary):

    try:

        stats = coinmarketcap.ticker(item['_id']['currency'], limit=3, convert='EUR')

        message = 'Currency: %s \nPrice: %s \nSentiment: %s \nWeighted sentiment: %s \nNumber of tweets: %s' \
              % (item['_id']['currency'],
                 float(stats[0]['price_eur']),
                 round(item['avg_sent'], 5),
                 round(item['w_avg_sent'], 5),
                 item['number_of_tweets'])

        if item['_id']['date'] == today:
            try:
                api.update_status(message)
                print('Tweet posted')
            except:
                print('Cannot post a tweet')
                pass

        cryptoscores.update({'currency': item['_id']['currency']},
                        {'$push': {'scores': {'date': dateutil.parser.parse(item['_id']['date']),
                                              'avg_sent': item['avg_sent'],
                                              'w_avg_sent': item['w_avg_sent'],
                                              'price_eur': float(stats[0]['price_eur']),
                                              '24h_volume_eur': float(stats[0]['24h_volume_eur']),
                                              'available_supply': float(stats[0]['available_supply']),
                                              'market_cap_eur': float(stats[0]['market_cap_eur']),
                                              'price_btc': float(stats[0]['price_btc']),
                                              'total_supply': float(stats[0]['total_supply'])}}})
    except Exception as ex:
        print('Error occurred:', ex, type(ex))
        pass
