from credentials import *
import tweepy
from textblob import TextBlob
import os
import json
from pymongo import MongoClient
import re
import dateutil.parser
from datetime import datetime
from coinmarketcap import Market
import time
from binance.client import Client


# Parameters
start_date = '2017-10-01'


# Options
today = datetime.now().strftime("%Y-%m-%d")
today_mdgb = dateutil.parser.parse(today)
tags = json.load(open('crypto_names.json'))
coinmarketcap = Market()


# Functions
def clean_tweet(txt):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", txt).split())


# DB connect
BASE_PATH = os.path.dirname(__file__)
DB_CONFG_PATH = os.path.normpath(os.path.join(BASE_PATH, 'config/db.json'))

cfg = json.load(open(DB_CONFG_PATH))

# db = 'mongodb://{host}:{port}'.format(**cfg['local'])  # local DB
# with MongoClient(db) as client:
#     cryptotweets = client[cfg['local']['database']]['crypto']
#     cryptoscores = client[cfg['local']['database']]['scores']

db = 'mongodb://{user}:{pass}@{host}:{port}'.format(**cfg['external'])  # DB on the server
with MongoClient(db) as client:
    cryptotweets = client[cfg['external']['database']]['crypto']
    cryptoscores = client[cfg['external']['database']]['scores']


# Twitter connect
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


# Binance connect
binance = json.load(open('config/binance.json'))
BinanceClient = Client(binance['API Key'], binance['Secret'])


# Get tweets
for coin in tags:

    print('Getting tweets for %s since %s' % (coin, start_date))

    coin_ticker = coin.split(' OR ')[1].replace('$', '')  # abbreviation of the coin

    try:

        for tweet in tweepy.Cursor(api.search,
                                   q=coin,
                                   include_entities=True,
                                   since=start_date).items():

            twt = tweet._json

            # essential entities - results are useless without them
            created = dateutil.parser.parse(twt['created_at'])
            id = twt['id_str']
            text = twt['text']
            username = twt['user']['screen_name']
            usercreated = dateutil.parser.parse(twt['user']['created_at'])
            userfollowers = twt['user']['followers_count']
            userfriends = twt['user']['friends_count']
            userid = twt['user']['id_str']
            usertweets = twt['user']['statuses_count']
            if twt['user']['verified']:
                userverified = 1
            else:
                userverified = 0

            # optional entities - nice to have
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
            polarity = sent.polarity  # positive or negative [-1, 1]
            subjectivity = sent.subjectivity  # [0, 1]

            if subjectivity != 0:
                weighted_sent = polarity / subjectivity  # subjective tweets are less important
            else:
                weighted_sent = 0

            # insert in db
            if not cryptotweets.find_one({'tweet_id': id}) and 'RT @' not in text:  # excluding repetitions and retweets

                cryptotweets.insert({'tweet_id': id,
                                     'tag': coin_ticker,
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
                                         'subjectivity': subjectivity,
                                         'weighted_sentiment': weighted_sent
                                     }})

                try:
                    cryptotweets.update({'tweet_id': id}, {'$set': {'user.location': userloc}})
                except:
                    pass

                try:
                    cryptotweets.update({'tweet_id': id}, {'$set': {'tweet.country': country}})
                except:
                    pass

                try:
                    cryptotweets.update({'tweet_id': id}, {'$set': {'tweet.language': language}})
                except:
                    pass

                try:
                    if hashtags:
                        cryptotweets.update({'tweet_id': id}, {'$set': {'tweet.hashtags': hashtags}})
                except:
                    pass

                try:
                    cryptotweets.update({'tweet_id': id}, {'$set': {'tweet.retweets': retweets}})
                except:
                    pass

                try:
                    cryptotweets.update({'tweet_id': id}, {'$set': {'tweet.favourites': favourites}})
                except:
                    pass

        # Insert a record in Scores collection if non-existent yet
        if not cryptoscores.find_one({'currency': coin_ticker}):
            cryptoscores.insert({'currency': coin_ticker})

        # Get average sentiment for each day
        print('Getting sentiment scores...')
        summary = cryptotweets.aggregate([{'$match': {'tag': coin_ticker}},
                                          {'$group': {'_id': {'currency': '$tag',
                                                              'date': {'$dateToString':
                                                                           {'format': "%Y-%m-%d",
                                                                            'date': '$tweet.created'}}},
                                                      'avg_sent': {'$avg': '$tweet.polarity'},
                                                      'w_avg_sent': {'$avg': '$tweet.weighted_sentiment'},
                                                      'number_of_tweets': {'$sum': 1},
                                                      'avg_followers_user': {'$avg': '$user.followers'},
                                                      'avg_tweets_user': {'$avg': '$user.tweets'},
                                                      'n_verified_users': {'$sum': '$user.verified'}}}])

        print(summary)

        # Insert in Scores db
        for item in list(summary):

            price_info = {}

            print(item)

            if item['_id']['currency'] != 'btc':
                try:
                    ticker = item['_id']['currency'].upper() + 'BTC'  # to get prices in BTC
                    print(ticker)

                    h_btc = BinanceClient.get_historical_klines(ticker, Client.KLINE_INTERVAL_1DAY,
                                                                item['_id']['date'],
                                                                item['_id']['date'])
                    print('RECEIVED PRICE INFO:', h_btc)

                    h_usd = BinanceClient.get_historical_klines('BTCUSDT', Client.KLINE_INTERVAL_1DAY,
                                                                item['_id']['date'],
                                                                item['_id']['date'])


                    btc_usd = float(h_usd[0][4])
                    price_usd = float(h_btc[0][4]) * btc_usd  # convert price from BTC to USD
                    price_btc = float(h_btc[0][4])
                    volume = float(h_btc[0][5])
                    n_trades = float(h_btc[0][8])

                    price_info = {'price_usd': price_usd,
                                  'price_btc': price_btc,
                                  'volume': volume,
                                  'n_trades': n_trades}

                    # [
                    #  [
                    # 		    1499040000000,      // Open time
                    # 		    "0.01634790",       // Open
                    # 		    "0.80000000",       // High
                    # 		    "0.01575800",       // Low
                    # 		    "0.01577100",       // Close
                    # 		    "148976.11427815",  // Volume
                    # 		    1499644799999,      // Close time
                    # 		    "2434.19055334",    // Quote asset volume
                    # 		    308,                // Number of trades
                    # 		    "1756.87402397",    // Taker buy base asset volume
                    # 		    "28.46694368",      // Taker buy quote asset volume
                    # 		    "17928899.62484339" // Can be ignored
                    # 		  ]
                    # 		]

                except Exception as priceEx1:
                    print('Cannot get price info', priceEx1, type(priceEx1))
                    pass

            else:

                ticker = item['_id']['currency'].upper() + 'USDT'

                try:
                    h = BinanceClient.get_historical_klines(ticker, Client.KLINE_INTERVAL_1DAY,
                                                            item['_id']['date'],
                                                            item['_id']['date'])

                    price_usd = float(h[0][4])
                    price_btc = 1
                    volume = float(h[0][5])
                    n_trades = float(h[0][8])

                    price_info = {'price_usd': price_usd,
                                  'price_btc': price_btc,
                                  'volume': volume,
                                  'n_trades': n_trades}

                except Exception as priceEx2:
                    print('Cannot get price info', priceEx2, type(priceEx2))
                    pass

            try:

                print('Prices:', price_btc, price_usd)

                cryptoscores.update({'currency': coin_ticker},
                                    {'$push': {'scores': {'date': dateutil.parser.parse(item['_id']['date']),
                                                          'avg_sent': item['avg_sent'],
                                                          'w_avg_sent': item['w_avg_sent'],
                                                          'num_tweets': item['number_of_tweets'],
                                                          'avg_followers_user': item['avg_followers_user'],
                                                          'avg_tweets_user': item['avg_tweets_user'],
                                                          'n_verified_users': item['n_verified_users'],
                                                          'price_usd': price_info['price_usd'],
                                                          'price_btc': price_info['price_btc'],
                                                          'volume': price_info['volume'],
                                                          'n_trades': price_info['n_trades']
                                                          }}})
            except:
                try:
                    print('No prices found')
                    cryptoscores.update({'currency': coin_ticker},
                                        {'$push': {'scores': {'date': dateutil.parser.parse(item['_id']['date']),
                                                              'avg_sent': item['avg_sent'],
                                                              'w_avg_sent': item['w_avg_sent'],
                                                              'num_tweets': item['number_of_tweets'],
                                                              'avg_followers_user': item['avg_followers_user'],
                                                              'avg_tweets_user': item['avg_tweets_user'],
                                                              'n_verified_users': item['n_verified_users']
                                                              }}})
                except:
                    pass

        # delete tweets
        cryptotweets.delete_many({'tag': coin_ticker})

        print('************************************')

    except Exception as MasterEx:
        print('Something happened...', MasterEx, type(MasterEx))
        pass

