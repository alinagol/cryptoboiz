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


# Get tweets
for searchtag in tags:

    print('Searching for tweets about', searchtag)

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

        if subjectivity != 0:
            weighted_sent = polarity/subjectivity
        else:
            weighted_sent = 0

        # insert in db
        if not cryptotweets.find_one({'tweet_id': id}) and 'RT @' not in text:

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
                                 'subjectivity': subjectivity,
                                 'weighted_sentiment': weighted_sent
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

    # Insert a record in Scores collection if non-existent yet
    if not cryptoscores.find_one({'currency': searchtag}):
        cryptoscores.insert({'currency': searchtag})

    try:

        # Get prices and other market info
        try:
            stats = coinmarketcap.ticker(searchtag, convert='EUR')
        except:
            print('Sleeping...')
            time.sleep(60)
            print('Retrying CoinMarketCap...')
            try:
                stats = coinmarketcap.ticker(searchtag, convert='EUR')
            except:
                pass

        # Get average sentiment
        summary = cryptotweets.aggregate([{'$match': {'tweet.created': {'$gt': today_mdgb}, 'tag': searchtag}},
                                      {'$group': {'_id': {'currency': '$tag',
                                                          'date': {'$dateToString':
                                                                       {'format': "%Y-%m-%d",
                                                                        'date': '$tweet.created'}}},
                                                  'avg_sent': {'$avg': '$tweet.polarity'},
                                                  'w_avg_sent': {'$avg': '$tweet.weighted_sentiment'},
                                                  'number_of_tweets': {'$sum': 1}}}])

        try:
            item = list(summary)[0]
        except:
            pass

        print(item)

        if stats and item:
            message = 'Currency: %s \nPrice: %s \nSentiment: %s \nWeighted sentiment: %s \nNumber of tweets: %s' \
                          % (searchtag,
                             float(stats[0]['price_eur']),
                             round(item['avg_sent'], 5),
                             round(item['w_avg_sent'], 5),
                             item['number_of_tweets'])

                # if item['_id']['date'] == today:
                #     try:
                #         api.update_status(message)
                #         print('Tweet posted')
                #     except:
                #         print('Cannot post a tweet')
                #         pass

            # TODO: spit into 2 updates (maybe)

            if not cryptoscores.find({'date': dateutil.parser.parse(item['_id']['date'])}):

                cryptoscores.update({'currency': searchtag},
                                    {'$push': {'scores': {'date': dateutil.parser.parse(item['_id']['date']),
                                                          'avg_sent': item['avg_sent'],
                                                          'w_avg_sent': item['w_avg_sent'],
                                                          'num_tweets': item['number_of_tweets'],
                                                          'price_eur': float(stats[0]['price_eur']),
                                                          '24h_volume_eur': float(stats[0]['24h_volume_eur']),
                                                          'available_supply': float(stats[0]['available_supply']),
                                                          'market_cap_eur': float(stats[0]['market_cap_eur']),
                                                          'price_btc': float(stats[0]['price_btc']),
                                                          'total_supply': float(stats[0]['total_supply'])}}})

    except:
        pass

    # delete tweets
    cryptotweets.delete_many({'tag': searchtag})
