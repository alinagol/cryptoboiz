from credentials import *
import tweepy
from textblob import TextBlob
import os
import json
from pymongo import MongoClient
import re

# Tags to track

TRACK_TERMS = ['ripple', 'dogecoin', 'XRP']


# DB connect

BASE_PATH = os.path.dirname(__file__)
DB_CONFG_PATH = os.path.normpath(os.path.join(BASE_PATH, 'dbconfig/config.json'))

cfg = json.load(open(DB_CONFG_PATH))
db = 'mongodb://{host}:{port}'.format(**cfg)
with MongoClient(db) as client:
    cryptotweets = client[cfg['database']][cfg['collection']]


# functions

def clean_tweet(tweet):
    '''
    Utility function to clean the text in a tweet by removing
    links and special characters using regex.
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())


# Modify Listener

class StreamListener(tweepy.StreamListener):

    def on_status(self, status):

        if status.retweeted:
            return

        description = status.user.description
        loc = status.user.location
        text = status.text
        coords = status.coordinates
        geo = status.geo
        hashtags = status.entries['hashtags']
        name = status.user.screen_name
        user_created = status.user.created_at
        followers = status.user.followers_count
        id_str = status.id_str
        created = status.created_at
        retweets = status.retweet_count
        blob = TextBlob(clean_tweet(text))
        sent = blob.sentiment
        polarity = sent.polarity
        subjectivity = sent.subjectivity

        tags = []
        for term in TRACK_TERMS:
            if term in text.lower():
                tags.append(term)

        cryptotweets.insert({'id': id_str})

        try:
            cryptotweets.update({'id': id_str},
                                {'tag': tags,
                                 'user': {
                                     'name': name,
                                     'created': user_created,
                                     'followers': followers,
                                     'description': description,
                                     'location': loc
                                 },
                                 'tweet': {
                                     'text': text,
                                     'geo': geo,
                                     'hashtags': hashtags,
                                     'coordinates': coords,
                                     'retweets': retweets,
                                     'created': created,
                                     'polarity': polarity,
                                     'subjectivity': subjectivity
                                 }})
        except:
            pass

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

api = tweepy.API(auth)

stream_listener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
stream.filter(track=TRACK_TERMS)


