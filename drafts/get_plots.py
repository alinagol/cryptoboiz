import os
import json
from pymongo import MongoClient
from credentials import *
import tweepy
import dateutil.parser
from datetime import datetime, timedelta
import matplotlib


# Options
today = datetime.now().strftime("%Y-%m-%d")
today_mdgb = dateutil.parser.parse(today)


# DB connect
BASE_PATH = os.path.dirname(__file__)
DB_CONFG_PATH = os.path.normpath(os.path.join(BASE_PATH, 'dbconfig/config.json'))

cfg = json.load(open(DB_CONFG_PATH))
db = 'mongodb://{host}:{port}'.format(**cfg)
with MongoClient(db) as client:
    cryptoscores = client[cfg['database']]['scores']


# Plot sentiment

