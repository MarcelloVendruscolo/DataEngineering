import sys
import json
import os
import re
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

TWEETS_DIRECTORY = '/home/ubuntu/volume/tweets'

try:
    client = MongoClient(host="localhost", port=27017)
    tweets_db = client.tweets_db
    print("Connected to MongoDB")
except:
    sys.stderr.write("Could not connect to MongoDB")
    sys.exit(1)

match = re.compile('tweets_*',re.IGNORECASE)
entries = os.listdir(TWEETS_DIRECTORY)

for entry in entries:   
    if (match.search(entry) is not None):
        with open(entry, 'r') as f:
            print("Inserting tweets of {0:s} in MongoDB".format(entry))
            for line in f:
                line = line.strip() # remove leading and trailing whitespace
                try:
                    a_tweet = json.loads(line)
                    tweets_db.tweets.insert_one(a_tweet)
                except:
                    continue
            print("Insertion of {0:s} completed".format(entry))
            f.close() # close file handle

client.close # close db connection