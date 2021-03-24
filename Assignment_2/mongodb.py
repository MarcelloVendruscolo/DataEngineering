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
    #if (match.search(entry) is not None):
    if (entry == 'tweets_0.txt' or entry == 'tweets_1.txt'):
        with open(entry, 'r') as f:
            list_tweets = []
            print("Inserting tweets of {0:s} in MongoDB".format(entry))
            for line in f:
                line = line.strip() # remove leading and trailing whitespace
                try:
                    a_tweet = json.loads(line)
                    if not a_tweet['retweeted']:
                    	list_tweets.append(a_tweet)
                    	if (len(list_tweets) == 99999):
                            print('Inserting batch of size 99999...')
                            tweets_db.tweets.insert_many(list_tweets)
                            list_tweets = []
                            print('Batch insertion completed')
                except:
                    continue
            if len(list_tweets) != 0:
            	print("Inserting last data contained in the buffer...")
            	tweets_db.tweets.insert_many(list_tweets)
            print("Insertion of {0:s} completed".format(entry))
            f.close() # close file handle

client.close # close db connection