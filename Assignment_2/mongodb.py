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

#match = re.compile('tweets_*',re.IGNORECASE)
entries = os.listdir(TWEETS_DIRECTORY)

for entry in entries:   
    #if (match.search(entry) is not None):
     if (entry == 'tweets_1.txt'):
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

%%bash
mongo tweets_db
var mapper = function() {
    var pronouns = ["HAN", "HON", "HEN", 'DEN', 'DET', 'DENNA', 'DENNE'];
    db.tweets.createIndex({ text: "text" });
    if (this.text) {
        for (var counter = 0; counter < pronouns.length; counter++) {
            var match = 0;
            match = this.find( { $text: { $search: "\"" +pronouns[counter]+ "\"" } }).count();
            if (match !== 0) {
                emit(pronouns[counter], 1);
            } else {
                emit(pronouns[counter], 0);
            }
        }
        emit('tweets_count', 1);
    }
}

var reducer = function (key,values) {
    return Array.sum(values);
}

db.tweets.mapReduce(mapper, reducer, { query: { }, out: "pronouns_count" })
db.pronouns_count.find()