#!/usr/bin/env python3
"""mapper.py"""

import sys
import json
import re

for line in sys.stdin: # input comes from STDIN (standard input)
    dictionary = {'han': '0', 'hon': '0', 'hen': '0', 'den': '0', 'det': '0', 'denna': '0', 'denne': '0'}
    line = line.strip() # remove leading and trailing whitespace
    try:
        a_tweet = json.loads(line)
        if not a_tweet['retweeted']:
            text_content = a_tweet['text']
            for pronoun in dictionary.keys():
                match = re.search(r'\b'+pronoun+'\\b', text_content, re.IGNORECASE)
                if match is not None:
                    dictionary.update({pronoun: '1'}) 
                print('{0:s}\t{1:s}'.format(pronoun, dictionary.get(pronoun)))
            print('{0:s}\t{1:s}'.format('total_tweets', '1'))
    except:
        continue