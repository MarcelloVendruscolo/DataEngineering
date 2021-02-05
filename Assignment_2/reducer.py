#!/usr/bin/env python3
"""reducer.py"""

from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None

for line in sys.stdin: # input comes from STDIN
    line = line.strip() # remove leading and trailing whitespace
    word, count = line.split('\t', 1) # parse the input we got from mapper.py
    try:
        count = int(count) # convert count (currently a string) to int
    except ValueError:
        # count was not a number, so silently ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            print('{0:s}\t{1:s}'.format(current_word, str(current_count))) # write result to STDOUT
        current_count = count
        current_word = word

if current_word == word:
    print('{0:s}\t{1:s}'.format(current_word, str(current_count))) # do not forget to output the last word if needed!