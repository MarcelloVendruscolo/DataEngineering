import json
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt

FILE_DIRECTORY = '/Users/marcellovendruscolo/Documents/vscode-workspace/DataEngineering/Assignment_2/0_3_mongooutput.json'
df = pd.read_json(FILE_DIRECTORY)

index_total_tweets = df['_id'].loc[lambda x: x=='tweets_count'].index[0]
total_tweets = df['value'][index_total_tweets]
df = df.drop([index_total_tweets])

df.columns = ['pronouns', 'frequency']

pronouns = np.array(df['pronouns'])
x = np.arange(len(pronouns))
width = 0.25

frequency = np.array(df['frequency'])
frequency = frequency/total_tweets

fig, ax = plt.subplots()
rects1 = ax.bar(x, frequency, width, color='green')

ax.set_ylabel('Counts')
ax.set_title('Normalised pronoun count in tweets (using MongoDB)')
ax.set_xticks(x)
ax.set_xticklabels(pronouns)

def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{0:.2f}'.format(height),
                    xy=(rect.get_x() + rect.get_width(), height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

autolabel(rects1)
fig.tight_layout()
plt.show()