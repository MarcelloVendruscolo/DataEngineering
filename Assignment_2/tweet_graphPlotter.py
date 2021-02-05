import numpy as np
import pandas as pd
import csv
import matplotlib
import matplotlib.pyplot as plt

df = pd.read_csv('/Users/marcellovendruscolo/Documents/vscode-workspace/DataEngineering/Assignment_2/tweetres', sep='\t', quoting=csv.QUOTE_NONE, encoding='utf-8', header=None)

index_total_tweets = df[0].loc[lambda x: x=='total_tweets'].index[0]
total_tweets = df[1][index_total_tweets]
df = df.drop([index_total_tweets])

df.columns = ['pronouns', 'frequency']

pronouns = np.array(df['pronouns'])
x = np.arange(len(pronouns))
width = 0.25

frequency = np.array(df['frequency'])
frequency = frequency/total_tweets

fig, ax = plt.subplots()
rects1 = ax.bar(x, frequency, width, color='black')

ax.set_ylabel('Counts')
ax.set_title('Normalised pronoun count in Tweets')
ax.set_xticks(x)
ax.set_xticklabels(pronouns)

def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width(), height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


autolabel(rects1)
fig.tight_layout()

plt.show()

