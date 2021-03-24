import numpy as np
import pandas as pd
import csv
import matplotlib
import matplotlib.pyplot as plt

FILE_DIRECTORY = '/Users/marcellovendruscolo/Documents/vscode-workspace/DataEngineering/Assignment_2/modified_wordcount_part-00000'
df = pd.read_csv(FILE_DIRECTORY, sep='\t', quoting=csv.QUOTE_NONE, encoding='utf-8', header=None)
df.columns = ['first_letter', 'frequency']

index_a = df['first_letter'].loc[lambda x: x=='a'].index[0]
index_z = df['first_letter'].loc[lambda x: x=='z'].index[0]

letters = np.array(df['first_letter'][index_a:index_z+1])
x = np.arange(len(letters))
width = 0.5

frequency = np.array(df['frequency'][index_a:index_z+1])

fig, ax = plt.subplots()
rects1 = ax.bar(x, frequency, width=0.3, color='red')

ax.set_ylabel('Counts')
ax.set_title('Modified (FirstLetter) Word Count')
ax.set_xticks(x)
ax.set_xticklabels(letters)

def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width(), height),
                    xytext=(-2, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

autolabel(rects1)
fig.tight_layout()
plt.show()