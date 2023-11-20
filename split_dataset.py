import pandas as pd

# Split original dataset
chunksize = 1000000
reader = pd.read_csv(open('./RedditDataset/the-reddit-covid-dataset-comments.csv', encoding='utf-8', errors='ignore'), usecols=["created_utc", "body", "sentiment", "score"], chunksize=chunksize)

threshold_date = "2019-10-25 00:00:00"
threshold_timestamp = pd.Timestamp(threshold_date)

i = 0
for chunk in reader:
    print("Iteration: ", i+1)
    chunk["created_utc"] = pd.to_datetime(chunk["created_utc"], unit="s")
    first_row = chunk.iloc[0]
    latest_date = first_row["created_utc"]
    if latest_date < threshold_timestamp:
        break
    chunk.to_csv("./RedditDataset/splited_reddit_dataset_{}.csv".format(i), index=False)
    i += 1
print("finished")



# Remove tweets before 2019-10-25 00:00:00
"""
threshold_date = "2019-10-25 00:00:00"
threshold_timestamp = pd.Timestamp(threshold_date)

last_df = pd.read_csv(open('./RedditDataset/splited_dataset/splited_reddit_dataset_17.csv', encoding='utf-8', errors='ignore'))

flag = True
upper = 0
lower = len(last_df)-1
index = 0
print(len(last_df))
while flag:

    index = int((upper + lower) / 2)
    print("new index: ", index)
    date = last_df.iloc[index]["created_utc"]
    print("current date: ", date )
    if abs(upper - lower) < 5:
        flag = False
        break
    elif pd.Timestamp(date) < threshold_timestamp:
        lower = index
    else:
        upper = index

print("Out")
last_df_processed = last_df.iloc[:index]
last_df_processed.to_csv("./RedditDataset/splited_dataset/splited_reddit_dataset_17.csv", index=False)
"""


# Compute emotion rating (currently we sample one from one hundred records)
emotion_lexicons = {}
emotions = ['anger', 'anticipation', 'disgust', 'fear', 'joy', 'negative', 'positive', 'sadness', 'surprise', 'trust']

for emotion in emotions:
    lexicon = pd.read_csv("./LexiconFiles/"+emotion+'-NRC-Emotion-Lexicon.txt', sep="\t", header=None)
    lexicon = lexicon.rename(columns={0: "word", 1: "present"})
    lexicon = lexicon[lexicon['present'] == 1]
    emotion_lexicons[emotion] = lexicon

def contains_word(s, w):
    return (' ' + w + ' ') in (' ' + s + ' ')

def calculate_emotion_scores(text, emotion_lexicons, emotion_scores):
    if len(text) > 0:
        num_words = len(text.split())
        for emotion in emotions:
            emotion_score = 0
            lexicon = emotion_lexicons[emotion]
            for word in lexicon['word'].tolist():
                if contains_word(text, word):
                    emotion_score += 1
            emotion_score = round(emotion_score / num_words, 4)
            emotion_scores[emotion].append(emotion_score)



