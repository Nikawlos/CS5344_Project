import pandas as pd
import datetime
import math

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

# ********** Iteratively process all splited dataset *************
total_chunk = 18
for i in range(18):
    df = pd.read_csv(open('./RedditDataset/splited_dataset/splited_reddit_dataset_{}.csv'.format(i), encoding='utf-8', errors='ignore'))
    df_rating = pd.DataFrame(columns=["date"])

    emotion_scores = {}
    for emotion in emotions:
        emotion_scores[emotion] = []

    num_rows = len(df)
    for k in range(num_rows):
        tweet = df.iloc[k]["body"]
        df_rating.loc[k] = df.iloc[k]["created_utc"]
        calculate_emotion_scores(tweet, emotion_lexicons, emotion_scores)

    for emotion in emotions:
        df_rating[emotion] = emotion_scores[emotion]

    df_rating.to_csv(f"./RedditDataset/rating_dataset/reddit_rating_{i}.csv", index=False)
    i += 1

