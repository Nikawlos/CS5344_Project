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

# ********** Process certain splited dataset *************
print("Start!")
time_start = datetime.datetime.now()

indice = [17]
for index in indice:
    df = pd.read_csv(open('./RedditDataset/splited_dataset/splited_reddit_dataset_{}.csv'.format(index), encoding='utf-8',errors='ignore'))

    chunk_size = 20000
    num_chunk = math.ceil(len(df)/chunk_size)
    num_rows = len(df)
    for i in range(num_chunk):
        print("---------------------")
        print("Doing chunk", i+1, "/", num_chunk, "for dataset ", index)
        chunk_start = datetime.datetime.now()

        prefix = chunk_size*i

        df_rating = pd.DataFrame(columns=["date"])
        emotion_scores = {}
        for emotion in emotions:
            emotion_scores[emotion] = []

        if i == num_chunk-1:
            chunk_size = num_rows-prefix

        for k in range(chunk_size):
            tweet = df.iloc[k+prefix]["body"]
            df_rating.loc[k+prefix] = df.iloc[k+prefix]["created_utc"]
            calculate_emotion_scores(tweet, emotion_lexicons, emotion_scores)

        for emotion in emotions:
            df_rating[emotion] = emotion_scores[emotion]

        df_rating.to_csv("./RedditDataset/rating_dataset/reddit_rating_{}_{}.csv".format(index, i), index=False)

        chunk_end = datetime.datetime.now()
        print("Chunk {} starts at：".format(i+1), chunk_start)
        print("Chunk {} ends at：".format(i+1), chunk_end)

print("finished")

time_end = datetime.datetime.now()
print("Start at：", time_start)
print("End at：", time_end)

