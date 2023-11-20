import pandas as pd

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


# Read dataset
print("Loading dataset")
train_df_raw = pd.read_csv(open('./TwitterDataset/Corona_NLP_train.csv', encoding='utf-8', errors='ignore'))
test_df_raw = pd.read_csv(open('./TwitterDataset/Corona_NLP_test.csv', encoding='utf-8', errors='ignore'))

train_df = train_df_raw[["Location", "TweetAt", "OriginalTweet", "Sentiment"]]
test_df = test_df_raw[["Location", "TweetAt", "OriginalTweet", "Sentiment"]]
print("Completed")

# Process for training dataset
print("Processing training dataset")
emotion_scores = {}
for emotion in emotions:
    emotion_scores[emotion] = []

for k in range(len(train_df)):
    tweet = train_df.iloc[k]["OriginalTweet"]
    calculate_emotion_scores(tweet, emotion_lexicons, emotion_scores)

for emotion in emotions:
    train_df_raw[emotion] = emotion_scores[emotion]

train_df_raw.to_csv("./TwitterDataset/twitter_train_rating.csv", index=False)
print("Done")


# Process for testing dataset
print("Processing testing dataset")
emotion_scores = {}
for emotion in emotions:
    emotion_scores[emotion] = []

for k in range(len(test_df)):
    tweet = test_df.iloc[k]["OriginalTweet"]
    calculate_emotion_scores(tweet, emotion_lexicons, emotion_scores)

for emotion in emotions:
    test_df_raw[emotion] = emotion_scores[emotion]

test_df_raw.to_csv("./TwitterDataset/twitter_test_rating.csv", index=False)
print("Done")


