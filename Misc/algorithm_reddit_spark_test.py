
# Import necessary libraries
import pyspark
from pyspark import SparkContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType
from nltk.tokenize import word_tokenize
import nltk
import pandas as pd
import datetime

def rdd_to_list(row):
    list_row = list(row)
    comment_text = " ".join(list_row[2:-2])
    new_row = [list_row[0], list_row[0], comment_text, list_row[0], list_row[0]]
    return new_row

# Helper function to match words
def contains_word(s, w):
    return (' ' + w + ' ') in (' ' + s + ' ')

# Function to calculate emotion scores for specific emotions from a predefined list
def calculate_emotion_scores(row, emotion_lexicon):
    emotion_scores = {emotion: 0 for emotion in emotions}
    text = row[2]
    if len(text) > 0:
        total_words = len(text.split())
        for emotion in emotions:
            lexicon = emotion_lexicons[emotion]
            for word in lexicon['word'].tolist():
                if contains_word(text, word):
                    emotion_scores[emotion] += 1
            emotion_scores[emotion] = round(emotion_scores[emotion] / total_words, 5) # Normalize the counts
    for emotion in emotions:
        row.append(emotion_scores[emotion])  # Store the emotion score
    return row

# Download the NLTK data
nltk.download('punkt')

# Create a Spark context
sc = SparkContext("local", "ReadCSV")
sqlContext = pyspark.SQLContext(sc)
sc._conf.set("spark.driver.memory", "8g")
sc._conf.set("spark.executor.memory", "4g")

# Define a list of emotion names
emotions = ['anger', 'anticipation', 'disgust', 'fear', 'joy', 'negative', 'positive', 'sadness', 'surprise', 'trust']

emotion_lexicons = {}
# For each emotion, read the respective emotion lexicon file, rename columns, and filter for words where 'present' is 1
for emotion in emotions:
    lexicon = pd.read_csv(emotion + '-NRC-Emotion-Lexicon.txt', sep="\t", header=None)
    lexicon = lexicon.rename(columns={0: "word", 1: "present"})
    lexicon = lexicon[lexicon['present'] == 1]
    emotion_lexicons[emotion] = lexicon


index = 17

# Read csv file through pandas
pd_df_raw = pd.read_csv(open("./dataset/splited_reddit_dataset_{}.csv".format(index), encoding="utf-8", errors="ignore"))
values = pd_df_raw.values.tolist()
columns = list(pd_df_raw.columns)
spark_df_raw = sqlContext.createDataFrame(values, columns)
rdd_raw = spark_df_raw.rdd
rdd_temp = rdd_raw.repartition(1000).map(rdd_to_list)

for emotion, lexicon in emotion_lexicons.items():
    rdd_with_emotion_scores = rdd_temp.repartition(1000).map(lambda x: calculate_emotion_scores(x, lexicon))

rdd_only_emotion_scores = rdd_with_emotion_scores.repartition(1000).map(lambda x: [x[1], x[5], x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14]])

df = sqlContext.createDataFrame(rdd_only_emotion_scores)
df.write.mode("overwrite").csv("./dataset/rating_dataset_{}.csv".format(index))

# Stop the Spark context
sc.stop()

