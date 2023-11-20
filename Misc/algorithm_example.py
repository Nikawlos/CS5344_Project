import pandas as pd

# anger lexicon
anger_lexicon = pd.read_csv('anger-NRC-Emotion-Lexicon.txt', sep = "\t", header=None)
anger_lexicon = anger_lexicon.rename(columns={0 : "word", 1 : "present"})
anger_lexicon = anger_lexicon[anger_lexicon['present'] == 1]
anger_lexicon = anger_lexicon

# helper function to match words
def contains_word(s, w):
    return (' ' + w + ' ') in (' ' + s + ' ')

# this is only for the anger scores for each tweet/comments
anger_scores = []
for text in new_text: # new_text is a list of tweets/comments
  if len(text.split()) > 0:
    emotion_scores = {"Anger": 0}
    for word in anger_lexicon['word'].tolist():
      if contains_word(text, word):
        emotion_scores['Anger'] += 1
    emotion_scores['Anger'] = emotion_scores['Anger'] / len(text.split()) # normalize the counts
    anger_scores.append(emotion_scores)
  else:
    anger_scores.append(None)
