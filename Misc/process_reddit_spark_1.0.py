'''
working,  now can follow the correct stopwords.txt

[['comment', 'hi1vs5n', '2qwzb', 'pregnant', 'FALSE', '1635206397', 'https://old.reddit.com/r/pregnant/comments/qfsajl/done_with_toxic_people_posting_false_information/hi1vs5n/', 'vaccinated special daughter fever risk vaccine fever strong antibodies covid pregnant blood tests check difference anti vaccine deciding wait based factors', '0.672', '1'], ['comment', 'hi1vsag', '2riyy', 'nova', 'FALSE', '1635206399', 'https://old.reddit.com/r/nova/comments/qfs53d/2h_waittime_for_a_scheduled_booster_shot_at/hi1vsag/', 'scheduled booster cvs option vaccines vaccine brand booster', '0', '2'], ['comment', 'hi1vs7i', '2qhov', 'vancouver', 'FALSE', '1635206397', 'https://old.reddit.com/r/vancouver/comments/qft2x3/bc_takes_note_as_new_zealand_moves_to_ban/hi1vs7i/', 'prices zealand canada grew rate covid agree ownership stopped going change', '0.1887', '32'], ['comment', 'hi1vs5v', '2qixm', 'startrek', 'FALSE', '1635206397', 'https://old.reddit.com/r/startrek/comments/qftvn2/so_i_saw_dune_last_night_and_just_got_done/hi1vs5v/', 'duty starfleet officer truth scientific truth historical truth personal truth guiding principle starfleet based find stand truth happened deserve wear uniform captain picard duty https reddit admins ineffectual response misinformation https lieu reddit gold awards donate response fund https respect subreddit rules https llap bot action performed automatically contact moderators subreddit questions concerns', '0.9562', '1']]


'''



# Import necessary libraries
import re
from pyspark import SparkContext
from nltk.tokenize import word_tokenize
import nltk
nltk.download('punkt')  # Download the NLTK data

# Create a Spark context
sc = SparkContext("local", "ReadCSV")

# Read the CSV file as an RDD of lines
lines = sc.textFile("the-reddit-covid-dataset-comments.csv")

# Skip the first line (header) of the CSV file
header = lines.first()
lines = lines.filter(lambda line: line != header)

# Initialize the group ID
group_id = None

# Define a function to assign group IDs based on 'comment' lines
def assign_group_id(line):
    if line.startswith("comment"):
        assign_group_id.group_id = line
    return (assign_group_id.group_id, line)

# Initialize the group_id attribute in the function
assign_group_id.group_id = group_id

# Assign group IDs to lines
grouped_lines = lines.map(assign_group_id)

# Group lines by the assigned group ID
grouped_lines = grouped_lines.groupByKey()

# Extract only the values, discarding the keys
values_only_rdd = grouped_lines.values()

# Function to join all strings within an RDD and flatten the result
def flatten_and_join(iterable):
    flat_elements = []
    for element in iterable:
        flat_elements.extend(element.split(','))
    return flat_elements

# Apply the flatten_and_join function to each RDD to flatten and join the strings
flattened_rdd = values_only_rdd.map(flatten_and_join)

# Tokenize the comment text within the RDD while keeping the rest of the elements
def tokenize_comment_text(line):
    text=[]
    #line = list(line)  # Convert ResultIterable to a list
    comment_text = line[7:-2]  # Get the comment text elements
    comment_text = " ".join(comment_text)  # Join the comment text elements into a single string
    tokens = [word_tokenize(comment_text)]  # Tokenize the comment text
    line[7:-2] = tokens  # Replace the original comment text with tokens
    return line

# Tokenize the comment text in each element of the RDD while keeping the rest of the elements
tokenized_rdd = flattened_rdd.map(tokenize_comment_text)



# Function to remove stopwords from the comment text
def remove_stopwords(line):
    from nltk.corpus import stopwords
    stop_words = set(stopwords.words('english'))
    comment_text = line[7]  # Get the comment text elements
    # Remove stopwords
    comment_text = [word.lower() for word in comment_text if word.isalpha() and word.lower() not in stop_words]
    line[7] = ' '.join(comment_text)  # Replace the original comment text with tokens without stopwords
    return line

# Read custom stopwords from a text file
with open("stopwords.txt", "r") as file:
    custom_stopwords = [line.strip() for line in file]

# Function to remove stopwords from the comment text
def remove_stopwords(line):
    comment_text = line[7]  # Get the comment text elements
    # Remove stopwords using your custom list
    comment_text = [word.lower() for word in comment_text if word.isalpha() and word.lower() not in custom_stopwords]
    line[7] = ' '.join(comment_text)  # Replace the original comment text with tokens without custom stopwords
    return line



# Apply the remove_stopwords function to the comments in the RDD
rdd = tokenized_rdd.map(remove_stopwords)

# Print the resulting RDD
print(rdd.collect()[:5])

# Stop the Spark context
sc.stop()
