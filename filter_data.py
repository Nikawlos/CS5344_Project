import pandas as pd

# Remove tweets before 2019-10-25 00:00:00
threshold_date = "2019-10-25 00:00:00"
threshold_timestamp = pd.Timestamp(threshold_date)

last_df = pd.read_csv(open('./RedditDataset/splited_dataset/splited_reddit_dataset_17.csv', encoding='utf-8', errors='ignore'))

flag = True
upper = 0
lower = len(last_df)-1
index = 0
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

