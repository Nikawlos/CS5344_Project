import pandas as pd

# Merge rating datasets
num_datasets = 18
df_whole = pd.DataFrame()
for i in range(num_datasets):
    print("Reading dataset ", i+1)
    df = pd.read_csv(open(f'./RedditDataset/rating_dataset/not merged/reddit_rating_{i}.csv', encoding='utf-8',errors='ignore'))
    if i == 0:
        df_whole = df
    else:
        df_whole = pd.concat([df_whole, df], axis=0)

print(len(df_whole))
print("Writing file")
df_whole.to_csv("./RedditDataset/rating_dataset/reddit_rating_merged.csv", index=False)



