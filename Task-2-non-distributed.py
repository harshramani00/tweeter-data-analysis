import sys
import json
from collections import Counter
from datetime import datetime
import time
import psutil
import os

def extract_hour(created_at_str):
    try:
        dt = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
        return dt.hour, dt.date()
    except:
        return None, None

def extract_hashtags(tweet):
    hashtags = []
    try:
        for ht in tweet.get("entities", {}).get("hashtags", []):
            tag = ht.get("text")
            if tag:
                hashtags.append(tag.lower())  # normalize
    except:
        pass
    return hashtags

def main(input_path, target_hour, top_n):
    start_time = time.time()
    process = psutil.Process(os.getpid())

    hashtags_counter = Counter()
    dates = []

    import gcsfs
    fs = gcsfs.GCSFileSystem(project='cse532-a4')

    with fs.open(input_path, 'r') as f:
        for line in f:
            try:
                tweet = json.loads(line.strip())
            except json.JSONDecodeError:
                continue

            created_at = tweet.get("created_at")
            if not created_at:
                continue

            hour, date = extract_hour(created_at)
            if hour == target_hour:
                hashtags = extract_hashtags(tweet)
                hashtags_counter.update(hashtags)
                if date:
                    dates.append(date)

    start_date = min(dates).isoformat() if dates else "N/A"
    end_date = max(dates).isoformat() if dates else "N/A"

    with fs.open("gs://cse532-a4-bucket/output/task2_non_dist_meta.txt", 'w') as meta_file:
        meta_file.write(f"{start_date}\n{end_date}\n{target_hour}\n")

    with fs.open("gs://cse532-a4-bucket/output/task2_non_dist_tags.txt", 'w') as tags_file:
        for tag, _ in hashtags_counter.most_common(top_n):
            tags_file.write(f"{tag}\n")

    end_time = time.time()
    mem = process.memory_info().rss / (1024 * 1024)  # in MB

    print(f"✅ Done. Execution time: {end_time - start_time:.2f} seconds")
    print(f"🧠 Peak memory usage: {mem:.2f} MB")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python task2_non_distributed.py <input_path> <hour (0-23)> <N>")
        sys.exit(1)

    input_path = sys.argv[1]
    target_hour = int(sys.argv[2])
    top_n = int(sys.argv[3])
    main(input_path, target_hour, top_n)
