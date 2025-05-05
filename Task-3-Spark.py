from pyspark import SparkContext
import json
import sys
from collections import Counter

input_path = sys.argv[1]
output_path = sys.argv[2]

sc = SparkContext(appName="Task3-BotDetection")

# Load and parse tweets
tweets = sc.textFile(input_path).map(lambda line: json.loads(line))

# Filter out invalid tweets
valid_tweets = tweets.filter(lambda t: t.get("user") and t.get("text"))

# Create (user_id, (text, location, device)) tuples
user_tweets = valid_tweets.map(lambda t: (
    t["user"]["id_str"],
    (t["text"], 
     t.get("user", {}).get("location", "Unknown"),
     t.get("source", "Unknown"))
))

# Group by user and aggregate tweet frequency and content similarity
def detect_bot(data):
    user_id, entries = data
    texts, locations, sources = zip(*entries)
    tweet_count = len(texts)
    common_text = Counter(texts).most_common(1)[0][1] if texts else 0
    common_loc = Counter(locations).most_common(1)[0][0] if locations else "Unknown"
    common_dev = Counter(sources).most_common(1)[0][0] if sources else "Unknown"

    # Simple heuristic: potential bot if >50 tweets or many duplicate tweets
    if tweet_count >= 50 or common_text >= tweet_count * 0.5:
        return (user_id, f"{common_loc}\t{common_dev}")
    return None

bots = (user_tweets
        .groupByKey()
        .mapValues(list)
        .map(detect_bot)
        .filter(lambda x: x is not None)
        .map(lambda x: f"{x[0]}\t{x[1]}"))

# Save result
bots.saveAsTextFile(output_path)
