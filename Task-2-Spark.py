from pyspark import SparkContext
import json
import sys
from datetime import datetime

# Get input arguments
input_path = sys.argv[1]
hour_filter = int(sys.argv[2])
top_n = int(sys.argv[3])
output_path = sys.argv[4]

sc = SparkContext(appName="Task2-HashtagCount")

# Parse timestamp
def parse_created_at(tweet):
    try:
        dt = datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S %z %Y")
        return dt
    except:
        return None

# Read and parse NDJSON
tweets = sc.textFile(input_path).map(lambda line: json.loads(line))

# Get valid timestamps
timestamps = tweets.map(lambda t: parse_created_at(t)).filter(lambda dt: dt is not None)

# Collect start and end date (output-1)
start_end = timestamps.map(lambda dt: dt.date().isoformat()).distinct().collect()
start_date = min(start_end)
end_date = max(start_end)

# Filter tweets for the specific hour
filtered = tweets.filter(lambda t: (
    'created_at' in t and
    parse_created_at(t) is not None and
    parse_created_at(t).hour == hour_filter
))

# Extract hashtags for that hour
hashtags = filtered.flatMap(lambda tweet: [
    tag['text'].lower() for tag in tweet.get('entities', {}).get('hashtags', [])
])

# Count and sort
top_hashtags = (hashtags.map(lambda tag: (tag, 1))
                        .reduceByKey(lambda a, b: a + b)
                        .takeOrdered(top_n, key=lambda x: -x[1]))

# Save both outputs to the output directory
sc.parallelize([f"{start_date}", f"{end_date}", f"{hour_filter}"]).saveAsTextFile(f"{output_path}/meta")
sc.parallelize([tag for tag, _ in top_hashtags]).saveAsTextFile(f"{output_path}/hashtags")
