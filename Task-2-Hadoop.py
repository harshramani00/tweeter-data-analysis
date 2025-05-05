#!/usr/bin/env python3
import sys
import json
from collections import defaultdict
from datetime import datetime

# Globals for shared logic
hour_filter = None
top_n = None
start_date = None
end_date = None

def parse_created_at(created_at):
    try:
        return datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
    except:
        return None

def mapper():
    global start_date, end_date, hour_filter

    hour_filter = int(sys.argv[2])
    dates = []

    for line in sys.stdin:
        try:
            tweet = json.loads(line.strip())
            created_at = tweet.get("created_at")
            dt = parse_created_at(created_at)
            if dt:
                dates.append(dt)
                if dt.hour == hour_filter:
                    hashtags = tweet.get("entities", {}).get("hashtags", [])
                    for tag in hashtags:
                        text = tag.get("text")
                        if text:
                            print(f"{text.lower()}\t1")
        except:
            continue

    # Emit start/end date to stderr for reducer to fetch later (or record manually)
    if dates:
        start = min(dates).strftime("%Y-%m-%d")
        end = max(dates).strftime("%Y-%m-%d")
        sys.stderr.write(f"{start},{end},{hour_filter}\n")

def reducer():
    global top_n
    top_n = int(sys.argv[2])
    hashtag_counts = defaultdict(int)

    for line in sys.stdin:
        try:
            hashtag, count = line.strip().split('\t')
            hashtag_counts[hashtag] += int(count)
        except:
            continue

    # Sort and get top N
    sorted_tags = sorted(hashtag_counts.items(), key=lambda x: -x[1])[:top_n]

    # Output-2: just hashtags
    for tag, _ in sorted_tags:
        print(tag)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.stderr.write("Usage: task2_hadoop.py mapper|reducer <hour|top_n>\n")
        sys.exit(1)

    if sys.argv[1] == "mapper":
        mapper()
    elif sys.argv[1] == "reducer":
        reducer()
