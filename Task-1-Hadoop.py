#!/usr/bin/env python3
import sys
import json
from collections import defaultdict

def extract_langs(stream):
    for line in stream:
        try:
            tweet = json.loads(line)
            lang = tweet.get("lang")
            if lang:
                yield lang
        except json.JSONDecodeError:
            continue

def mapper():
    for lang in extract_langs(sys.stdin):
        print(f"{lang}\t1")

def reducer():
    lang_counts = defaultdict(int)
    for line in sys.stdin:
        try:
            lang, count = line.strip().split('\t')
            lang_counts[lang] += int(count)
        except:
            continue
    for lang, count in lang_counts.items():
        print(f"{lang}\t{count}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("Specify either mapper or reducer\n")
        sys.exit(1)
    if sys.argv[1] == "mapper":
        mapper()
    elif sys.argv[1] == "reducer":
        reducer()
