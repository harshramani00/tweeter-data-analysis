# task1_nondistributed.py
import sys
import json
import time
import resource
from collections import Counter

def process_tweet(tweet, lang_counter):
    lang = tweet.get('lang')
    if lang:
        lang_counter[lang] += 1

def open_file(path, mode='r', encoding='utf-8'):
    if path.startswith("gs://"):
        import gcsfs
        fs = gcsfs.GCSFileSystem(project='cse532-a4')
        return fs.open(path, mode, encoding=encoding)
    else:
        return open(path, mode, encoding=encoding)

def main(input_path, output_path):
    start_time = time.time()

    lang_counter = Counter()

    with open_file(input_path, 'r') as f:
        for line in f:
            try:
                tweet = json.loads(line.strip())
                if isinstance(tweet, dict):
                    process_tweet(tweet, lang_counter)
            except json.JSONDecodeError:
                continue

    with open_file(output_path, 'w') as out:
        for lang, count in sorted(lang_counter.items(), key=lambda x: x[1], reverse=True):
            out.write(f"{lang}\t{count}\n")

    end_time = time.time()
    mem_usage_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

    print(f"✅ Done. {len(lang_counter)} languages found.")
    print(f"📁 Output written to: {output_path}")
    print(f"⏱️ Execution time: {end_time - start_time:.2f} seconds")
    print(f"💾 Peak memory used: {mem_usage_mb:.2f} MB")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python task1_nondistributed.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(input_path, output_path)
