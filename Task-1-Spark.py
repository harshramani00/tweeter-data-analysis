# task2_spark.py
from pyspark import SparkContext
import json

def parse_line(line):
    try:
        tweet = json.loads(line)
        lang = tweet.get('lang')
        if lang:
            return [(lang, 1)]
    except:
        pass
    return []

if __name__ == "__main__":
    sc = SparkContext(appName="Task2-LanguageCount")

    input_path = "gs://cse532-a4-bucket/data/250000-tweets-ndjson.json"
    output_path = "gs://cse532-a4-bucket/output/task2_spark_output"

    lines = sc.textFile(input_path)
    lang_counts = lines.flatMap(parse_line) \
                       .reduceByKey(lambda a, b: a + b)

    lang_counts.saveAsTextFile(output_path)
    sc.stop()
