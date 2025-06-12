import os
import re
import argparse
from pyspark.sql import SparkSession
from collections import Counter

def main():
    parser = argparse.ArgumentParser(description="Inverted Index Spark.")
    parser.add_argument("-i", "--input-parquet", type=str, required=True)
    parser.add_argument("-o", "--output-dir", type=str, required=True)
    args = parser.parse_args()

    spark = initialize_spark()
    inverted_index(args.input_parquet, args.output_dir, spark)
    spark.stop()

def inverted_index(input_parquet, output_dir, spark):
    parquet_df = spark.read.parquet(input_parquet)

    rdd = parquet_df.rdd.map(lambda row: (row['filename'], row['content'])).repartition(24).persist()

    tokenizer = re.compile(r'\W+')

    local_word_counts = rdd.flatMap(lambda pair: [
        (token, [(pair[0], count)])
        for token, count in Counter(
            token for token in tokenizer.split(pair[1].lower()) if token
        ).items()
    ])

    inverted_rdd = local_word_counts.reduceByKey(lambda a, b: a + b)

    inverted_rdd.saveAsTextFile(output_dir)

def initialize_spark():
    spark = (SparkSession.builder
        .master("yarn")
        .appName("Inverted Index RDD Integrato")
        .config("spark.executor.memory", "3g")
        .config("spark.driver.memory", "3g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.instances", "4")
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.default.parallelism", "24")
        .getOrCreate())
    return spark

if __name__ == "__main__":
    main()
