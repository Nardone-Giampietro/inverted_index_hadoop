import os
import argparse
from pyspark.sql import SparkSession

def initialize_spark():
    spark = (SparkSession.builder
        .master("local[*]")
        .appName("TXT to Parquet")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate())
    return spark

def txt_to_parquet(input_dir, output_dir):
    spark = initialize_spark()

    input_dir = normalize_path(input_dir)
    output_dir = normalize_path(output_dir)

    files_rdd = spark.sparkContext.wholeTextFiles(input_dir)

    files_df = files_rdd.map(lambda pair: (os.path.basename(pair[0]), pair[1])) \
                        .toDF(["filename", "content"])

    files_df.write.mode("overwrite").parquet(output_dir)

    spark.stop()


def normalize_path(path):
    abs_path = os.path.abspath(path)
    return f"file://{abs_path}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convertitore da TXT a Parquet.")
    parser.add_argument("-i", "--input-dir", type=str, required=True, help="Directory di input contenente i file TXT.")
    parser.add_argument("-o", "--output-dir", type=str, required=True, help="Percorso directory di output Parquet.")
    args = parser.parse_args()

    txt_to_parquet(args.input_dir, args.output_dir)
