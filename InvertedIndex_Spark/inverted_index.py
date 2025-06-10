from pyspark.sql import SparkSession
from pathlib import Path
import os, re, sys, argparse

def main():
    parser = argparse.ArgumentParser(
        description="Inverted Index con Spark."
    )
    parser.add_argument(
        "-i", "--input-dir",
        type=str,
        required=True,
        help="Percorso di un file o di una cartella di input."
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        required=True,
        help="Percorso della cartella di output."
    )

    args = parser.parse_args()
    spark = initialize_spark()
    inverted_index(args.input_dir, args.output_dir, spark.sparkContext)
    spark.sparkContext.stop()


def inverted_index(input_dir, output_dir, context):
    collection_rdd = context.wholeTextFiles(input_dir)
    collection_rdd = (
        collection_rdd
            .flatMap(lambda pair: [
                ((token, os.path.basename(pair[0])), 1) 
                for token in re.split(r'\W+', pair[1].lower())
                if token and token != ""
            ])
            .reduceByKey(lambda x, y: x + y)
            .map(lambda x: (x[0][0], (x[0][1], x[1])))
            .groupByKey()  
            .mapValues(list)
    )
    collection_rdd.repartition(1).saveAsTextFile(output_dir)

def initialize_spark():
    spark = (SparkSession
        .builder
#        .master("local[*]")
        .master("yarn")
        .appName("Inverted Index.")
#        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )
    return spark


if __name__ == "__main__":
    main()
