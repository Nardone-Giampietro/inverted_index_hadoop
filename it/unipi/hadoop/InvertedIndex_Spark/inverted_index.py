from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pathlib import Path
from collections import Counter
import os, re, sys, argparse

def main():
    """
    Funzione principale:
    - Passaggio degli argomenti da riga di comando.
    - Inizializza Spark.
    - Legge i file di input.
    - Costruisce l'inverted index.
    - Salva il risultato nella cartella di output.
    """
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

    # Lettura dei file come DataFrame di righe testuali, aggiungendo il nome del file
    df = spark.read.text(args.input_dir).withColumn("filename", input_file_name())

    # Conversione in RDD: (nomefile, linea di testo)
    rdd = df.rdd.map(lambda row: (
        os.path.basename(row.filename),
        row.value
    ))

    # Costruzione dell'inverted index
    inverted_index(rdd, args.output_dir)
    spark.sparkContext.stop()


def inverted_index(rdd, output_dir):
    """
    Costruisce l'inverted index da un RDD di (filename, linea).
    Salva il risultato su HDFS o filesystem locale.

    :param rdd: RDD contenente tuple (filename, line)
    :param output_dir: directory in cui salvare il risultato
    """
    tokenizer = re.compile(r'\W+')

    # Fase 1: conteggio locale delle parole per file
    # Output: ((parola, nomefile), occorrenze)
    local_word_counts = (
        rdd.flatMap(lambda pair: [
            ((token, pair[0]), count)
            for token, count in Counter(
                token for token in tokenizer.split(pair[1].lower()) if token
            ).items()
        ])
    )

    word_doc_counts = local_word_counts.reduceByKey(lambda x, y: x + y)
    
    # Fase 2: somma globale delle occorrenze ((parola, file), totale)
    # Output: (parola, [(file1, count1), (file2, count2), ...])
    word_to_doc_count = (
        word_doc_counts
        .map(lambda x: (x[0][0], [(x[0][1], x[1])]))
        .reduceByKey(lambda list1, list2: list1 + list2)
    )

    # Salvataggio in output_dir (in formato testuale)
    word_to_doc_count.saveAsTextFile(output_dir)


def initialize_spark():
    """
    Inizializza e restituisce una SparkSession in modalità YARN.
    """
    spark = (
        SparkSession
        .builder
        .master("yarn")
        .appName("Inverted Index.")
        .getOrCreate()
    )
    return spark


if __name__ == "__main__":
    main()
