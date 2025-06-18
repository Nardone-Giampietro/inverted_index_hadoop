import argparse
import os
import re
import tracemalloc
import time
from tqdm import tqdm # Libreria per la barra di avanzamento

class WordOccurrences:
    """
    Classe che rappresenta una parola e tiene traccia del numero di occorrenze nei vari file.
    """
    def __init__(self, word):
        self.word = word       # Parola
        self.file_counts = {}  # Dizionario: {nome_file: conteggio}

    def add_occurrence(self, filename):
        """
        Registra un'occorrenza della parola nel file specificato.
        """
        if filename in self.file_counts:
            self.file_counts[filename] += 1
        else:
            self.file_counts[filename] = 1

    def __str__(self):
        """
        Ritorna la rappresentazione testuale della parola e dei file in cui compare con le rispettive occorrenze.
        Esempio: "cloud    file1.txt:3 file2.txt:1"
        """
        files_str = ' '.join(f"{fname}:{count}" for fname, count in self.file_counts.items())
        return f"{self.word}    {files_str}"


class WordCollection:
    """
    Classe che raccoglie tutte le parole trovate e gestisce l'inserimento delle occorrenze.
    """
    def __init__(self):
        self.words = {}  # Dizionario: {parola: WordOccurrences}

    def add_word_occurrence(self, word, filename):
        """
        Aggiunge un'occorrenza della parola nel file specificato.
        """
        if word in self.words:
            self.words[word].add_occurrence(filename)
        else:
            word_obj = WordOccurrences(word)
            word_obj.add_occurrence(filename)
            self.words[word] = word_obj

    def __str__(self):
        """
        Restituisce l'intero inverted index come stringa.
        """
        return '\n'.join(str(word_obj) for word_obj in self.words.values())


def inverted_index(input_dir, output_dir):
    """
    Costruisce un inverted index da tutti i file .txt contenuti nella cartella di input.
    Il risultato viene salvato in 'inverted_index.txt' nella cartella di output.
    """
    wc = WordCollection()

    # Recupera tutti i file .txt nella cartella di input
    txt_files = [filename for filename in os.listdir(input_dir) if filename.endswith('.txt')]

    for filename in tqdm(txt_files, desc="Processing files"):
        full_path = os.path.join(input_dir, filename)
        with open(full_path, 'r', encoding='utf-8') as file:
            for line in file:
                # Normalizza in minuscolo e separa le parole da caratteri non alfanumerici
                words = re.split(r'\W+', line.lower())
                for token in words:
                    if token:
                        wc.add_word_occurrence(token, filename)

    # Scrive l'output su file
    output_file = os.path.join(output_dir, 'inverted_index.txt')
    with open(output_file, 'w', encoding='utf-8') as out_file:
        out_file.write(str(wc))


def main():
    """
    Funzione principale:
    - Passaggio degli argomenti da riga di comando
    - Misura il tempo di esecuzione e picco di memoria
    - Chiama la funzione che genera l'inverted index
    """
    parser = argparse.ArgumentParser(description="Inverted Index seriale con misurazione memoria e tempo esecuzione.")
    parser.add_argument("-i", "--input-dir", type=str, help="Cartella input", required=True)
    parser.add_argument("-o", "--output-dir", type=str, help="Cartella output", required=True)
    
    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    # Avvia il tracciamento
    tracemalloc.start()
    start_time = time.time()

    inverted_index(args.input_dir, args.output_dir)

    # Termina il tracciamento
    end_time = time.time()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # Output delle metriche
    print(f"\nTempo totale esecuzione: {end_time - start_time:.2f} secondi")
    print(f"Memoria massima usata: {peak / (1024**2):.2f} MB")


if __name__ == "__main__":
    main()
