import argparse
import os
import sys

def main():
    """
    Dato un insieme di file di inverted index in una cartella e una query,
    restituisce i nomi dei documenti che contengono tutte le parole della query.
    """
    parser = argparse.ArgumentParser(
        description="Cerca documenti contenenti tutte le parole della query."
    )
    parser.add_argument(
        "-i", "--index-dir",
        required=True,
        help="Percorso della cartella contenente i file inverted index"
    )
    parser.add_argument(
        "-q", "--query",
        required=True,
        help="Query di ricerca"
    )

    args = parser.parse_args()
    index_dir = args.index_dir
    query_words = args.query.lower().strip().split() # Normalizzazione della query in minuscolo
    query_set = set(query_words)

    current_docs = None  # Insieme corrente dei documenti comuni a tutte le parole incontrate
    found_words = set()  # Traccia quali parole della query sono state trovate nell’indice

    if not os.path.isdir(index_dir):
        sys.exit(f"Errore: la cartella '{index_dir}' non esiste.")

    try:
        # Processa tutti i file della cartella
        for filename in os.listdir(index_dir):
            filepath = os.path.join(index_dir, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.strip().split()
                    if not parts:
                        continue
                    word = parts[0]
                    if word in query_set:
                        # Estrae solo i nomi dei file, ignorando il conteggio
                        docs = {entry.split(':', 1)[0] for entry in parts[1:]}
                        found_words.add(word)
                        if current_docs is None:
                            current_docs = docs # Prima parola trovata: inizializza l’intersezione
                        else:
                            current_docs &= docs # Intersezione con i documenti della nuova parola
                        if not current_docs:
                            break # Intersezione vuota
                        if found_words == query_set:
                            break # Tutte le parole della query sono state trovate
            if found_words == query_set:
                break  # Non serve continuare se tutte le parole sono state trovate

    except Exception as e:
        sys.exit(f"Errore durante la lettura dei file: {e}")

    if found_words == query_set and current_docs:
        print("Documenti trovati:", ", ".join(sorted(current_docs)))
    else:
        print("Nessun documento contiene tutte le parole della query.")

if __name__ == '__main__':
    main()
