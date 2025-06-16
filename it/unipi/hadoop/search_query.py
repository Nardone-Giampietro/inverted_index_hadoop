import os
import argparse


def file_contains_all_terms(filepath, terms):
    """
    Scansiona il file riga per riga. Restituisce True solo se tutti
    i termini specificati sono presenti nel file.
    La ricerca è ottimizzata per fermarsi non appena tutti i termini sono stati trovati.
    """

    terms_remaining = set(terms)

    try:

        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:

                line = line.lower()

                for term in list(terms_remaining):
                    if term in line:
                        terms_remaining.remove(term)

                if not terms_remaining:
                    return True
    except Exception as e:

        print(f"[WARN] Cannot read {filepath}: {e}")

    return False


def search_files(folder_path, terms):
    """
    Cerca in tutti i file di una cartella specificata.
    Usa os.scandir per migliori prestazioni rispetto a os.listdir.
    Restituisce un elenco ordinato dei nomi dei file che contengono tutti i termini di ricerca.
    """
    matches = []
    try:
        with os.scandir(folder_path) as it:
            for entry in it:

                if entry.is_file():
                    if file_contains_all_terms(entry.path, terms):
                        matches.append(entry.name)
    except FileNotFoundError:
        print(f"[ERROR] The folder '{folder_path}' was not found.")
        return []

    return sorted(matches)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Search for files in a folder that contain ALL the specified query terms."
    )
    parser.add_argument("folder", help="Path to the folder to search in.")
    parser.add_argument(
        "query", nargs="+", help="One or more space-separated terms to search for."
    )
    args = parser.parse_args()

    search_terms = [t.lower() for t in args.query]

    results = search_files(args.folder, search_terms)

    for filename in results:
        print(filename)
