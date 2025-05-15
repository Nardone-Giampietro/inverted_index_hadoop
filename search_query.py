import os
import argparse


def file_contains_all_terms(filepath, terms):
    """
    Scansiona il file riga per riga, rimuovendo da `terms_remaining`
    ogni termine trovato; termina appena è vuoto.
    """
    terms_remaining = set(terms)
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.lower()
                # itero solo sui termini ancora mancanti
                for term in list(terms_remaining):
                    if term in line:
                        terms_remaining.remove(term)
                if not terms_remaining:
                    return True
    except Exception as e:
        # se non riesce a leggere il file, lo skippo
        print(f"[WARN] Cannot read {filepath}: {e}")
    return False


def search_files(folder_path, terms):
    """
    Usa os.scandir per iterare più velocemente e cerca ogni file.
    """
    matches = []
    with os.scandir(folder_path) as it:
        for entry in it:
            if entry.is_file():
                if file_contains_all_terms(entry.path, terms):
                    matches.append(entry.name)
    return sorted(matches)


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Search all files in a folder for ALL query terms"
    )
    p.add_argument("folder", help="Path to folder containing text files")
    p.add_argument(
        "query", nargs="+", help="One or more terms to search for (space-separated)"
    )
    args = p.parse_args()

    # Preparo i termini in lower-case
    terms = [t.lower() for t in args.query]
    results = search_files(args.folder, terms)

    if results:
        print("Matching files:")
        for fn in results:
            print(" -", fn)
    else:
        print("No files matched your query.")
