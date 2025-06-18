import os
import argparse
import time


def file_contains_all_terms(filepath, terms):
    """
    Scansiona il file riga per riga, restituendo True non appena
    tutti i termini (substring match su lowercased) sono stati trovati.
    """
    terms_remaining = set(terms)

    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                lower = line.lower()

                for term in list(terms_remaining):
                    if term in lower:
                        terms_remaining.remove(term)
                if not terms_remaining:
                    return True
    except Exception:
        return False

    return False


def search_files(folder_path, terms):
    """
    Scansiona tutti i file in folder_path e restituisce i nomi
    di quelli che contengono *tutti* i termini (AND substring).
    """
    matches = []
    try:
        with os.scandir(folder_path) as it:
            for entry in it:
                if entry.is_file() and file_contains_all_terms(entry.path, terms):
                    matches.append(entry.name)
    except FileNotFoundError:
        print(f"[ERROR] The folder '{folder_path}' was not found.")
        return []
    return sorted(matches)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Search for files containing ALL the query terms (non-parallel, fast substring match)."
    )
    parser.add_argument("folder", help="Cartella di input da scandire")
    parser.add_argument("query", nargs="+", help="Termini da cercare (spazio-separati)")
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Stampa solo il tempo, non i nomi dei file",
    )
    args = parser.parse_args()

    terms = [t.lower() for t in args.query]

    start = time.perf_counter()
    results = search_files(args.folder, terms)
    elapsed = time.perf_counter() - start

    if not args.quiet:
        for fn in results:
            print(fn)

    print(
        f"\nTempo totale: {elapsed:.4f} secondi su '{args.folder}' ({len(results)} match)"
    )
