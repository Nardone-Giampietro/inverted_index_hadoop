import os
import re
import argparse
import time


def file_contains_all_terms(filepath, terms):
    terms_remaining = set(terms)
    patterns = {term: re.compile(rf"\b{re.escape(term)}\b") for term in terms_remaining}

    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                lower = line.lower()
                for term in list(terms_remaining):
                    if patterns[term].search(lower):
                        terms_remaining.remove(term)
                if not terms_remaining:
                    return True
    except Exception:
        pass

    return False


def search_files(folder_path, terms):
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
        description="Search for files in a folder that contain ALL the specified query terms as whole words."
    )
    parser.add_argument("folder", help="Path to the folder to search in.")
    parser.add_argument(
        "query", nargs="+", help="One or more space-separated terms to search for."
    )
    args = parser.parse_args()

    search_terms = [t.lower() for t in args.query]

    start = time.perf_counter()
    results = search_files(args.folder, search_terms)
    elapsed = time.perf_counter() - start

    for fn in results:
        print(fn)

    print(
        f"\nTempo totale: {elapsed:.4f} secondi su '{args.folder}' ({len(results)} match)"
    )
