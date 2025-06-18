from merging import merge_files

datasets = [
    ("gutenberg_books", "data_8KB", 0.008, 126022),
    ("gutenberg_books", "data_8MB", 8, 126022),
    ("gutenberg_books", "data_512MB", 512, 126022),
]

for inp, out, size_mb, maxf in datasets:
    print(f"Building {out} → target {size_mb} MB …")
    merge_files(inp, out, size_mb, maxf)
