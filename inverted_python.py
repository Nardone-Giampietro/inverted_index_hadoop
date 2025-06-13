import argparse
import os
import re
from tqdm import tqdm

class WordOccurrences:
    def __init__(self, word):
        self.word = word
        self.file_counts = {}

    def add_occurrence(self, filename):
        if filename in self.file_counts:
            self.file_counts[filename] += 1
        else:
            self.file_counts[filename] = 1

    def __str__(self):
        files_str = ' '.join(f"{fname}:{count}" for fname, count in self.file_counts.items())
        return f"{self.word}    {files_str}"


class WordCollection:
    def __init__(self):
        self.words = {}

    def add_word_occurrence(self, word, filename):
        if word in self.words:
            self.words[word].add_occurrence(filename)
        else:
            word_obj = WordOccurrences(word)
            word_obj.add_occurrence(filename)
            self.words[word] = word_obj

    def __str__(self):
        return '\n'.join(str(word_obj) for word_obj in self.words.values())

def inverted_index(input_dir, output_dir):
    wc = WordCollection()

    txt_files = [filename for filename in os.listdir(input_dir) if filename.endswith('.txt')]

    for filename in tqdm(txt_files, desc="Processing files"):
        full_path = os.path.join(input_dir, filename)
        with open(full_path, 'r', encoding='utf-8') as file:
            for line in file:
                words = re.split(r'\W+', line.lower())
                for token in words:
                    if token:
                        wc.add_word_occurrence(token, filename)

    output_file = os.path.join(output_dir, 'inverted_index.txt')
    with open(output_file, 'w', encoding='utf-8') as out_file:
        out_file.write(str(wc))


def main():
    parser = argparse.ArgumentParser(
        description="Inverted Index seriale con Python"
    )
    parser.add_argument(
        "-i", "--input-dir",
        type=str,
        help="Percorso della cartella di input.",
        required=True
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        required=True,
        help="Percorso della cartella di output."
    )
    
    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    inverted_index(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
