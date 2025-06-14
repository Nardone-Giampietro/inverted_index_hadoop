import os
import argparse

def merge_files(input_dir, output_dir, target_size_mb):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    target_size_bytes = target_size_mb * 1024 * 1024

    merged_index = 0
    current_size = 0
    out_file = open(os.path.join(output_dir, f'merged_{merged_index:02d}.txt'), 'w', encoding='utf-8')

    txt_files = []
    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith('.txt'):
                txt_files.append(os.path.join(root, filename))

    for file_path in txt_files:
        with open(file_path, 'r', encoding='utf-8') as infile:
            for line in infile:
                encoded_line = line.encode('utf-8')
                line_size = len(encoded_line)

                # Se sforiamo la dimensione target, apriamo nuovo file
                if current_size + line_size > target_size_bytes:
                    out_file.close()
                    merged_index += 1
                    current_size = 0
                    out_file = open(os.path.join(output_dir, f'merged_{merged_index:02d}.txt'), 'w', encoding='utf-8')

                out_file.write(line)
                current_size += line_size

    out_file.close()
    print(f"Creati {merged_index + 1} file merged in '{output_dir}'.")

def main():
    parser = argparse.ArgumentParser(description="Merge file txt in blocchi di dimensione fissata.")
    parser.add_argument("-i","--input-dir", required=True, help="Cartella input con i file txt")
    parser.add_argument("-o", "--output-dir",  required=True, help="Cartella di output per i merged")
    parser.add_argument("-d", "--dimensione", type=int, required=True, help="Dimensione target merged file in MB")

    args = parser.parse_args()
    merge_files(args.input_dir, args.output_dir, args.dimensione)

if __name__ == "__main__":
    main()

