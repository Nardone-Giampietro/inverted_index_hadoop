"""
Descrizione:
    Questo programma legge una directory contenente file di testo (.txt),
    e li unisce riga per riga in nuovi file chiamati merged_00.txt, merged_01.txt, ecc.

    - Ogni file merged avrà una dimensione approssimativa indicata dall'utente (in MB).
    - È possibile limitare il numero massimo di file merged da generare.
    - Le righe dei file originali possono essere spezzate tra più file merged.

Utilizzo:
    python merge_txt_files.py 
        --input-dir <cartella_input> 
        --output-dir <cartella_output> 
        --dimensione <dimensione_target_MB> 
        --max-merged <numero_massimo_merged>
"""

import os
import argparse

def merge_files(input_dir, output_dir, target_size_mb, max_merged_files):
    """
    Esegue il merge dei file txt in blocchi di dimensione approssimativa.

    Args:
        input_dir (str): Directory contenente i file txt di input.
        output_dir (str): Directory dove scrivere i file merged.
        target_size_mb (int): Dimensione target per ogni file merged, in MB.
        max_merged_files (int): Numero massimo di file merged da generare.
    """
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Convertiamo la dimensione target in byte
    target_size_bytes = target_size_mb * 1024 * 1024

    merged_index = 0
    current_size = 0
    out_file = open(os.path.join(output_dir, f'merged_{merged_index:02d}.txt'), 'w', encoding='utf-8')

    # Costruzione lista completa di tutti i file txt
    txt_files = []
    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith('.txt'):
                txt_files.append(os.path.join(root, filename))

    # Elaborazione dei file
    for file_path in txt_files:
        with open(file_path, 'r', encoding='utf-8') as infile:
            for line in infile:
                # Calcolo della dimensione effettiva della riga in byte
                encoded_line = line.encode('utf-8')
                line_size = len(encoded_line)

                # Se superiamo la dimensione target, chiudiamo il file corrente e apriamo il successivo
                if current_size + line_size > target_size_bytes:
                    out_file.close()
                    merged_index += 1

                    # Se abbiamo raggiunto il numero massimo di merged file, interrompiamo
                    if merged_index >= max_merged_files:
                        print(f"Raggiunto il limite massimo di {max_merged_files} file merged.")
                        return

                    current_size = 0
                    out_file = open(os.path.join(output_dir, f'merged_{merged_index:02d}.txt'), 'w', encoding='utf-8')

                out_file.write(line)
                current_size += line_size

    out_file.close()
    print(f"Creati {merged_index + 1} file merged in '{output_dir}'.")

def main():
    """
    Parser degli argomenti da linea di comando.
    """
    parser = argparse.ArgumentParser(description="Merge file txt in blocchi di dimensione fissata e numero massimo di file.")
    parser.add_argument("-i", "--input-dir", required=True, help="Cartella input con i file txt")
    parser.add_argument("-o", "--output-dir", required=True, help="Cartella di output per i merged")
    parser.add_argument("-d", "--dimensione", type=int, required=True, help="Dimensione target merged file in MB")
    parser.add_argument("-m", "--max-merged", type=int, required=True, help="Numero massimo di merged file da creare")

    args = parser.parse_args()
    merge_files(args.input_dir, args.output_dir, args.dimensione, args.max_merged)

if __name__ == "__main__":
    main()
