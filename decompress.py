import os
from tqdm import tqdm
import gzip
import shutil

def decompress_files(date):
    base_folder_path = f'D:\\VD_data\\{date}'
    compressed_folder_path = os.path.join(base_folder_path, 'compressed')
    decompressed_folder_path = os.path.join(base_folder_path, 'decompressed')
    log_file_path = os.path.join(base_folder_path, 'download_issues.log')
    
    os.makedirs(decompressed_folder_path, exist_ok=True)

    # List all .xml.gz files in the compressed folder
    compressed_files = [f for f in os.listdir(compressed_folder_path) if f.endswith('.xml.gz')]
    total_files = len(compressed_files)
    print("Decompressing xml.gz files...")

    # Progress bar setup
    with tqdm(total=total_files) as progress:
        for file in compressed_files:
            compressed_file_path = os.path.join(compressed_folder_path, file)
            decompressed_file_path = os.path.join(decompressed_folder_path, file[:-3])  # Remove .gz from filename

            # Skip if decompressed file already exists
            if os.path.exists(decompressed_file_path):
                print(f'Skipped: {file} (Already decompressed)')
                progress.update(1)
                continue

            try:
                # Decompress file
                with gzip.open(compressed_file_path, 'rb') as f_in, open(decompressed_file_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                #print(f'Decompressed: {file}')
            except Exception as e:
                with open(log_file_path, 'a') as log_file:
                    log_file.write(f'Failed to decompress {file}: {e}\n')
                print(f'Failed to decompress: {file}')
            progress.update(1)

    print("Decompression process completed.")