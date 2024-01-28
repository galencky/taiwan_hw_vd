import os
import requests
import concurrent.futures
from tqdm.notebook import tqdm
import gzip
import shutil
import pandas as pd
import xml.etree.ElementTree as ET
from IPython.display import display, HTML

# Specify the path to the directory you want to create
directory_path = f'D:\\VD_data'

# Check if the directory already exists
if not os.path.exists(directory_path):
    # Create the directory if it does not exist
    os.makedirs(directory_path)
    print(f"Directory '{directory_path}' created successfully.")
else:
    print(f"Directory '{directory_path}' already exists.")

#########################################################################################

def download_file(url, file_path, log_file_path):
    """Download a single file, check its size, and return the status."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        with open(file_path, 'wb') as file:
            file.write(response.content)

        # Check file size (< 1KB)
        if os.path.getsize(file_path) < 1024:
            os.remove(file_path)
            with open(log_file_path, 'a') as log_file:
                log_file.write(f'Deleted: File too small (<1KB): {url}\n')
            print(f'Deleted: {url} (File too small)')
            return url, 'small'
        #print(f'Downloaded: {url}')
        return url, True
    except requests.RequestException as e:
        with open(log_file_path, 'a') as log_file:
            log_file.write(f'Failed to download {url}: {e}\n')
        print(f'Failed to download: {url}')
        return url, False

def download_files_for_day(date, max_concurrent_downloads=10):
    print(f"Starting download for date: {date}")
    base_folder_path = f'D:\\VD_data\\{date}'
    compressed_folder_path = os.path.join(base_folder_path, 'compressed')
    os.makedirs(compressed_folder_path, exist_ok=True)
    log_file_path = os.path.join(base_folder_path, 'download_issues.log')

    # Prepare the download tasks
    download_tasks = []
    skipped_files = 0
    for hour in range(24):
        for minute in range(60):
            current_time = f'{hour:02d}{minute:02d}'
            url = f'https://tisvcloud.freeway.gov.tw/history/motc20/VD/{date}/VDLive_{current_time}.xml.gz'
            file_path = os.path.join(compressed_folder_path, f'VDLive_{current_time}.xml.gz')
            if os.path.exists(file_path):
                skipped_files += 1
                #print(f'Skipped: {url} (File already exists)')
            else:
                download_tasks.append((url, file_path))
    if skipped_files > 0:
        print(f'Skipped {skipped_files} files. (File already exists)')

    # Download files concurrently with a progress bar
    failed_downloads = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_downloads) as executor, tqdm(total=len(download_tasks) + skipped_files) as progress:
        progress.update(skipped_files)
        future_to_url = {executor.submit(download_file, url, file_path, log_file_path): url for url, file_path in download_tasks}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                _, result = future.result()
                if result != True:
                    failed_downloads.append((url, os.path.join(compressed_folder_path, url.split('/')[-1])))
                progress.update(1)
            except Exception as e:
                failed_downloads.append((url, os.path.join(compressed_folder_path, url.split('/')[-1])))
                print(f'Error during download: {url}')
                progress.update(1)

    # Retry failed downloads
    if len(failed_downloads) > 0:
        print("Retrying failed downloads...")
        with tqdm(total=len(failed_downloads)) as progress:
            for url, file_path in failed_downloads:
                _, result = download_file(url, file_path, log_file_path)
                if result != True:
                    with open(log_file_path, 'a') as log_file:
                        log_file.write(f'Failed to download on retry: {url}\n')
                    print(f'Failed to download on retry: {url}')
                progress.update(1)

    print("Download process completed.")