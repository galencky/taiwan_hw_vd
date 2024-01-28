{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Taiwan Highway VD data fetching\n",
    "\n",
    "#### Run the code first then execute the xml.gz downloader."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Directory 'D:\\VD_data' already exists.\n"
     ]
    }
   ],
   "source": [
    "import os\n",
    "import requests\n",
    "import concurrent.futures\n",
    "from tqdm.notebook import tqdm\n",
    "import gzip\n",
    "import shutil\n",
    "import pandas as pd\n",
    "import xml.etree.ElementTree as ET\n",
    "from IPython.display import display, HTML\n",
    "\n",
    "# Specify the path to the directory you want to create\n",
    "directory_path = f'D:\\\\VD_data'\n",
    "\n",
    "# Check if the directory already exists\n",
    "if not os.path.exists(directory_path):\n",
    "    # Create the directory if it does not exist\n",
    "    os.makedirs(directory_path)\n",
    "    print(f\"Directory '{directory_path}' created successfully.\")\n",
    "else:\n",
    "    print(f\"Directory '{directory_path}' already exists.\")\n",
    "\n",
    "#########################################################################################\n",
    "\n",
    "def download_file(url, file_path, log_file_path):\n",
    "    \"\"\"Download a single file, check its size, and return the status.\"\"\"\n",
    "    try:\n",
    "        response = requests.get(url)\n",
    "        response.raise_for_status()  # Raise an error for bad status codes\n",
    "        with open(file_path, 'wb') as file:\n",
    "            file.write(response.content)\n",
    "\n",
    "        # Check file size (< 1KB)\n",
    "        if os.path.getsize(file_path) < 1024:\n",
    "            os.remove(file_path)\n",
    "            with open(log_file_path, 'a') as log_file:\n",
    "                log_file.write(f'Deleted: File too small (<1KB): {url}\\n')\n",
    "            print(f'Deleted: {url} (File too small)')\n",
    "            return url, 'small'\n",
    "        #print(f'Downloaded: {url}')\n",
    "        return url, True\n",
    "    except requests.RequestException as e:\n",
    "        with open(log_file_path, 'a') as log_file:\n",
    "            log_file.write(f'Failed to download {url}: {e}\\n')\n",
    "        print(f'Failed to download: {url}')\n",
    "        return url, False\n",
    "\n",
    "def download_files_for_day(date, max_concurrent_downloads=10):\n",
    "    print(f\"Starting download for date: {date}\")\n",
    "    base_folder_path = f'D:\\\\VD_data\\\\{date}'\n",
    "    compressed_folder_path = os.path.join(base_folder_path, 'compressed')\n",
    "    os.makedirs(compressed_folder_path, exist_ok=True)\n",
    "    log_file_path = os.path.join(base_folder_path, 'download_issues.log')\n",
    "\n",
    "    # Prepare the download tasks\n",
    "    download_tasks = []\n",
    "    skipped_files = 0\n",
    "    for hour in range(24):\n",
    "        for minute in range(60):\n",
    "            current_time = f'{hour:02d}{minute:02d}'\n",
    "            url = f'https://tisvcloud.freeway.gov.tw/history/motc20/VD/{date}/VDLive_{current_time}.xml.gz'\n",
    "            file_path = os.path.join(compressed_folder_path, f'VDLive_{current_time}.xml.gz')\n",
    "            if os.path.exists(file_path):\n",
    "                skipped_files += 1\n",
    "                #print(f'Skipped: {url} (File already exists)')\n",
    "            else:\n",
    "                download_tasks.append((url, file_path))\n",
    "    if skipped_files > 0:\n",
    "        print(f'Skipped {skipped_files} files. (File already exists)')\n",
    "\n",
    "    # Download files concurrently with a progress bar\n",
    "    failed_downloads = []\n",
    "    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_downloads) as executor, tqdm(total=len(download_tasks) + skipped_files) as progress:\n",
    "        progress.update(skipped_files)\n",
    "        future_to_url = {executor.submit(download_file, url, file_path, log_file_path): url for url, file_path in download_tasks}\n",
    "        for future in concurrent.futures.as_completed(future_to_url):\n",
    "            url = future_to_url[future]\n",
    "            try:\n",
    "                _, result = future.result()\n",
    "                if result != True:\n",
    "                    failed_downloads.append((url, os.path.join(compressed_folder_path, url.split('/')[-1])))\n",
    "                progress.update(1)\n",
    "            except Exception as e:\n",
    "                failed_downloads.append((url, os.path.join(compressed_folder_path, url.split('/')[-1])))\n",
    "                print(f'Error during download: {url}')\n",
    "                progress.update(1)\n",
    "\n",
    "    # Retry failed downloads\n",
    "    if len(failed_downloads) > 0:\n",
    "        print(\"Retrying failed downloads...\")\n",
    "        with tqdm(total=len(failed_downloads)) as progress:\n",
    "            for url, file_path in failed_downloads:\n",
    "                _, result = download_file(url, file_path, log_file_path)\n",
    "                if result != True:\n",
    "                    with open(log_file_path, 'a') as log_file:\n",
    "                        log_file.write(f'Failed to download on retry: {url}\\n')\n",
    "                    print(f'Failed to download on retry: {url}')\n",
    "                progress.update(1)\n",
    "\n",
    "    print(\"Download process completed.\")\n",
    "\n",
    "#########################################################################################\n",
    "\n",
    "def decompress_files(date):\n",
    "    base_folder_path = f'D:\\\\VD_data\\\\{date}'\n",
    "    compressed_folder_path = os.path.join(base_folder_path, 'compressed')\n",
    "    decompressed_folder_path = os.path.join(base_folder_path, 'decompressed')\n",
    "    log_file_path = os.path.join(base_folder_path, 'download_issues.log')\n",
    "    \n",
    "    os.makedirs(decompressed_folder_path, exist_ok=True)\n",
    "\n",
    "    # List all .xml.gz files in the compressed folder\n",
    "    compressed_files = [f for f in os.listdir(compressed_folder_path) if f.endswith('.xml.gz')]\n",
    "    total_files = len(compressed_files)\n",
    "    print(\"Decompressing xml.gz files...\")\n",
    "\n",
    "    # Progress bar setup\n",
    "    with tqdm(total=total_files) as progress:\n",
    "        for file in compressed_files:\n",
    "            compressed_file_path = os.path.join(compressed_folder_path, file)\n",
    "            decompressed_file_path = os.path.join(decompressed_folder_path, file[:-3])  # Remove .gz from filename\n",
    "\n",
    "            # Skip if decompressed file already exists\n",
    "            if os.path.exists(decompressed_file_path):\n",
    "                print(f'Skipped: {file} (Already decompressed)')\n",
    "                progress.update(1)\n",
    "                continue\n",
    "\n",
    "            try:\n",
    "                # Decompress file\n",
    "                with gzip.open(compressed_file_path, 'rb') as f_in, open(decompressed_file_path, 'wb') as f_out:\n",
    "                    shutil.copyfileobj(f_in, f_out)\n",
    "                #print(f'Decompressed: {file}')\n",
    "            except Exception as e:\n",
    "                with open(log_file_path, 'a') as log_file:\n",
    "                    log_file.write(f'Failed to decompress {file}: {e}\\n')\n",
    "                print(f'Failed to decompress: {file}')\n",
    "            progress.update(1)\n",
    "\n",
    "    print(\"Decompression process completed.\")\n",
    "\n",
    "#########################################################################################\n",
    "\n",
    "def convert_xml_to_csv(date):\n",
    "    input_dir = f\"D:\\\\VD_data\\\\{date}\\\\decompressed\"\n",
    "    output_dir = f\"D:\\\\VD_data\\\\{date}\\\\csv\"\n",
    "\n",
    "    # Create output directory if it doesn't exist\n",
    "    os.makedirs(output_dir, exist_ok=True)\n",
    "\n",
    "    # Define the namespace\n",
    "    namespace = {'ns1': 'http://traffic.transportdata.tw/standard/traffic/schema/'}\n",
    "\n",
    "    def get_nested_element_text(parent, path):\n",
    "        element = parent.find(path, namespace)\n",
    "        return element.text if element is not None else ''\n",
    "\n",
    "    # Get the list of XML files\n",
    "    xml_files = [f for f in os.listdir(input_dir) if f.endswith('.xml')]\n",
    "    total_files = len(xml_files)\n",
    "\n",
    "    # Initialize the progress bar\n",
    "    with tqdm(total=total_files) as progress:\n",
    "        for file_name in xml_files:\n",
    "            try:\n",
    "                tree = ET.parse(os.path.join(input_dir, file_name))\n",
    "                root = tree.getroot()\n",
    "\n",
    "                # Prepare a dictionary to store data for each VDID\n",
    "                data_dict = {}\n",
    "\n",
    "                for vdlive in root.findall('.//ns1:VDLive', namespace):\n",
    "                    vdid = get_nested_element_text(vdlive, 'ns1:VDID')\n",
    "\n",
    "                    if vdid not in data_dict:\n",
    "                        data_dict[vdid] = {\n",
    "                            'VDID': vdid\n",
    "                        }\n",
    "\n",
    "                    for lane in vdlive.findall('.//ns1:Lane', namespace):\n",
    "                        lane_id = get_nested_element_text(lane, 'ns1:LaneID')\n",
    "                        speed = get_nested_element_text(lane, 'ns1:Speed')\n",
    "                        occupancy = get_nested_element_text(lane, 'ns1:Occupancy')\n",
    "\n",
    "                        data_dict[vdid][f'L{lane_id}_Speed'] = speed\n",
    "                        data_dict[vdid][f'L{lane_id}_Occupancy'] = occupancy\n",
    "\n",
    "                        # Initialize volume and speed values for S, L, T\n",
    "                        data_dict[vdid][f'L{lane_id}_S_Volume'] = 0\n",
    "                        data_dict[vdid][f'L{lane_id}_L_Volume'] = 0\n",
    "                        data_dict[vdid][f'L{lane_id}_T_Volume'] = 0\n",
    "                        data_dict[vdid][f'L{lane_id}_S_Vehicle_Speed'] = 0\n",
    "                        data_dict[vdid][f'L{lane_id}_L_Vehicle_Speed'] = 0\n",
    "                        data_dict[vdid][f'L{lane_id}_T_Vehicle_Speed'] = 0\n",
    "\n",
    "                        for vehicle in lane.findall('.//ns1:Vehicle', namespace):\n",
    "                            vehicle_type = get_nested_element_text(vehicle, 'ns1:VehicleType')\n",
    "                            volume = get_nested_element_text(vehicle, 'ns1:Volume')\n",
    "                            speed2 = get_nested_element_text(vehicle, 'ns1:Speed')\n",
    "\n",
    "                            if vehicle_type == 'S':\n",
    "                                data_dict[vdid][f'L{lane_id}_S_Volume'] = volume\n",
    "                                data_dict[vdid][f'L{lane_id}_S_Vehicle_Speed'] = speed2\n",
    "                            elif vehicle_type == 'L':\n",
    "                                data_dict[vdid][f'L{lane_id}_L_Volume'] = volume\n",
    "                                data_dict[vdid][f'L{lane_id}_L_Vehicle_Speed'] = speed2\n",
    "                            elif vehicle_type == 'T':\n",
    "                                data_dict[vdid][f'L{lane_id}_T_Volume'] = volume\n",
    "                                data_dict[vdid][f'L{lane_id}_T_Vehicle_Speed'] = speed2\n",
    "\n",
    "                # Create DataFrame from the data dictionary values\n",
    "                df = pd.DataFrame(list(data_dict.values()))\n",
    "\n",
    "                # Save the modified data to a CSV file\n",
    "                output_file = os.path.join(output_dir, file_name.replace('.xml', '.csv'))\n",
    "                df.to_csv(output_file, index=False)\n",
    "\n",
    "                # Update the progress bar\n",
    "                progress.update(1)\n",
    "            except Exception as e:\n",
    "                print(f\"Error converting file {file_name}: {e}\")\n",
    "\n",
    "#########################################################################################\n",
    "\n",
    "def process_csv_files(date):\n",
    "    # Define input and output directories based on the provided date\n",
    "    input_directory = f'D:/VD_data/{date}/csv'\n",
    "    output_directory = f'D:/VD_data/{date}/VDID'\n",
    "\n",
    "    # Create the output directory if it doesn't exist\n",
    "    if not os.path.exists(output_directory):\n",
    "        os.makedirs(output_directory)\n",
    "\n",
    "    # Initialize an empty list to store DataFrames\n",
    "    dfs = []\n",
    "\n",
    "    # List all CSV files in the input directory\n",
    "    csv_files = [f for f in os.listdir(input_directory) if f.endswith('.csv')]\n",
    "    \n",
    "    print(f\"Processing {len(csv_files)} CSV files:\")\n",
    "    \n",
    "    with tqdm(total=len(csv_files), unit='file') as pbar_files:\n",
    "        for i, filename in enumerate(csv_files, start=1):\n",
    "            # Extract the time from the filename (e.g., VDLive_0855.csv -> '0855')\n",
    "            time = filename.split('_')[1].split('.')[0]\n",
    "            \n",
    "            try:\n",
    "                # Read the CSV file and insert the 'time' column at the beginning\n",
    "                df = pd.read_csv(os.path.join(input_directory, filename))\n",
    "                df.insert(0, 'time', time)\n",
    "                \n",
    "                # Append the DataFrame to the list\n",
    "                dfs.append(df)\n",
    "                \n",
    "                pbar_files.update(1)\n",
    "            except Exception as e:\n",
    "                # Print an error message and continue processing other files\n",
    "                display(HTML(f'<span style=\"color:red\">Error processing file {filename}: {str(e)}</span>'))\n",
    "\n",
    "    # Concatenate all DataFrames in the list to create the combined DataFrame\n",
    "    combined_df = pd.concat(dfs, ignore_index=True)\n",
    "\n",
    "    # Group the combined DataFrame by 'VDID'\n",
    "    groups = combined_df.groupby('VDID')\n",
    "    \n",
    "    print(f\"\\nSaving {len(groups)} VDID-specific CSV files:\")\n",
    "    \n",
    "    with tqdm(total=len(groups), unit='VDID') as pbar_vdids:\n",
    "        for i, (vdid, group_df) in enumerate(groups, start=1):\n",
    "            try:\n",
    "                # Save the group-specific data to a CSV file in the output directory\n",
    "                group_df.to_csv(os.path.join(output_directory, f'{vdid}.csv'), index=False)\n",
    "                \n",
    "                pbar_vdids.update(1)\n",
    "            except Exception as e:\n",
    "                # Print an error message if saving fails\n",
    "                display(HTML(f'<span style=\"color:red\">Error saving VDID {vdid}: {str(e)}</span>'))\n",
    "    \n",
    "    print(f\"\\n{len(groups)} VDID-specific CSV files saved.\")\n",
    "\n",
    "#########################################################################################\n",
    "\n",
    "def check_files(date):\n",
    "    # Define the directory paths based on the input date\n",
    "    csv_directory = os.path.join(r'D:\\VD_data', date, 'csv')\n",
    "    vdid_directory = os.path.join(r'D:\\VD_data', date, 'VDID')\n",
    "    \n",
    "    # Create dictionaries to store the distribution of row counts for CSV files and VDID files\n",
    "    csv_row_counts = {}\n",
    "    vdid_row_counts = {}\n",
    "    \n",
    "    # Get the total number of CSV files in the CSV directory\n",
    "    total_csv_files = len([filename for filename in os.listdir(csv_directory) if filename.endswith(\".csv\")])\n",
    "    \n",
    "    # Create a progress bar for processing CSV files\n",
    "    csv_progress_bar = tqdm(total=total_csv_files, desc=\"Processing CSV files\")\n",
    "    \n",
    "    # Iterate through CSV files in the CSV directory\n",
    "    for filename in os.listdir(csv_directory):\n",
    "        if filename.endswith(\".csv\"):\n",
    "            file_path = os.path.join(csv_directory, filename)\n",
    "            \n",
    "            # Read the CSV file into a DataFrame\n",
    "            df = pd.read_csv(file_path)\n",
    "            \n",
    "            # Get the number of rows in the DataFrame\n",
    "            num_rows = len(df)\n",
    "            \n",
    "            # Update the csv_row_counts dictionary\n",
    "            if num_rows in csv_row_counts:\n",
    "                csv_row_counts[num_rows] += 1\n",
    "            else:\n",
    "                csv_row_counts[num_rows] = 1\n",
    "            \n",
    "            # Update the CSV progress bar\n",
    "            csv_progress_bar.update(1)\n",
    "    \n",
    "    # Close the CSV progress bar\n",
    "    csv_progress_bar.close()\n",
    "    \n",
    "    # Survey the 'VDID' directory\n",
    "    if os.path.exists(vdid_directory):\n",
    "        vdid_files = os.listdir(vdid_directory)\n",
    "        \n",
    "        # Create a progress bar for processing VDID files\n",
    "        vdid_progress_bar = tqdm(total=len(vdid_files), desc=\"Processing VDID files\")\n",
    "        \n",
    "        # Iterate through VDID files in the VDID directory\n",
    "        for filename in vdid_files:\n",
    "            file_path = os.path.join(vdid_directory, filename)\n",
    "            \n",
    "            # Read the VDID file into a DataFrame\n",
    "            df = pd.read_csv(file_path)\n",
    "            \n",
    "            # Get the number of rows in the DataFrame\n",
    "            num_rows = len(df)\n",
    "            \n",
    "            # Update the vdid_row_counts dictionary\n",
    "            if num_rows in vdid_row_counts:\n",
    "                vdid_row_counts[num_rows] += 1\n",
    "            else:\n",
    "                vdid_row_counts[num_rows] = 1\n",
    "            \n",
    "            # Update the VDID progress bar\n",
    "            vdid_progress_bar.update(1)\n",
    "        \n",
    "        # Close the VDID progress bar\n",
    "        vdid_progress_bar.close()\n",
    "    \n",
    "    # Calculate the total rows for CSV and VDID files\n",
    "    total_csv_rows = sum(num_rows * count for num_rows, count in csv_row_counts.items())\n",
    "    total_vdid_rows = sum(num_rows * count for num_rows, count in vdid_row_counts.items())\n",
    "    \n",
    "    # Define the output directory and log file path\n",
    "    output_directory = os.path.join(r'D:\\VD_data', date)\n",
    "    log_file_path = os.path.join(output_directory, 'log.txt')\n",
    "    \n",
    "    # Write the distribution of row counts for CSV files to the log file\n",
    "    with open(log_file_path, 'w') as log_file:\n",
    "        log_file.write(\"Distribution of row counts for CSV files:\\n\")\n",
    "        for num_rows, count in sorted(csv_row_counts.items()):\n",
    "            log_file.write(f\"CSV files with {num_rows} rows: {count} files\\n\")\n",
    "        \n",
    "        # Write the distribution of row counts for VDID files to the log file\n",
    "        log_file.write(\"Distribution of row counts for VDID files:\\n\")\n",
    "        for num_rows, count in sorted(vdid_row_counts.items()):\n",
    "            log_file.write(f\"VDID files with {num_rows} rows: {count} files\\n\")\n",
    "        \n",
    "        # Write the total rows for CSV and VDID files\n",
    "        log_file.write(f\"Total rows in CSV files: {total_csv_rows}\\n\")\n",
    "        log_file.write(f\"Total rows in VDID files: {total_vdid_rows}\\n\")\n",
    "    \n",
    "    # Print the distribution of row counts for CSV files\n",
    "    print()\n",
    "    print(\"Distribution of row counts for CSV files:\")\n",
    "    for num_rows, count in sorted(csv_row_counts.items()):\n",
    "        print(f\"CSV files with {num_rows} rows: {count} files\")\n",
    "    \n",
    "    # Print the distribution of row counts for VDID files\n",
    "    print()\n",
    "    print(\"Distribution of row counts for VDID files:\")\n",
    "    for num_rows, count in sorted(vdid_row_counts.items()):\n",
    "        print(f\"VDID files with {num_rows} rows: {count} files\")\n",
    "    \n",
    "    # Print the total rows for CSV and VDID files\n",
    "    print()\n",
    "    print(f\"Total rows in CSV files: {total_csv_rows}\")\n",
    "    print(f\"Total rows in VDID files: {total_vdid_rows}\")\n",
    "    \n",
    "    # Compare and report any differences in total rows\n",
    "    if total_csv_rows == total_vdid_rows:\n",
    "        print(\"Total rows in CSV and VDID files are the same.\")\n",
    "    else:\n",
    "        print(\"Total rows in CSV and VDID files are different.\")\n",
    "\n",
    "#########################################################################################\n",
    "\n",
    "def delete_files(date, delete_compressed, delete_decompressed, delete_csv):\n",
    "    # Define the directory paths based on the input date\n",
    "    compressed_directory = os.path.join(r'D:\\VD_data', date, 'compressed')\n",
    "    decompressed_directory = os.path.join(r'D:\\VD_data', date, 'decompressed')\n",
    "    csv_directory = os.path.join(r'D:\\VD_data', date, 'csv')\n",
    "    \n",
    "    # Helper function to delete files in a directory\n",
    "    def delete_files_in_directory(directory):\n",
    "        if os.path.exists(directory):\n",
    "            file_list = os.listdir(directory)\n",
    "            for file in file_list:\n",
    "                file_path = os.path.join(directory, file)\n",
    "                try:\n",
    "                    if os.path.isfile(file_path):\n",
    "                        os.remove(file_path)\n",
    "                except Exception as e:\n",
    "                    print(f\"Error deleting file: {file_path} ({e})\")\n",
    "        print(f\"Deleted file: {directory}\")\n",
    "    \n",
    "    # Delete files in the specified directories based on the parameter values\n",
    "    if delete_compressed == 1:\n",
    "        delete_files_in_directory(compressed_directory)\n",
    "    \n",
    "    if delete_decompressed == 1:\n",
    "        delete_files_in_directory(decompressed_directory)\n",
    "    \n",
    "    if delete_csv == 1:\n",
    "        delete_files_in_directory(csv_directory)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Starting download for date: 20240126\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "5f140523adca4eb28f9dba01ea250c98",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Download process completed.\n",
      "Decompressing xml.gz files...\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "4f504a1462fc47f09807126d3e2d8d08",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Decompression process completed.\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "d4a2706f79214f8b9ade430fe63865ce",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Processing 1440 CSV files:\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "c1d5b44994694af993403f0774d77570",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?file/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "Saving 3630 VDID-specific CSV files:\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "21b4092b4e1a4344ad9b089bbe7a2a4b",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/3630 [00:00<?, ?VDID/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "3630 VDID-specific CSV files saved.\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "b84f3e16da0f4e6390c7531cfdf64b89",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "Processing CSV files:   0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "cc1ded66c270472ab7f2f3e8175516c0",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "Processing VDID files:   0%|          | 0/3630 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "Distribution of row counts for CSV files:\n",
      "CSV files with 3628 rows: 121 files\n",
      "CSV files with 3630 rows: 1319 files\n",
      "\n",
      "Distribution of row counts for VDID files:\n",
      "VDID files with 1319 rows: 2 files\n",
      "VDID files with 1440 rows: 3628 files\n",
      "\n",
      "Total rows in CSV files: 5226958\n",
      "Total rows in VDID files: 5226958\n",
      "Total rows in CSV and VDID files are the same.\n",
      "Deleted file: D:\\VD_data\\20240126\\compressed\n",
      "Deleted file: D:\\VD_data\\20240126\\decompressed\n",
      "Starting download for date: 20240127\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "cac8bf9402f348f9b00e345bf477c393",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Failed to download: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240127/VDLive_1239.xml.gz\n",
      "Failed to download: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240127/VDLive_1738.xml.gz\n",
      "Retrying failed downloads...\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "67634ffc08ab48ed818030312324f753",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/2 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Download process completed.\n",
      "Decompressing xml.gz files...\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "46846b2b2c0442339b62485d08e29658",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Decompression process completed.\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "949a4bdf431d42d89f165ec2e3b3a0e1",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Processing 1440 CSV files:\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "53f6d4f8619e4a68898b6e98c0b3634c",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?file/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "Saving 3630 VDID-specific CSV files:\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "b0cbf063562e462fbb6bc2d471fd6f0b",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/3630 [00:00<?, ?VDID/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "3630 VDID-specific CSV files saved.\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "0b5461322bee4cb6bcc98fee0ca3e0a6",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "Processing CSV files:   0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "970f2874b23f4360abfed9a39b93a4fe",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "Processing VDID files:   0%|          | 0/3630 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "Distribution of row counts for CSV files:\n",
      "CSV files with 3630 rows: 1440 files\n",
      "\n",
      "Distribution of row counts for VDID files:\n",
      "VDID files with 1440 rows: 3630 files\n",
      "\n",
      "Total rows in CSV files: 5227200\n",
      "Total rows in VDID files: 5227200\n",
      "Total rows in CSV and VDID files are the same.\n",
      "Deleted file: D:\\VD_data\\20240127\\compressed\n",
      "Deleted file: D:\\VD_data\\20240127\\decompressed\n",
      "Starting download for date: 20240124\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "999ed7da347e4429957b66345babd544",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Download process completed.\n",
      "Decompressing xml.gz files...\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "3304377295554511a05aa8865b656c3d",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Decompression process completed.\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "abb66ebadf0b4b1caece60e7ab9da849",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Processing 1440 CSV files:\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "01948396c49241968fe782b3cacabc92",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?file/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "Saving 3766 VDID-specific CSV files:\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "1affcee0225f4d1287916415ac6bed26",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/3766 [00:00<?, ?VDID/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "3766 VDID-specific CSV files saved.\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "939b01cfb8ae4828b4b3e03e0cf48c58",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "Processing CSV files:   0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "ed2895e8967a47b49a33ae8a0cb0e2c4",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "Processing VDID files:   0%|          | 0/3766 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "Distribution of row counts for CSV files:\n",
      "CSV files with 3629 rows: 1319 files\n",
      "CSV files with 3766 rows: 121 files\n",
      "\n",
      "Distribution of row counts for VDID files:\n",
      "VDID files with 121 rows: 137 files\n",
      "VDID files with 1440 rows: 3629 files\n",
      "\n",
      "Total rows in CSV files: 5242337\n",
      "Total rows in VDID files: 5242337\n",
      "Total rows in CSV and VDID files are the same.\n",
      "Deleted file: D:\\VD_data\\20240124\\compressed\n",
      "Deleted file: D:\\VD_data\\20240124\\decompressed\n",
      "Starting download for date: 20240123\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "6dc60cd70b29477f9994bfecad976b63",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1440 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0916.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0917.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0918.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0919.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0920.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0921.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0923.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0924.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0927.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0925.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0922.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0926.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0928.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0929.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0930.xml.gz (File too small)\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0931.xml.gz (File too small)\n",
      "Retrying failed downloads...\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "4158fee53a2a4eddac7825bf2e89d59b",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/16 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0916.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0916.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0917.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0917.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0918.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0918.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0919.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0919.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0920.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0920.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0921.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0921.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0923.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0923.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0924.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0924.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0927.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0927.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0925.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0925.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0922.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0922.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0926.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0926.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0928.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0928.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0929.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0929.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0930.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0930.xml.gz\n",
      "Deleted: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0931.xml.gz (File too small)\n",
      "Failed to download on retry: https://tisvcloud.freeway.gov.tw/history/motc20/VD/20240123/VDLive_0931.xml.gz\n",
      "Download process completed.\n",
      "Decompressing xml.gz files...\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "918e27cd7d9f4f599ccaf43d4e7b690b",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1424 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Decompression process completed.\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "efbb51845eb5491aa70b1847d7778b4d",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1424 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Processing 1424 CSV files:\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "df19f6633c414226bfd265560f5bdc43",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/1424 [00:00<?, ?file/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "Saving 3766 VDID-specific CSV files:\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "c9818aca574a4598b250a4551b8fa17e",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "  0%|          | 0/3766 [00:00<?, ?VDID/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "3766 VDID-specific CSV files saved.\n"
     ]
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "3ca1358208094cdda4afe4c10f63be7c",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "Processing CSV files:   0%|          | 0/1424 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "14ae30068059478daf417fb1e9d10b1b",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "Processing VDID files:   0%|          | 0/3766 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "Distribution of row counts for CSV files:\n",
      "CSV files with 3766 rows: 1424 files\n",
      "\n",
      "Distribution of row counts for VDID files:\n",
      "VDID files with 1424 rows: 3766 files\n",
      "\n",
      "Total rows in CSV files: 5362784\n",
      "Total rows in VDID files: 5362784\n",
      "Total rows in CSV and VDID files are the same.\n",
      "Deleted file: D:\\VD_data\\20240123\\compressed\n",
      "Deleted file: D:\\VD_data\\20240123\\decompressed\n"
     ]
    }
   ],
   "source": [
    "def fetch_vd(date, delete_compressed, delete_decompressed, delete_csv):\n",
    "    download_files_for_day(date, max_concurrent_downloads=5)\n",
    "    decompress_files(date)\n",
    "    convert_xml_to_csv(date)\n",
    "    process_csv_files(date)\n",
    "    check_files(date)\n",
    "    delete_files(date, delete_compressed, delete_decompressed, delete_csv)\n",
    "\n",
    "\n",
    "fetch_vd(\"20240126\", 1, 1, 0) # date, delete_compressed, delete_decompressed, delete_csv\n",
    "fetch_vd(\"20240127\", 1, 1, 0)\n",
    "fetch_vd(\"20240124\", 1, 1, 0)\n",
    "fetch_vd(\"20240123\", 1, 1, 0)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.3"
  },
  "orig_nbformat": 4
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
