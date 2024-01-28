import os
from tqdm import tqdm
import pandas as pd

def check_files(date):
    # Define the directory paths based on the input date
    csv_directory = os.path.join(r'D:\VD_data', date, 'csv')
    vdid_directory = os.path.join(r'D:\VD_data', date, 'VDID')
    
    # Create dictionaries to store the distribution of row counts for CSV files and VDID files
    csv_row_counts = {}
    vdid_row_counts = {}
    
    # Get the total number of CSV files in the CSV directory
    total_csv_files = len([filename for filename in os.listdir(csv_directory) if filename.endswith(".csv")])
    
    # Create a progress bar for processing CSV files
    csv_progress_bar = tqdm(total=total_csv_files, desc="Processing CSV files")
    
    # Iterate through CSV files in the CSV directory
    for filename in os.listdir(csv_directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(csv_directory, filename)
            
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_path)
            
            # Get the number of rows in the DataFrame
            num_rows = len(df)
            
            # Update the csv_row_counts dictionary
            if num_rows in csv_row_counts:
                csv_row_counts[num_rows] += 1
            else:
                csv_row_counts[num_rows] = 1
            
            # Update the CSV progress bar
            csv_progress_bar.update(1)
    
    # Close the CSV progress bar
    csv_progress_bar.close()
    
    # Survey the 'VDID' directory
    if os.path.exists(vdid_directory):
        vdid_files = os.listdir(vdid_directory)
        
        # Create a progress bar for processing VDID files
        vdid_progress_bar = tqdm(total=len(vdid_files), desc="Processing VDID files")
        
        # Iterate through VDID files in the VDID directory
        for filename in vdid_files:
            file_path = os.path.join(vdid_directory, filename)
            
            # Read the VDID file into a DataFrame
            df = pd.read_csv(file_path)
            
            # Get the number of rows in the DataFrame
            num_rows = len(df)
            
            # Update the vdid_row_counts dictionary
            if num_rows in vdid_row_counts:
                vdid_row_counts[num_rows] += 1
            else:
                vdid_row_counts[num_rows] = 1
            
            # Update the VDID progress bar
            vdid_progress_bar.update(1)
        
        # Close the VDID progress bar
        vdid_progress_bar.close()
    
    # Calculate the total rows for CSV and VDID files
    total_csv_rows = sum(num_rows * count for num_rows, count in csv_row_counts.items())
    total_vdid_rows = sum(num_rows * count for num_rows, count in vdid_row_counts.items())
    
    # Define the output directory and log file path
    output_directory = os.path.join(r'D:\VD_data', date)
    log_file_path = os.path.join(output_directory, 'log.txt')
    
    # Write the distribution of row counts for CSV files to the log file
    with open(log_file_path, 'w') as log_file:
        log_file.write("Distribution of row counts for CSV files:\n")
        for num_rows, count in sorted(csv_row_counts.items()):
            log_file.write(f"CSV files with {num_rows} rows: {count} files\n")
        
        # Write the distribution of row counts for VDID files to the log file
        log_file.write("Distribution of row counts for VDID files:\n")
        for num_rows, count in sorted(vdid_row_counts.items()):
            log_file.write(f"VDID files with {num_rows} rows: {count} files\n")
        
        # Write the total rows for CSV and VDID files
        log_file.write(f"Total rows in CSV files: {total_csv_rows}\n")
        log_file.write(f"Total rows in VDID files: {total_vdid_rows}\n")
    
    # Print the distribution of row counts for CSV files
    print()
    print("Distribution of row counts for CSV files:")
    for num_rows, count in sorted(csv_row_counts.items()):
        print(f"CSV files with {num_rows} rows: {count} files")
    
    # Print the distribution of row counts for VDID files
    print()
    print("Distribution of row counts for VDID files:")
    for num_rows, count in sorted(vdid_row_counts.items()):
        print(f"VDID files with {num_rows} rows: {count} files")
    
    # Print the total rows for CSV and VDID files
    print()
    print(f"Total rows in CSV files: {total_csv_rows}")
    print(f"Total rows in VDID files: {total_vdid_rows}")
    
    # Compare and report any differences in total rows
    if total_csv_rows == total_vdid_rows:
        print("Total rows in CSV and VDID files are the same.")
    else:
        print("Total rows in CSV and VDID files are different.")