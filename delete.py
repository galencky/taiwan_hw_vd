import os

def delete_files(date, delete_compressed, delete_decompressed, delete_csv):
    # Define the directory paths based on the input date
    compressed_directory = os.path.join(r'D:\VD_data', date, 'compressed')
    decompressed_directory = os.path.join(r'D:\VD_data', date, 'decompressed')
    csv_directory = os.path.join(r'D:\VD_data', date, 'csv')
    
    # Helper function to delete files in a directory
    def delete_files_in_directory(directory):
        if os.path.exists(directory):
            file_list = os.listdir(directory)
            for file in file_list:
                file_path = os.path.join(directory, file)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                except Exception as e:
                    print(f"Error deleting file: {file_path} ({e})")
        print(f"Deleted file: {directory}")
    
    # Delete files in the specified directories based on the parameter values
    if delete_compressed == 1:
        delete_files_in_directory(compressed_directory)
    
    if delete_decompressed == 1:
        delete_files_in_directory(decompressed_directory)
    
    if delete_csv == 1:
        delete_files_in_directory(csv_directory)