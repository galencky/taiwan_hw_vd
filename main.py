from download import download_files_for_day
from decompress import decompress_files
from convert import convert_xml_to_csv
from process import process_csv_files
from check import check_files
from delete import delete_files

def main():
    date = input("Enter the date (e.g., '20240126'): ")
    delete_compressed = int(input("Delete compressed files (1 for Yes, 0 for No): "))
    delete_decompressed = int(input("Delete decompressed files (1 for Yes, 0 for No): "))
    delete_csv = int(input("Delete CSV files (1 for Yes, 0 for No): "))

    download_files_for_day(date, max_concurrent_downloads=5)
    decompress_files(date)
    convert_xml_to_csv(date)
    process_csv_files(date)
    check_files(date)
    delete_files(date, delete_compressed, delete_decompressed, delete_csv)

if __name__ == "__main__":
    main()
