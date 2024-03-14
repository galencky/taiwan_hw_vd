# Taiwan Highway Vehicle Detector Data Analysis

Welcome to the GitHub repository for Taiwan Highway Vehicle Detector Data Analysis! This project is dedicated to gathering and processing data from highway vehicle detectors across Taiwan. Our code is designed to help you easily fetch, decompress, and analyze vehicle detector data. Whether you're a researcher, student, or enthusiast in the field of traffic data analysis, this guide will help you get started with our codebase.

## Introduction
The Taiwan Highway Vehicle Detector Data Analysis pipeline is structured to automate the process of gathering, decompressing, processing, and organizing highway vehicle detector data for analysis. The pipeline operates in several distinct stages, each designed to efficiently handle the data from raw format to a structured, analyzable form. Here's an overview of how the pipeline works:

### 1. Setting Up the Environment

- **Prerequisites:** Users must ensure that Python 3 is installed along with necessary libraries (`numpy`, `pandas`, `requests`, `tqdm`, `pytz`, and `gzip` for decompression).
- **Directory Setup:** Users create a directory for storing downloaded and processed data, referred to as `directory_path` in the code.

### 2. Data Download

- **URL Generation:** The pipeline constructs URLs for the targeted data based on the date and time intervals. The data is hosted on the Taiwan Highway Vehicle Detector system, and URLs point to compressed XML files containing the vehicle detection data.
- **Concurrent Downloads:** To speed up the process, the pipeline downloads multiple files concurrently, adhering to a specified limit (e.g., 10 concurrent downloads). This stage leverages Python's `concurrent.futures` and `requests` libraries.

### 3. Data Decompression

- **Decompression:** After downloading, compressed `.xml.gz` files are decompressed to `.xml` format. This process uses the `gzip` library and is done file-by-file to manage disk usage and ensure data integrity.

### 4. Conversion to CSV

- **Parsing XML:** Each decompressed XML file is parsed to extract relevant data, including vehicle speeds, types, and other detector information.
- **Data Flattening:** The structured data from XML is flattened into a tabular format suitable for CSV files. This process involves organizing the data by Vehicle Detector ID (VDID) and lane information.
- **CSV File Creation:** For each XML file, a corresponding CSV file is created with the processed data. This conversion facilitates easier data analysis using common data analysis tools and libraries.

### 5. Organizing Data by VDID

- **Reading CSV Files:** The pipeline reads the generated CSV files and merges them based on VDID, creating a consolidated view of the data for each detector.
- **VDID-specific Files:** Data is then saved into separate CSV files for each VDID, allowing for focused analysis on specific detectors or groups of detectors.

### 6. Cleanup and Archiving

- **Temporary File Deletion:** Users have the option to delete intermediate files (compressed, decompressed, and CSV) to free up disk space. This step is configurable and can be adjusted based on the user's needs.
- **Zip Archive Creation:** For ease of distribution or storage, the pipeline can zip the processed CSV files. This step is also optional and controlled by user input.

### 7. Batch Processing

- **Automated Fetching for Multiple Dates:** The pipeline supports batch processing, allowing users to specify a start date and a number of days backwards to process data in bulk. This feature automates the fetching and processing for extended periods.

### Key Features

- **Efficiency:** By using concurrent downloads and processing, the pipeline minimizes the time required to obtain and prepare the data for analysis.
- **Flexibility:** Users can specify which steps of the pipeline to execute and whether to keep or delete intermediate files, offering control over the process and disk usage.
- **Ease of Use:** The structured approach, combined with detailed documentation, makes the pipeline accessible to users with varying levels of technical expertise.

This pipeline facilitates comprehensive analysis of Taiwan's highway vehicle detector data by simplifying the data acquisition and preparation process, allowing analysts to focus more on the analysis and less on data management tasks.



## Prerequisites

Before you begin, make sure you have the following:

- Python 3 environment with basic data analysis libraries installed (e.g., NumPy, Pandas).
- A good internet connection for downloading datasets.
- Sufficient disk space for storing downloaded and processed data.

This project is set up in a Kaggle Python 3 environment, leveraging the kaggle/python Docker image: [https://github.com/kaggle/docker-python](https://github.com/kaggle/docker-python). While designed for Kaggle, it can be adapted for other environments as needed.

## Installation

No additional installation is required if you are using the Kaggle environment. For other environments, ensure you have the necessary Python packages by running:

```bash
pip install numpy pandas requests tqdm pytz
```

## Usage

1. **Setting Up Your Environment:**
   Make sure your Python environment meets the prerequisites mentioned above. If running locally, navigate to your project directory.

2. **Download and Prepare Data:**
   To start downloading and processing data, you will primarily interact with the `fetch_vd` and `batch_fetch_vd` functions.

### Single Day Fetch Example

To fetch data for a single day, you can use the `fetch_vd` function like so:

```python
directory_path = "D:\\VD_data" # Your target directory for data
yesterday_date = "YYYYMMDD" # Replace with the specific date you want to fetch
fetch_vd(directory_path, yesterday_date, delete_compressed=1, delete_decompressed=1, delete_csv=0, delete_files_sp_zip=1)
```

### Batch Fetch Example

For fetching data for multiple days in a batch, use the `batch_fetch_vd` function:

```python
start_date = "20230523" # Start date in YYYYMMDD format
num_days_backwards = 23 # Number of days to fetch data for, going backwards from the start_date
batch_fetch_vd(start_date, num_days_backwards, directory_path, delete_compressed=True, delete_decompressed=True, delete_csv=False, delete_files_sp_zip=True)
```

3. **Data Processing:**
   After downloading, the scripts will automatically decompress, convert XML data to CSV format, and organize it by vehicle detector ID (VDID) for easier analysis.

4. **Customizing Data Fetching:**
   The function parameters `delete_compressed`, `delete_decompressed`, `delete_csv`, and `delete_files_sp_zip` allow you to control which intermediary files are kept or deleted, offering flexibility in managing disk space.

## Contributing

We welcome contributions and suggestions! Please open an issue or pull request if you have improvements or ideas on how to enhance the data analysis process.

## License

This project is open-sourced under the MIT License. See the LICENSE file for more details.

This README is a comprehensive guide for using your code to analyze Taiwan Highway Vehicle Detector data. Feel free to adjust any parts as needed to match your repository structure or preferences.
