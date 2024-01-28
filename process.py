def process_csv_files(date):
    # Define input and output directories based on the provided date
    input_directory = f'D:/VD_data/{date}/csv'
    output_directory = f'D:/VD_data/{date}/VDID'

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Initialize an empty list to store DataFrames
    dfs = []

    # List all CSV files in the input directory
    csv_files = [f for f in os.listdir(input_directory) if f.endswith('.csv')]
    
    print(f"Processing {len(csv_files)} CSV files:")
    
    with tqdm(total=len(csv_files), unit='file') as pbar_files:
        for i, filename in enumerate(csv_files, start=1):
            # Extract the time from the filename (e.g., VDLive_0855.csv -> '0855')
            time = filename.split('_')[1].split('.')[0]
            
            try:
                # Read the CSV file and insert the 'time' column at the beginning
                df = pd.read_csv(os.path.join(input_directory, filename))
                df.insert(0, 'time', time)
                
                # Append the DataFrame to the list
                dfs.append(df)
                
                pbar_files.update(1)
            except Exception as e:
                # Print an error message and continue processing other files
                display(HTML(f'<span style="color:red">Error processing file {filename}: {str(e)}</span>'))

    # Concatenate all DataFrames in the list to create the combined DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)

    # Group the combined DataFrame by 'VDID'
    groups = combined_df.groupby('VDID')
    
    print(f"\nSaving {len(groups)} VDID-specific CSV files:")
    
    with tqdm(total=len(groups), unit='VDID') as pbar_vdids:
        for i, (vdid, group_df) in enumerate(groups, start=1):
            try:
                # Save the group-specific data to a CSV file in the output directory
                group_df.to_csv(os.path.join(output_directory, f'{vdid}.csv'), index=False)
                
                pbar_vdids.update(1)
            except Exception as e:
                # Print an error message if saving fails
                display(HTML(f'<span style="color:red">Error saving VDID {vdid}: {str(e)}</span>'))
    
    print(f"\n{len(groups)} VDID-specific CSV files saved.")