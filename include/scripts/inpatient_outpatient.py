import os
import json
import pandas as pd
import shutil

# Function to read configuration from a file
def read_config(config_file='config.txt'):
    config = {}
    with open(config_file, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip empty lines or comments
            if line and not line.startswith("#"):
                try:
                    key, value = line.split("=", 1)  # Split on the first '=' only
                    config[key.strip()] = value.strip()
                except ValueError:
                    print(f"Skipping invalid config line: {line}")
    # Verify required keys
    required_keys = ['input_directory', 'output_datasets']
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required configuration key: {key}")
    return config

# Function to convert JSON to CSV
def process_json_to_csv(input_file, output_file):
    """Converts JSON data to CSV."""
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    with open(input_file, 'r') as file:
        json_data = json.load(file)
    df = pd.DataFrame(json_data)
    df.to_csv(output_file, index=False)
    print(f"Data from {input_file} successfully saved to {output_file}")

# Function to copy and process hospital_general_info CSV file with data type conversions
def process_hospital_general_info(input_file, output_file):
    """Processes the hospital_general_info.csv file and converts data types for specific columns."""
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    df = pd.read_csv(input_file)
    
    # Convert specific columns to match BigQuery schema
    df["provider_id"] = pd.to_numeric(df["provider_id"], errors="coerce").fillna(0).astype(int)
    df["zip_code"] = pd.to_numeric(df["zip_code"], errors="coerce").fillna(0).astype(int)
    
    # Save the processed DataFrame to CSV
    df.to_csv(output_file, index=False)
    print(f"Processed hospital_general_info.csv successfully saved to {output_file}")

# Function to copy other CSV files to the destination
def copy_csv_files(src_file, dest_folder):
    """Copies a CSV file from the source to the destination folder."""
    if not os.path.exists(src_file):
        raise FileNotFoundError(f"File not found: {src_file}")
    
    shutil.copy(src_file, dest_folder)
    print(f"{src_file} successfully copied to {dest_folder}")

# Main function to handle the JSON and CSV processing
def inpatient_and_outpatient_files(config_file='config.txt'):
    """Processes multiple JSON to CSV conversions and CSV file copying."""
    # Load configurations
    config = read_config(config_file)
    
    # Paths
    input_directory = config['input_directory']
    output_folder_path = config['output_datasets']
    os.makedirs(output_folder_path, exist_ok=True)
    
    # JSON to CSV transformation
    target_json_files = ['inpatient_2011.json', 'inpatient_2012.json', 'inpatient_2013.json']
    for filename in target_json_files:
        json_file_path = os.path.join(input_directory, filename)
        output_csv_path = os.path.join(output_folder_path, filename.replace('.json', '.csv'))
        process_json_to_csv(json_file_path, output_csv_path)

    # Process hospital_general_info.csv with data type conversions
    hospital_info_path = os.path.join(input_directory, 'hospital_general_info.csv')
    hospital_info_output_path = os.path.join(output_folder_path, 'hospital_general_info.csv')
    process_hospital_general_info(hospital_info_path, hospital_info_output_path)

    # Copying additional CSV files
    additional_csv_files = ['outpatient_charges_2011.csv', 
                            'outpatient_charges_2012.csv', 'outpatient_charges_2013.csv']
    for csv_file in additional_csv_files:
        csv_source_path = os.path.join(input_directory, csv_file)
        copy_csv_files(csv_source_path, output_folder_path)
