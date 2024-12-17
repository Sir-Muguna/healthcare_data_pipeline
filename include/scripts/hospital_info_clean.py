import pandas as pd
import os

# Function to read configuration from a file
def read_config(config_file):
    config = {}
    with open(config_file, 'r') as f:
        for line in f:
            if '=' in line and not line.strip().startswith('#'):
                key, value = line.strip().split('=', 1)
                config[key.strip()] = value.strip()
    return config

# Main cleaning function
def hospitalinfo_cleaned(config_file="/usr/local/airflow/include/scripts/config.txt"):
    config = read_config(config_file)

    if 'hospitalinfo_input' not in config or 'cleaned_hospitalinfo_output' not in config:
        raise ValueError("Config file must contain 'hospitalinfo_input' and 'cleaned_hospitalinfo_output' paths.")

    input_file = config['hospitalinfo_input']
    output_file = config['cleaned_hospitalinfo_output']

    # Load the CSV file into a DataFrame
    df = pd.read_csv(input_file)

    # Select specific columns
    columns_to_keep = [
        'provider_id', 'hospital_name', 'address', 'city', 'state', 
        'zip_code', 'mortality_group_measure_count', 'facility_mortaility_measures_count'
    ]
    df = df[columns_to_keep]

    # Clean up 'provider_id' column
    if 'provider_id' in df.columns:
        df['provider_id'] = df['provider_id'].str.replace('F$', '', regex=True)

    # Replace 'Not Available' with None in specified columns
    df['mortality_group_measure_count'] = df['mortality_group_measure_count'].replace('Not Available', None)
    df['facility_mortaility_measures_count'] = df['facility_mortaility_measures_count'].replace('Not Available', None)

    # Convert to best data types and save
    df = df.convert_dtypes()
    df.to_csv(output_file, index=False)
    print(f"Cleaned data saved to {output_file}")

# Entry point for direct execution in terminal
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Clean hospital info CSV data.")
    parser.add_argument(
        "config_file",
        nargs="?",
        default="/usr/local/airflow/include/scripts/config.txt",
        help="Path to the configuration file (default: /usr/local/airflow/include/scripts/config.txt)"
    )
    args = parser.parse_args()
    hospitalinfo_cleaned(args.config_file)
