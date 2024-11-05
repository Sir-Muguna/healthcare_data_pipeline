import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def read_config(config_file):
    config = {}
    with open(config_file, 'r') as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):  # Ignore comments and empty lines
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
    return config

def get_confirm_token(response):
    """Extracts the confirm token for large file downloads from Google Drive."""
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value
    return None

def download_google_files(config_file='config.txt'):
    config = read_config(config_file)
    OUTPUT_DIRECTORY = config.get('output_sources')  

    FILES = {
        "hospital_general_info.csv": config.get("hospital_general_info.csv"),
        "inpatient_2011.json": config.get("inpatient_2011.json"),
        "inpatient_2012.json": config.get("inpatient_2012.json"),
        "inpatient_2013.json": config.get("inpatient_2013.json"),
    }

    BASE_URL = "https://drive.google.com/uc?export=download&id="

    def download_file(file_name, file_id):
        url = BASE_URL + file_id
        session = requests.Session()

        response = session.get(url, stream=True)
        token = get_confirm_token(response)

        if token:
            # Re-attempt download with confirmation token
            params = {'confirm': token}
            response = session.get(url, params=params, stream=True)

        if response.status_code == 200:
            os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
            file_path = os.path.join(OUTPUT_DIRECTORY, file_name)
            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            logging.info(f"{file_name} downloaded successfully.")
            return f"{file_name} downloaded successfully."
        else:
            logging.error(f"Failed to download {file_name}. Status code: {response.status_code}")
            return f"Failed to download {file_name}. Status code: {response.status_code}"

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_file = {executor.submit(download_file, name, file_id): name for name, file_id in FILES.items()}
        for future in as_completed(future_to_file):
            file_name = future_to_file[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                results.append(f"Error downloading {file_name}: {e}")

    return results
