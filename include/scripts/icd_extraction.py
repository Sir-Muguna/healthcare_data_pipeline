import requests
import urllib3
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Disable SSL warnings for this script
urllib3.disable_warnings()

# Configure logging for better debugging and tracking
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Constants for the API
TOKEN_ENDPOINT = 'https://icdaccessmanagement.who.int/connect/token'
CHAPTERS = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI', 'XVII', 'XVIII', 'XIX', 'XX', 'XXI', 'XXII']
MAX_WORKERS = 15  # Increased to allow more concurrent requests

def extract_icd_data(output_file):
    """
    Extracts ICD data from the API and saves it to a JSON file.
    """
    def read_config(config_file):
        """Reads the configuration file for API credentials and paths."""
        config = {}
        try:
            with open(config_file, 'r') as file:
                for line in file:
                    line = line.strip()
                    if not line or '=' not in line:
                        continue
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
            logging.info("Config file loaded successfully.")
        except Exception as e:
            logging.error(f"Error reading config file: {e}")
        return config

    def get_access_token(client_id, client_secret, scope, grant_type):
        """Fetches an OAuth2 access token from the WHO ICD API."""
        payload = {'client_id': client_id, 'client_secret': client_secret, 'scope': scope, 'grant_type': grant_type}
        try:
            response = requests.post(TOKEN_ENDPOINT, data=payload, verify=False)
            if response.status_code == 200:
                logging.info("Successfully retrieved the token!")
                return response.json().get('access_token')
            else:
                logging.error("Failed to retrieve OAuth2 token. Status code: %s", response.status_code)
                return None
        except requests.RequestException as e:
            logging.error(f"Error fetching token: {e}")
            return None

    def fetch_data(token, uri, data_type):
        """Fetches data from the ICD API."""
        headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json', 'Accept-Language': 'en', 'API-Version': 'v2'}
        try:
            response = requests.get(uri, headers=headers, verify=False)
            if response.status_code == 200:
                logging.info("API request successful for %s: %s", data_type, uri)
                return response.json()
            else:
                logging.error("Failed to retrieve data from API for %s. Status code: %s", data_type, response.status_code)
                return None
        except requests.RequestException as e:
            logging.error(f"Error fetching data from API for %s: {e}")
            return None

    def process_hierarchy(token, uri, level_name):
        """Processes hierarchical levels and fetches children recursively."""
        level_data = fetch_data(token, uri, level_name)
        if not level_data:
            return None

        level_info = {"code": level_data["code"], "title": level_data["title"]["@value"], "classKind": level_data["classKind"]}
        children_data = {}

        def process_child(child_uri):
            """Process each child level in the hierarchy."""
            child_data = fetch_data(token, child_uri, f"Child {child_uri}")
            if not child_data:
                return None

            child_info = {"code": child_data["code"], "title": child_data["title"]["@value"]}
            grandchildren_data = {}

            if child_data.get("classKind") == "block" and 'child' in child_data:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    grandchild_futures = {executor.submit(fetch_data, token, gc_uri, f"Grandchild {gc_uri}"): gc_uri for gc_uri in child_data['child']}
                    for gc_future in as_completed(grandchild_futures):
                        grandchild_data = gc_future.result()
                        if grandchild_data:
                            grandchildren_data[grandchild_data['code']] = {"code": grandchild_data["code"], "title": grandchild_data["title"]["@value"]}

            child_info['grandchildren'] = grandchildren_data
            return child_info

        if 'child' in level_data:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                child_futures = {executor.submit(process_child, child_uri): child_uri for child_uri in level_data['child']}
                for future in as_completed(child_futures):
                    child_data = future.result()
                    if child_data:
                        children_data[child_data['code']] = child_data

        return {"info": level_info, "children": children_data}

    # Load config and retrieve token
    config_file = '/usr/local/airflow/include/scripts/config.txt'
    config = read_config(config_file)
    client_id = config.get('client_id')
    client_secret = config.get('client_secret')
    scope = config.get('scope', 'icdapi_access')
    grant_type = config.get('grant_type', 'client_credentials')
    if not client_id or not client_secret:
        logging.error("Client ID or Client Secret missing in config file.")
        return

    all_data = {}
    token = get_access_token(client_id, client_secret, scope, grant_type)
    if not token:
        return

    # Process chapters with concurrency
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        chapter_futures = {executor.submit(process_hierarchy, token, f'http://id.who.int/icd/release/10/2019/{chapter}', f"Chapter {chapter}"): chapter for chapter in CHAPTERS}
        for future in as_completed(chapter_futures):
            chapter = chapter_futures[future]
            chapter_result = future.result()
            if chapter_result:
                all_data[chapter] = chapter_result

    # Save collected data to a JSON file
    with open(output_file, 'w') as json_file:
        json.dump(all_data, json_file, indent=4)
    logging.info("Data saved successfully to %s.", output_file)

if __name__ == "__main__":
    output_file = '/usr/local/airflow/include/sources/icd_data.json'
    extract_icd_data(output_file)
