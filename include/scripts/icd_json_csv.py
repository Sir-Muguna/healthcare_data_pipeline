import json
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def read_config(config_file):
    config = {}
    try:
        with open(config_file, 'r') as file:
            for line in file:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    config[key] = value
        logging.info("Config file loaded successfully.")
    except FileNotFoundError:
        logging.error(f"Config file not found at {config_file}")
    except Exception as e:
        logging.error(f"Error reading config file: {e}")
    return config

def process_code_element(child_code, child_title, grandchild_code, grandchild_title, chapter, chapter_desc, rows):
    try:
        if '-' in grandchild_code:
            rows.append({
                'chapter': chapter,
                'chapter_description': chapter_desc,
                'code_category': grandchild_code,
                'code_category_description': grandchild_title,
                'icd_code': child_code,
                'disease_description': child_title,
            })
        else:
            rows.append({
                'chapter': chapter,
                'chapter_description': chapter_desc,
                'code_category': child_code,
                'code_category_description': child_title,
                'icd_code': grandchild_code,
                'disease_description': grandchild_title,
            })
    except Exception as e:
        logging.error(f"Error processing element: {e}")

def json_to_csv(input_file, output_file):
    try:
        with open(input_file, 'r') as json_file:
            data = json.load(json_file)

        rows = []

        # Extract data from the JSON structure
        for chapter, chapter_info in data.items():
            chapter_code = chapter_info['info']['code']
            chapter_desc = chapter_info['info']['title']

            for child_code, child_info in chapter_info.get('children', {}).items():
                child_title = child_info.get('title', '')

                if '-' in child_code:
                    rows.append({
                        'chapter': chapter_code,
                        'chapter_description': chapter_desc,
                        'code_category': child_code,
                        'code_category_description': child_title,
                        'icd_code': '',
                        'disease_description': '',
                    })

                if 'grandchildren' in child_info:
                    for grandchild_code, grandchild_info in child_info['grandchildren'].items():
                        if '-' in grandchild_code:
                            rows.append({
                                'chapter': chapter_code,
                                'chapter_description': chapter_desc,
                                'code_category': grandchild_code,
                                'code_category_description': grandchild_info.get('title', ''),
                                'icd_code': '',
                                'disease_description': ''
                            })
                        elif 'great-grandchildren' in grandchild_info:
                            for great_grandchild_code, great_grandchild_info in grandchild_info['great-grandchildren'].items():
                                rows.append({
                                    'chapter': chapter_code,
                                    'chapter_description': chapter_desc,
                                    'code_category': grandchild_code,
                                    'code_category_description': grandchild_info.get('title', ''),
                                    'icd_code': great_grandchild_code,
                                    'disease_description': great_grandchild_info.get('title', '')
                                })
                        else:
                            process_code_element(child_code, child_title, grandchild_code, grandchild_info.get('title', ''), chapter_code, chapter_desc, rows)
                else:
                    rows.append({
                        'chapter': chapter_code,
                        'chapter_description': chapter_desc,
                        'code_category': child_code,
                        'code_category_description': child_title,
                        'icd_code': child_code,
                        'disease_description': child_info.get('title', ''),
                    })

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Create a DataFrame from the rows and save it to CSV
        df = pd.DataFrame(rows)
        df.to_csv(output_file, index=False)
        logging.info(f"Successfully transformed JSON data to CSV at: {output_file}")

    except FileNotFoundError:
        logging.error(f"Input file not found at {input_file}")
    except json.JSONDecodeError:
        logging.error("Error decoding JSON file.")
    except Exception as e:
        logging.error(f"Unexpected error during JSON processing: {e}")

def main():
    config_file = '/usr/local/airflow/include/scripts/config.txt'
    config = read_config(config_file)
    input_file = config.get('input_file')
    output_file = config.get('output_csv')  # Confirm this matches `config.txt`

    if input_file and output_file:
        if os.path.isfile(input_file):
            json_to_csv(input_file, output_file)
        else:
            logging.error("Input file path is invalid.")
    else:
        logging.error("Missing input or output file path in config.")

if __name__ == '__main__':
    main()
