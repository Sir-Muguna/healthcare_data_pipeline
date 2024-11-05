import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def read_config(config_file):
    config = {}
    try:
        with open(config_file, 'r') as file:
            for line in file:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    config[key] = value
        logging.info("Config loaded successfully.")
        for key, value in config.items():
            logging.info(f"{key} = {value}")
    except Exception as e:
        logging.error(f"Error reading config file: {e}")
    return config

config = read_config('/usr/local/airflow/include/scripts/config.txt')
