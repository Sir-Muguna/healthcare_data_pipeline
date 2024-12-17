import os
import sqlite3
import pandas as pd

# Function to read configuration from a file
def read_config(config_file):
    config = {}
    try:
        with open(config_file, 'r') as f:
            for line in f:
                if line.strip() and '=' in line:
                    key, value = line.strip().split('=', 1)
                    config[key.strip()] = value.strip()
    except FileNotFoundError:
        return f"Configuration file {config_file} not found."
    return config

def export_sqltables_to_csv(config_file='config.txt'):
    """Exports all tables from the SQLite database to CSV files."""
    config = read_config(config_file)
    
    # Check if config loading succeeded
    if isinstance(config, str):
        return [config]  # Return error message if config loading failed
    
    # Extract constants from config
    DB_PATH = config.get('db_path')
    OUTPUT_DIRECTORY = config.get('output_sources', './csv_exports')  # Default to './csv_exports'
    
    # Validate database path
    if not os.path.exists(DB_PATH):
        return [f"Database file not found at {DB_PATH}"]
    
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    results = []
    conn = None

    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(DB_PATH)
        results.append("Connected to the database successfully.")
        
        # Fetch all table names in the database
        tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
        table_names = [row[0] for row in conn.execute(tables_query).fetchall()]
        
        if not table_names:
            results.append("No tables found in the database.")
        else:
            results.append(f"Tables in the database: {table_names}")
            
            # Export each table to CSV
            for table_name in table_names:
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                
                if df.empty:
                    results.append(f"Table {table_name} is empty; skipping export.")
                    continue
                
                csv_path = os.path.join(OUTPUT_DIRECTORY, f"{table_name}.csv")
                df.to_csv(csv_path, index=False)
                results.append(f"Exported {table_name} to {csv_path}")
                
    except sqlite3.Error as e:
        results.append(f"Error connecting to database: {e}")
        
    finally:
        if conn:
            conn.close()
        results.append("Database connection closed.")
    
    return results
