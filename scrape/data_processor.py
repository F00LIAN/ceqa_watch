import os 
import logging
import pandas as pd
from psycopg2.extras import execute_values
from utils import db_connection, reorder_filtered_columns, split_received_date, generate_entry_id_and_date_gathered, download_csv, upload_to_blob, delete_from_blob, cleanup_local_file, run_parcel_expansion
from const import KEEPS, BASE_URL, INSERT_QUERY, DOCUMENT_TYPE

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to check for columns with values exceeding the specified length
def check_column_lengths(df, max_length=50):
    """
    Check the maximum length of the values in each column of the DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the columns to check.
        max_length (int): The length limit to check for (default is 50).
        
    Returns:
        dict: A dictionary of column names and their maximum value lengths.
    """
    long_columns = {}
    
    for col in df.columns:
        max_len = df[col].astype(str).map(len).max()
        logging.debug(f"Max length of column {col}: {max_len}")
        if max_len > max_length:
            long_columns[col] = max_len
    
    return long_columns

class CEQADataProcessor:
    """
    A class to process CEQA data, download it for different cities, upload to Azure Blob Storage, 
    insert it into the PostgreSQL database with UPSERT functionality, 
    and remove the file from the Blob storage after successful processing.
    """
    
    def __init__(self, table_name):
        """
        Initialize the data processor with a specified database table.
        
        Args:
            table_name (str): The name of the table to insert the data into.
        """
        self.table_name = table_name
        logging.debug(f"CEQADataProcessor initialized with table {self.table_name}")

    def process_csv(self, file_name):
        """
        Process a CSV file, clean the data, and insert it into the database using UPSERT.

        Args:
            file_name (str): The path to the file to process from Azure Blob Storage.
        """
        logging.debug(f"Processing CSV file {file_name}")
        connection = None
        cursor = None  # Initialize cursor to None to handle errors correctly

        try:

            """ METHODS FOR PROCESSING THE CSV FILE """

            # Reading the CSV file
            df = pd.read_csv(file_name, encoding="ISO-8859-1")
            logging.info(f"Read {len(df)} rows from {file_name}")

            # Filter to keep only the required columns
            df_filtered = df.loc[:, KEEPS]
            logging.debug("Filtered dataframe to required columns")

            # Replace null values with 'unknown'
            df_filtered = df_filtered.fillna('Unknown')
            logging.debug("Replaced NULL values with 'unknown'")

            # Generate a unique entry_id by combining SCH Number
            df_filtered = split_received_date(df_filtered, 'Received')

            # Generate entry_id and date_gathered
            df_filtered = generate_entry_id_and_date_gathered(df_filtered)
            logging.debug(f"Generated entry_id successfully")
            
            # Expand parcel data and process it
            df_expanded = run_parcel_expansion(df_filtered)
            logging.debug("Expanded parcel data successfully")

            # map the dictionary to the document type column
            df_expanded['Document Type Details'] = df_expanded['Document Type'].map(DOCUMENT_TYPE)
            #logging.debug(f"Expanded columns: {df_expanded.columns}")

            # Reorder the columns to fit the insert query
            df_expanded = reorder_filtered_columns(df_expanded)
            logging.debug("Reordered columns for insertion")

            # Convert the filtered DataFrame into a list of tuples for insertion
            data_tuples = list(df_expanded.itertuples(index=False, name=None))
            logging.debug(f"Prepared {len(data_tuples)} rows for insertion")

            # Establish database connection and insert data
            connection = db_connection()
            cursor = connection.cursor()

            # UPSERT query execution using psycopg2's execute_values
            insert_query = INSERT_QUERY
            execute_values(cursor, insert_query, data_tuples)
            connection.commit()

            logging.info(f"Data from {file_name} successfully upserted.")

        except Exception as e:
            if connection:
                connection.rollback()  # Rollback transaction in case of error
            logging.error(f"Error inserting data from {file_name}: {e}")
            raise  # Re-raise the exception after logging it

        finally:
            # Ensure that both cursor and connection are properly closed
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def run_for_cities(self, cities):
        """
        Process data for multiple cities by downloading CSVs, uploading them to Azure Blob Storage, 
        processing them, and removing them from the blob storage if successful.

        Args:
            cities (list): List of city names to process.
        """
        for city in cities:
            logging.debug(f"Starting process for city: {city}")
            try:
                # Step 1: Download the CSV file
                csv_file = download_csv(city, BASE_URL)
                
                # Step 2: Upload the CSV file to Azure Blob Storage
                upload_to_blob(csv_file)
                
                # Step 3: Process the CSV (Insert into the database)
                self.process_csv(csv_file)

                # Step 4: Remove the file from Azure Blob Storage
                delete_from_blob(csv_file)
                logging.info(f"Successfully processed and deleted {csv_file} from Azure Blob Storage.")
                
                # Step 5: Cleanup local file
                cleanup_local_file(csv_file)

            except Exception as e:
                logging.error(f"Error processing data for {city}: {e}")
                # Make sure to clean up the local file if it exists
                cleanup_local_file(csv_file)