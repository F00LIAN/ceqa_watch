import os
import requests
import pandas as pd
import numpy as np
import logging
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient
from dotenv import load_dotenv
import psycopg2
from datetime import datetime
from psycopg2 import sql
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from azure.storage.blob import BlobServiceClient

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def db_connection():
    """
    Establish a connection to the PostgreSQL database.
    
    Returns:
        connection: A psycopg2 connection object.
    """
    try:
        db_host = os.getenv('DB_HOST')
        db_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        db_port = os.getenv('DB_PORT')

        logging.debug(f"Connecting to database {db_name} on host {db_host}")
        connection = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        logging.info("Database connection established successfully")
        return connection
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        raise

def download_csv(city_name, base_url):
    """
    Download a CSV file for a specified city.
    
    Args:
        city_name (str): The name of the city for which to download the data.
        base_url (str): The base URL for CEQA data.
    
    Returns:
        str: The local path to the downloaded CSV file.
    """
    try:
        city_query = city_name.replace(" ", "%20").replace(",", "%2C")
        download_url = f"{base_url}{city_query}&OutputFormat=CSV"
        logging.debug(f"Downloading CSV from URL: {download_url}")
        response = requests.get(download_url)
        
        # Filename for the city CSV
        file_name = f"{city_name}_ceqa.csv"
        
        # Saving the file locally temporarily before uploading to Azure
        with open(file_name, 'wb') as file:
            file.write(response.content)
        logging.info(f"CSV downloaded and saved locally as {file_name}")
        
        return file_name
    except Exception as e:
        logging.error(f"Error downloading CSV for {city_name}: {e}")
        raise

def upload_to_blob(file_name):
    """
    Upload a file to Azure Blob Storage.

    Args:
        file_name (str): The name of the file to upload.
    """
    try:
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        container_name = os.getenv('AZURE_CONTAINER_NAME')

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

        logging.debug(f"Uploading {file_name} to Azure Blob Storage in container {container_name}")
        with open(file_name, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Uploaded {file_name} to Azure Blob Storage.")
    
    except Exception as e:
        logging.error(f"Error uploading {file_name} to Azure Blob Storage: {e}")
        raise

def download_from_blob(file_name):
    """
    Download a file from Azure Blob Storage to a local file.

    Args:
        file_name (str): The name of the file in Azure Blob Storage.
    """
    try:
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        container_name = os.getenv('AZURE_CONTAINER_NAME')

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

        logging.debug(f"Downloading {file_name} from Azure Blob Storage")
        download_path = f"downloaded_{file_name}"

        with open(download_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

        logging.info(f"Downloaded {file_name} from Azure Blob Storage to {download_path}")
        return download_path
    except Exception as e:
        logging.error(f"Error downloading {file_name} from Azure Blob Storage: {e}")
        raise

def delete_from_blob(file_name):
    """
    Delete a file from Azure Blob Storage.

    Args:
        file_name (str): The name of the file to delete.
    """
    try:
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        container_name = os.getenv('AZURE_CONTAINER_NAME')

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

        logging.debug(f"Deleting {file_name} from Azure Blob Storage in container {container_name}")
        blob_client.delete_blob()
        logging.info(f"Deleted {file_name} from Azure Blob Storage.")
    
    except Exception as e:
        logging.error(f"Error deleting {file_name} from Azure Blob Storage: {e}")
        raise

def cleanup_local_file(file_name):
    """
    Remove the local file once processing is done.
    
    Args:
        file_name (str): The name of the local file to be removed.
    """
    try:
        if os.path.exists(file_name):
            os.remove(file_name)
            logging.info(f"Removed local file {file_name}")
        else:
            logging.warning(f"Local file {file_name} does not exist")
    except Exception as e:
        logging.error(f"Error removing local file {file_name}: {e}")
        raise

def split_received_date(df, column):
    """
    Process a DataFrame to split the 'received' date column into 'received_month', 
    'received_day', and 'received_year' columns.

    Args:
        df (pd.DataFrame): Input DataFrame containing the 'received' date column.
    
    Returns:
        pd.DataFrame: DataFrame with 'received_month', 'received_day', and 'received_year' columns.
    """
    
    # Check if the 'received' column exists in the DataFrame
    if column not in df.columns:
        raise ValueError("The DataFrame does not contain a 'received' column.")
    
    # Convert the 'received' column to datetime format (it handles invalid parsing automatically)
    df[column] = pd.to_datetime(df[column], format='%m/%d/%Y', errors='coerce')

    # Check for any rows that couldn't be converted to datetime (NaT)
    if df[column].isna().any():
        raise ValueError("Some dates in the 'received' column could not be parsed.")
    
    # Create new columns for month, day, and year by extracting from the datetime column
    df[f'{column}_month'] = df[column].dt.month
    df[f'{column}_day'] = df[column].dt.day
    df[f'{column}_year'] = df[column].dt.year

    # Optionally, drop the original 'received' column if no longer needed
    # df.drop('received', axis=1, inplace=True)

    return df

import pandas as pd
import numpy as np
import re

# Function to clean parcel numbers by removing hyphens, non-numeric characters, and handling NaN values
def clean_parcel(parcel):
    """Clean parcel numbers by removing hyphens, non-numeric characters, and handling NaN values."""
    if pd.isna(parcel):
        return "Unknown"
    else:
        return re.sub(r'\D', '', parcel)  # Remove all non-numeric characters

# Function to validate the parcels based on length and content
def validate_parcel(parcel):
    """Validate the parcel based on length and content."""
    # Check if the parcel is a string and consists of exactly 9-10 digits
    if isinstance(parcel, str) and re.match(r'^\d{9,10}$', parcel):
        return parcel
    else:
        return "Unknown"

# Function to expand parcel numbers by handling complex cases like 'and', 'thru', semicolons, slashes, and other edge cases
def expand_parcel_numbers(parcel):
    """Expand parcel numbers by handling complex cases like 'and', 'thru', semicolons, slashes, and new edge cases."""
    if pd.isna(parcel) or not isinstance(parcel, str) or not parcel.strip():
        return ["Unknown"]

    # Replace 'and' with a comma and normalize 'thru'
    parcel = parcel.replace(' and ', ', ').replace('thru', ' thru ').replace(' to ', ' to ').replace('&', ', ')

    # Handle multiple semicolons or commas
    parcel = re.sub(r'[;,]+', ',', parcel)  # Replace multiple semicolons or commas with a single comma

    # Split by semicolons first to handle different parcel number groups
    groups = [group.strip() for group in parcel.split(';')]

    expanded_parcels = []

    for group in groups:
        # Split by commas for multiple parcels in the same group
        parts = [p.strip() for p in group.split(',')]

        base_parcel = None

        for part in parts:
            # Check if the part contains slashes (e.g., '3204008045/047') and handle accordingly
            if '/' in part:
                split_parts = part.split('/')
                base_parcel = split_parts[0]
                if len(split_parts) > 1:
                    for suffix in split_parts[1:]:
                        if suffix.isdigit():
                            base_prefix = base_parcel[:-len(suffix)]  # Adjust the prefix based on suffix length
                            expanded_parcels.append(validate_parcel(clean_parcel(base_prefix + suffix.zfill(3))))
                        else:
                            expanded_parcels.append("Unknown")
                    else:
                        expanded_parcels.append(validate_parcel(clean_parcel(base_parcel)))

            # Handle ranges with 'to' (e.g., '3123-014-900 to 916')
            elif ' to ' in part:
                try:
                    start, end = part.split(' to ')
                    start = start.strip()
                    end = end.strip()

                    if base_parcel is None:
                        base_parcel = start  # Base is the starting parcel number

                    base_prefix = '-'.join(base_parcel.split('-')[:-1]) + '-'

                    start_number = int(start.split('-')[-1])
                    end_number = int(end)

                    # Expand the range and append the parcels
                    for i in range(start_number, end_number + 1):
                        expanded_parcels.append(validate_parcel(clean_parcel(base_prefix + f"{i:03d}")))
                except Exception:
                    expanded_parcels.append("Unknown")  # Handle invalid range format

            # Handle ranges with 'thru' (e.g., '3203-018-064 thru -071')
            elif 'thru' in part:
                try:
                    start, end = part.split('thru')
                    start = start.strip()
                    end = end.strip().strip('-')

                    if base_parcel is None:
                        base_parcel = start  # Base is the starting parcel number

                    base_prefix = '-'.join(base_parcel.split('-')[:-1]) + '-'

                    start_number = int(start.split('-')[-1])
                    end_number = int(end.split('-')[-1])

                    # Expand the range and append the parcels
                    for i in range(start_number, end_number + 1):
                        expanded_parcels.append(validate_parcel(clean_parcel(base_prefix + f"{i:03d}")))
                except Exception:
                    expanded_parcels.append("Unknown")  # Handle invalid range format

            # Handle full parcel numbers (e.g., '3219-018-01')
            elif part.count('-') == 2:
                base_parcel = part
                expanded_parcels.append(validate_parcel(clean_parcel(base_parcel)))

            # Handle comma-separated extensions (e.g., '02' from '3219-018-01')
            elif base_parcel:
                base_prefix = '-'.join(base_parcel.split('-')[:-1]) + '-'
                full_parcel = validate_parcel(clean_parcel(base_prefix + part.strip('-')))
                expanded_parcels.append(full_parcel)

            else:
                expanded_parcels.append("Unknown")  # Handle cases where no base parcel is found

    return expanded_parcels

# Function to process the DataFrame and expand the parcel numbers
def process_parcel_data(df):
    """Process the DataFrame and expand the parcel numbers."""
    if 'Location Parcel Number' not in df.columns:
        raise ValueError("Column 'Location Parcel Number' is missing in the DataFrame.")

    # Apply the function to expand parcel numbers
    df['Expanded Parcels'] = df['Location Parcel Number'].apply(expand_parcel_numbers)

    # Find the maximum number of parcels in any row
    max_columns = df['Expanded Parcels'].apply(len).max()

    # Create the new columns dynamically based on the max number of parcels
    parcel_columns = pd.DataFrame(df['Expanded Parcels'].to_list(), columns=[f'Parcel_{i+1}' for i in range(max_columns)])

    # Replace any NaN or None values in the DataFrame with "Unknown"
    parcel_columns.fillna("Unknown", inplace=True)

    # Concatenate the new columns back to the original DataFrame
    df = pd.concat([df, parcel_columns], axis=1)

    # Drop the 'Expanded Parcels' and original 'Location Parcel Number' column
    df.drop(['Expanded Parcels', 'Location Parcel Number'], axis=1, inplace=True)

    return df

# Function to combine parcel columns into a single 'Location Parcel Number' column
def combine_parcels(df, max_length=50):
    parcel_columns = [col for col in df.columns if col.startswith('Parcel_')]
    df['Location Parcel Number'] = df[parcel_columns].apply(
        lambda row: ', '.join(row[row != 'Unknown'].astype(str)), axis=1)

    # Truncate if necessary
    df['Location Parcel Number'] = df['Location Parcel Number'].apply(
        lambda val: val[:max_length] if len(val) > max_length else val
    )
    
    # replace any None values with 'Unknown'
    df['Location Parcel Number'] = df['Location Parcel Number'].replace({None: 'Unknown'})
    df['Location Parcel Number'] = df['Location Parcel Number'].replace({'': 'Unknown'})

    # Drop extra Parcel columns
    df.drop(parcel_columns, axis=1, inplace=True)
    
    return df

# Function to execute the full process on a given DataFrame
def run_parcel_expansion(data):
    """Main function to run the parcel expansion process."""
    # Ensure data contains 'SCH Number' and 'Location Parcel Number'
    if data.empty:
        raise ValueError("Data is empty.")

    if not set(['SCH Number', 'Location Parcel Number']).issubset(data.columns):
        raise ValueError("Data must contain 'SCH Number' and 'Location Parcel Number' columns")

    # Process the DataFrame to expand parcel numbers
    df_cleaned = process_parcel_data(data)

    # Combine the expanded parcels back into a single column
    df_cleaned = combine_parcels(df_cleaned)

    return df_cleaned

def generate_entry_id_and_date_gathered(df, sch_column='SCH Number', month_column='Received_month', day_column='Received_day', year_column='Received_year'):
    """
    Generate a unique 'entry_id' by concatenating the SCH Number, Received_month, Received_day, and Received_year,
    and add a 'date_gathered' column with the current date (month, day, and year).

    Args:
        df (pd.DataFrame): DataFrame containing the relevant columns for SCH Number and Received date parts.
        sch_column (str): Column name for SCH Number. Default is 'SCH Number'.
        month_column (str): Column name for the received month. Default is 'Received_month'.
        day_column (str): Column name for the received day. Default is 'Received_day'.
        year_column (str): Column name for the received year. Default is 'Received_year'.
    
    Returns:
        pd.DataFrame: The input DataFrame with new 'entry_id' and 'date_gathered' columns.
    """
    # Generate 'entry_id' by concatenating the relevant columns
    df.loc[:, 'entry_id'] = (
        df[sch_column].astype(str) + 
        df[month_column].astype(str) + 
        df[day_column].astype(str) + 
        df[year_column].astype(str)
    )
    
    # Get the current date and format it as month, day, and year
    current_date = datetime.now()
    df.loc[:, 'date_gathered'] = current_date.strftime('%Y-%m-%d')  # Adds date in 'YYYY-MM-DD' format

    return df

def reorder_filtered_columns(data):
    
    df_filtered = data[['entry_id', 'SCH Number', 'Lead Agency Title', 'Document Title', 
                           'Document Type', 'Received', 'Posted', 'Document Description', 
                           'Cities', 'Counties', 'Location Cross Streets', 
                           'Location Total Acres', 'NOC Project Issues', 
                           'NOC Public Review Start Date', 'NOC Public Review End Date', 
                           'NOE Exempt Status', 'NOE Exempt Citation', 'NOE Reasons for Exemption', 
                           'NOD Agency', 'NOD Approved By Lead Agency', 'NOD Approved Date', 
                           'NOD Significant Environmental Impact', 
                           'NOD Environmental Impact Report Prepared', 
                           'NOD Negative Declaration Prepared', 'NOD Other Document Type', 
                           'NOD Mitigation Measures', 'NOD Mitigation Reporting Or Monitoring Plan', 
                           'NOD Statement Of Overriding Considerations Adopted', 
                           'NOD Findings Made Pursuant', 'NOD Final EIR Available Location', 
                           'date_gathered', 'Location Parcel Number', 'Document Type Details']]
    
    # drop duplicate entry_id
    df_filtered.drop_duplicates(subset=['entry_id'], inplace=True)
    
    return df_filtered