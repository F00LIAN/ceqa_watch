import os 
import streamlit as st
from dotenv import load_dotenv
from utils import (fetch_unique_doc_types, 
                   get_db_connection, 
                   fetch_unique_cities,
                   fetch_filtered_data)
from instructions import instructions_tab

# CEQA App for Land Acquisition Teams to track CEQA documents specifically for Early Identification of projects. 
# Load environment variables    
load_dotenv()

# Setup a session state variables 
def initialize_session_state():
    env_vars = {
        "db_host": "DB_HOST",
        "db_name": "DB_NAME",
        "db_user": "DB_USER",
        "db_password": "DB_PASSWORD",
        "db_port": "DB_PORT",
        'azure_storage_connection_string': 'AZURE_STORAGE_CONNECTION_STRING',
        'azure_storage_key': 'AZURE_STORAGE_KEY',
        'azure_container_name': 'AZURE_CONTAINER_NAME',
    }

    for var, env in env_vars.items():
        if var not in st.session_state:
            st.session_state[var] = os.getenv(env)

def render_instructions_tab():
    """Render the instructions tab."""
    st.header("Instructions")
    instructions_tab()

def render_explore_data_tab(conn):
    """Render the Explore Data tab with filters."""
    st.header("Explore Data")
    
    # Fetch unique values from the database for filters
    city_names = fetch_unique_cities(conn)
    doc_types = fetch_unique_doc_types(conn)

    # Add "All" as the first option for the select boxes
    city_names.insert(0, "All")
    doc_types.insert(0, "All")

    with st.form("filter_form"):
        # Select filters for City, Document Type, and Received Date Range
        city_name = st.selectbox("City Name", city_names)  # City filter
        doc_type = st.selectbox("Document Type", doc_types)  # Document type filter
        date_range = st.selectbox("Received Date Range", ["All", "Three Days", "One Week", "One Month"])  # Date range filter
        
        submit_button = st.form_submit_button("Filter")

        if submit_button:
            # Fetch and display filtered data
            filtered_data = fetch_filtered_data(conn, city_name, doc_type, date_range)
            st.write(f"Filtered by: City - {city_name}, Doc Type - {doc_type}, Date Range - {date_range}")
            st.dataframe(filtered_data)

def render_process_files_tab():
    """Render the Process Files tab."""
    st.header("Process Files")
    st.write("Process files content goes here...")

# -------------- Main Program --------------

def main():
    # Load environment variables
    load_dotenv()

    # Initialize session state variables
    initialize_session_state()

    # Establish database connection
    conn = get_db_connection()

    # Set page layout and title
    st.set_page_config(layout="wide")
    st.title("👁️‍🗨️ CEQA Project Watch")

    # Create tabs
    tabs = st.tabs(["🖥️ Instructions", "🔎 Explore Data", "🧠 Process Planning Docs"])

    # Render each tab's content
    with tabs[0]:
        render_instructions_tab()

    with tabs[1]:
        render_explore_data_tab(conn)

    with tabs[2]:
        render_process_files_tab()

    # Close database connection after use
    conn.close()

if __name__ == "__main__":
    main()