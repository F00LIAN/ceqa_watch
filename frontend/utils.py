import sys
import base64
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import psycopg2
from datetime import timedelta, datetime
import streamlit as st
import pandas as pd
from streamlit_pdf_viewer import pdf_viewer
import plotly.express as px
import plotly.graph_objects as go

from const import database_columns

def format_finished(finished, error):
    return '✅' if finished else '❌' if error else '➖'

#def refresh_data():
#    st.session_state.data = fetch_data_from_db(st.session_state.db_name)

def get_db_connection():
    """Establish and return a PostgreSQL database connection."""
    conn = psycopg2.connect(
        host=st.session_state.db_host,
        database=st.session_state.db_name,
        user=st.session_state.db_user,
        password=st.session_state.db_password,
        port=st.session_state.db_port
    )
    return conn

def fetch_unique_doc_types(conn):
    """Fetch distinct document types from the database using an existing connection."""
    query = """
    SELECT DISTINCT document_type_details
    FROM ceqa_data
    WHERE document_type_details IN (
        'Notice of Preparation of a Draft EIR',
        'Draft Environmental Impact Report',
        'Notice of Determination',
        'Mitigated Negative Declaration'
    )
    """  # Adjust table name as needed
    
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchall()

    doc_types = [row[0] for row in result]
    return doc_types

def fetch_unique_cities(conn):
    """Fetch distinct cities from the database using an existing connection."""
    query = "SELECT DISTINCT lead_agency_title FROM ceqa_data"  # Adjust table name as needed
    
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchall()

    cities = [row[0] for row in result]
    return cities

def ceqa_received_dates(conn):
    """Fetch distinct received dates from the database using an existing connection."""
    query = "SELECT DISTINCT received FROM ceqa_data"  # Adjust table name as needed
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchall()

    received_dates = [row[0] for row in result]
    return received_dates

def fetch_filtered_data(conn, city_name, doc_type, date_range):
    """Fetch filtered data based on city, document type, and received date range."""
    
    # Base query
    query = "SELECT * FROM ceqa_data WHERE 1=1"

    # Dynamically build the query based on the filters
    if city_name != "All":
        query += f" AND lead_agency_title = '{city_name}'"
    
    if doc_type != "All":
        query += f" AND document_type_details = '{doc_type}'"
    
    # Date range filter based on the user's selection
    if date_range == "Three Days":
        date_threshold = datetime.now() - timedelta(days=3)
        query += f" AND received >= '{date_threshold.date()}'"
    elif date_range == "One Week":
        date_threshold = datetime.now() - timedelta(weeks=1)
        query += f" AND received >= '{date_threshold.date()}'"
    elif date_range == "One Month":
        date_threshold = datetime.now() - timedelta(days=30)
        query += f" AND received >= '{date_threshold.date()}'"

    # Execute the query
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchall()

    # Define the correct number of columns based on your table structure
    column_names = [
        "sch_number", "lead_agency_title", "document_title", 
        "document_type_details", "received", "posted", "document_description", "cities", 
        "counties", "location_cross_streets", "location_total_acres", "noc_project_issues", 
        "noc_public_review_start_date", "noc_public_review_end_date", "noc_exempt_status", 
        "noc_exempt_citation", "noc_reasons_for_exemption", "nod_agency", 
        "nod_approved_by_lead_agency", "nod_approved_date", "nod_significant_environmental_impact", 
        "nod_environmental_impact_report_prepared", "nod_negative_declaration_prepared", 
        "nod_other_document_type", "nod_mitigation_measures", 
        "nod_mitigation_reporting_or_monitoring_plan", "nod_statement_of_overriding_considerations_adopted", 
        "nod_findings_made_pursuant", "nod_final_eir_available_location", 
        "location_parcel_number", "date_gathered",  # Add any additional columns
        "new_column_1", "new_column_2"  # Placeholder names for extra columns if any
    ]

    # Convert the result to a DataFrame
    filtered_df = pd.DataFrame(result, columns=column_names)

    return filtered_df

def fetch_project_details(conn, sch_number):
    """Fetch detailed information for a specific project identified by the SCH number."""
    query = f"SELECT * FROM ceqa_data WHERE sch_number = '{sch_number}'"
    
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()

    if result:
        columns = [
            "entry_id", "sch_number", "lead_agency_title", "document_title", "document_type", "received", "posted", 
            "document_description", "cities", "counties", "location_cross_streets", "location_total_acres", 
            "noc_project_issues", "noc_public_review_start_date", "noc_public_review_end_date", "noc_exempt_status", 
            "noc_exempt_citation", "noc_reasons_for_exemption", "nod_agency", "nod_approved_by_lead_agency", 
            "nod_approved_date", "nod_significant_environmental_impact", "nod_environmental_impact_report_prepared", 
            "nod_negative_declaration_prepared", "nod_other_document_type", "nod_mitigation_measures", 
            "nod_mitigation_reporting_or_monitoring_plan", "nod_statement_of_overriding_considerations_adopted", 
            "nod_findings_made_pursuant", "nod_final_eir_available_location", "location_parcel_number", "date_gathered"
        ]
        project_details = pd.DataFrame([result], columns=columns)
        return project_details
    else:
        return None