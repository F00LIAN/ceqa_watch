import sys
import base64
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import psycopg2
import sqlalchemy
import streamlit as st
import pandas as pd
from streamlit_pdf_viewer import pdf_viewer
import plotly.express as px
import plotly.graph_objects as go

# Create a single form to add a few filters to query the database

