import streamlit as st
from google.oauth2 import service_account
import pandas as pd
from sqlalchemy import create_engine

# Create a connection function
def init_connection():
    # Create SSL certificates
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    
    # Configure the connection
    db_config = {
        "instance_connection_name": st.secrets["instance_connection_name"],
        "database": st.secrets["database"],
        "user": st.secrets["user"],
        "password": st.secrets["password"]
    }
    
    # Create the connection URL for PostgreSQL
    conn_url = (
        f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}'
        f'@/{db_config["database"]}?host=/cloudsql/'
        f'{db_config["instance_connection_name"]}'
    )
    
    # Create the connection
    return create_engine(conn_url)

# Create the connection in Streamlit
conn = st.connection('google_cloud_sql', type='sql')

# Example query using the connection
@st.cache_data(ttl=600)
def run_query(query):
    return conn.query(query)

# Usage example
df = run_query('SELECT * FROM "EMPLOYEE";')

st.dataframe(df)