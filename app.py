import streamlit as st
import pandas as pd
from urllib.parse import quote

# Get connection details from secrets
db_config = {
    "host": st.secrets["google_db"]["host"],  # Public IP of your Cloud SQL instance
    "database": st.secrets["google_db"]["database"],
    "user": st.secrets["google_db"]["user"],
    "password": st.secrets["google_db"]["password"],
    "port": "5432"  # Default PostgreSQL port
}

encoded_password = quote(db_config["password"])

# Create the connection URL for PostgreSQL
conn_url = (
    f'postgresql+psycopg2://{db_config["user"]}:{encoded_password}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
)

st.write(conn_url)

# Create the connection with the URL specified
conn = st.connection(
    "google_cloud_sql",
    type="sql",
    url=conn_url
)

# Example query using the connection
@st.cache_data(ttl=600)
def run_query(query):
    return conn.query(query)

data = run_query('SELECT * FROM "EMPLOYEE"')