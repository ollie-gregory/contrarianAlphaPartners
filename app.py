import streamlit as st
from urllib.parse import quote

# Get connection details from secrets
db_config = {
    "host": st.secrets["google_db"]["host"],  # Public IP of your Cloud SQL instance
    "database": st.secrets["google_db"]["database"],
    "user": st.secrets["google_db"]["user"],
    "password": st.secrets["google_db"]["password"],
    "port": "5432"  # Default PostgreSQL port
}

# URL encode the password
encoded_password = quote(db_config["password"])

# Create the connection URL for PostgreSQL
conn_url = (
    f'postgresql+psycopg2://{db_config["user"]}:{encoded_password}'
    f'@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
)

# Debugging: Print connection URL (without exposing sensitive info)
st.write(f"Connecting to database at {db_config['host']}")

# Create the connection with the URL specified
conn = st.connection(conn_url)

if conn:
    st.write("Connection successful!")