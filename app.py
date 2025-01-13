import streamlit as st
import hashlib

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


# Manage auth state
if "auth_status" not in st.session_state:
    st.session_state.auth_status = False

if "user" not in st.session_state:
    st.session_state.user = None
    

# Connect to SQL database
conn = st.connection('sql')


# Connect to MongoDB
mongo_username = st.secrets['mongo']['username']
mongo_password = st.secrets['mongo']['password']

uri = f"mongodb+srv://{mongo_username}:{mongo_password}@realtimeprices.u7enq.mongodb.net/?retryWrites=true&w=majority&appName=RealTimePrices"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

database = client['Hourly_Data']


# Authentication page functions
def check_auth(username, password):

    username = username
    password_hash = hashlib.sha256(password.encode()).hexdigest() # Hash the password so it matches the database

    query = f"""
            SELECT *
            FROM "EMPLOYEE"
            WHERE username = '{username}' AND password = '{password_hash}'
            """

    user = conn.query(query)
    
    user = dict(user[0]) if user else None

    st.session_state.user = user

    if user:
        return True
    else:
        return False

def auth_page():
    st.set_page_config(
        page_title="Login",
        page_icon="./docs/logo.png",
        layout="centered"
    )

    st.header("Welcome to the Contrarian Alpha Partners Dashboard!")
    st.write("Please login to access your personalised dashboard.")

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if check_auth(username, password):
            st.session_state.auth_status = True
            st.rerun()
        else:
            st.error("Invalid username or password")
            
    st.write("""
            The manager login details display information relevant to the managers. They are:

            - Michael Burry,     Username: `mburry08`,        Password: `CDS4LIFE`
            - Mark Baum,         Username: `mbaum08`,        Password: `iHateWallStreet2020`
            - Jamie Shipley,     Username: `jshipley08`,      Password: `ilovetrading97`
            - Charlie Geller,    Username: `cgeller08`,       Password: `MrTiddles2014`
            - Warren Buffet,     Username: `wbuffet41`,       Password: `ValueInvestingSince1941`
            - Ray Dalio,         Username: `rdalio83`,        Password: `iHateRisk92`
            - Harper Stern,      Username: `hstern20`,        Password: `NYC-LDN19`
            - Nicole Craig,      Username: `ncraig95`,        Password: `MoneyMoney95`
            - Eric Tao,          Username: `etao81`,          Password: `KingOfTheBullPen2012`
            - Kathy Tao,         Username: `ktao03`,          Password: `Wharton99`

            The CEO login details display information about the firm's performance. They are:

            - David Solomon,     Username: `dsolomon18`,      Password: `iLoveGoldman99`""")
    

def main_page(user):
    st.write("hello, world")

if st.session_state.auth_status:
    main_page(st.session_state.user)
else:
    auth_page()