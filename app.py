import streamlit as st
import hashlib
import pandas as pd
import plotly.express as px

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
    user = user.iloc[0].to_dict() if not user.empty else None

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


# Manager view functions:
def get_fund_stocks(user):
    query = f"""
            WITH fund_manager AS (
                SELECT fund_id, manager_id FROM "FUND" WHERE manager_id = {user['emp_id']}
            ),
            stock_assets AS (
                SELECT * FROM "ASSET" a
                JOIN fund_manager fm ON fm.fund_id = a.fund_id
                WHERE asset_type = 'stock' AND stock_quantity > 0
            )
            SELECT ticker FROM "STOCK" s
            JOIN stock_assets sa ON sa.stock_id = s.stock_id
            """

    stocks = conn.query(query)
    stocks_list = stocks['ticker'].tolist()
    
    return stocks_list

def fund_value_widget(user):
    
    query = f"""
        WITH fund_manager AS (
            SELECT fund_id, manager_id FROM "FUND" WHERE manager_id = {user}
        ),
        stock_prices AS (
            SELECT stock_id, current_price 
            FROM "STOCK"
        )
        SELECT ROUND(SUM(a.cash_balance + (a.stock_quantity * sp.current_price)), 2) as total_value
        FROM "ASSET" a
        JOIN fund_manager fm ON a.fund_id = fm.fund_id
        JOIN stock_prices sp ON a.stock_id = sp.stock_id;
        """
    
    total_value_df = conn.query(query)
    total_value = total_value_df.iloc[0][0]
    
    st.metric("Total Fund Value", f"${total_value:,}")

def manager_view(user):

    green_triangle = ":green[▲]"
    red_triangle = ":red[▼]"
    
    # Get the list of stocks this fund holds
    stocks_list = get_fund_stocks(user['emp_id'])

    col1, sp1, col2, sp2, col3 = st.columns([9,1,9,1,9])

    with col1:

        st.header(f"Welcome, {user['fname']}!")

        # Current value widget:
        fund_value_widget(user['emp_id'])
        
        # Fund value over time widget:
        query = f"""
                WITH RECURSIVE date_range AS (
                    SELECT DATE_TRUNC('month', MIN(date)) AS month_date
                    FROM "STOCK_HISTORY"

                    UNION ALL

                    SELECT DATE_TRUNC('month', month_date + INTERVAL '1 month')
                    FROM date_range
                    WHERE month_date < (SELECT DATE_TRUNC('month', MAX(date)) FROM "STOCK_HISTORY")
                ),
                fund_assets AS (
                    SELECT ha.*
                    FROM "HISTORIC_ASSET" ha
                    INNER JOIN "FUND" f ON f.fund_id = ha.fund_id
                    WHERE f.manager_id = {user['emp_id']}
                ),
                latest_dates AS (
                    SELECT 
                        d.month_date,
                        MAX(fa.record_date) as latest_date
                    FROM date_range d
                    LEFT JOIN fund_assets fa 
                        ON DATE_TRUNC('month', fa.record_date) <= d.month_date
                    GROUP BY d.month_date
                ),
                monthly_assets AS (
                    SELECT 
                        ld.month_date,
                        ld.latest_date,
                        fa.*
                    FROM latest_dates ld
                    LEFT JOIN fund_assets fa ON fa.record_date = ld.latest_date
                ),
                asset_prices AS (
                    SELECT 
                        ma.*,
                        (
                            SELECT close_price 
                            FROM "STOCK_HISTORY" sh
                            WHERE sh.stock_id = ma.h_stock_id 
                            AND sh.date <= ma.month_date + INTERVAL '1 month' - INTERVAL '1 day'
                            ORDER BY sh.date DESC
                            LIMIT 1
                        ) as stock_price
                    FROM monthly_assets ma
                )
                SELECT month_date as "Date", SUM(h_cash_balance + (h_stock_quantity * stock_price)) as "Fund Value" FROM asset_prices
                GROUP BY month_date
                ORDER BY month_date DESC;
                """

        df = conn.query(query)

        df["Date"] = pd.to_datetime(df["Date"])
        df["Fund Value"] = df["Fund Value"] / 1e6

        fig = px.line(df, x="Date", y="Fund Value")
        
def ceo_view(user):
    return None
        

def main_page_logic(user):

    st.set_page_config(
        page_title="Dashboard",
        page_icon="./docs/logo.png",
        layout="wide"
    )
    
    role = user["role"]

    if role == "manager":
        manager_view(user)
    elif role == "CEO":
        ceo_view(user)
    
# Page deployment logic
if st.session_state.auth_status:
    main_page_logic(st.session_state.user)
else:
    auth_page()