import streamlit as st
import hashlib
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt

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
                SELECT fund_id, manager_id FROM "FUND" WHERE manager_id = {user}
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

@st.cache_resource
def fund_value_over_time(user):
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
                WHERE f.manager_id = {user}
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
        
                # Customize the y-axis to display values in millions
    fig.update_layout(
        yaxis=dict(
            title=None,
            tickformat=".0f",  # Remove decimals
            tickprefix="$",  # Add dollar sign
            ticksuffix="M",  # Add "M" for millions
        ),
        xaxis=dict(
            title=None,
            rangeslider=dict(visible=True)
        ),
        width=800,
        height=250,
        margin=dict(l=5, r=5, t=5, b=10),
    )
        
    return fig
    
@st.cache_resource
def fund_industry_exposure(user):
    
    # Industry exposure widget:
    query = f"""
            WITH fund_manager AS (
                SELECT fund_id, manager_id
                FROM "FUND"
                WHERE manager_id = {user}
                )
            SELECT SUM(ROUND((a.stock_quantity * s.current_price),2)) "Investment Value", s.industry "Industry" FROM "ASSET" a
            JOIN fund_manager fm ON fm.fund_id = a.fund_id
            JOIN "STOCK" s ON s.stock_id = a.stock_id
            WHERE asset_type = 'stock' AND stock_quantity > 0
            GROUP BY s.industry
            """
    
    df = conn.query(query)
    
    df["Industry Allocation"] = (df["Investment Value"] / df["Investment Value"].sum()) * 100
    df.sort_values("Industry Allocation", ascending=True, inplace=True)
    
    fig, ax = plt.subplots(figsize=(6,4))
    
    fig.patch.set_facecolor('#00172B')  
    ax.set_facecolor('#00172B')         
    ax.patch.set_facecolor('#00172B')
    
    ax.barh(df["Industry"], df["Industry Allocation"], zorder=100)
    
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(True)
    ax.spines["bottom"].set_visible(False)
    
    ax.spines["left"].set_color("white")
    ax.spines["left"].set_position(('outward', 10))
    
    ax.tick_params(colors='white')  # Tick labels

    ax.xaxis.tick_top()
    ax.xaxis.set_label_position("top")
    ax.grid(False)
    ax.grid(axis='x', linestyle='--', alpha=0.3, zorder = -100, color='white')
    
    return fig

def fund_portfolio_allocation(user, ticker):
    
    query = f"""
            WITH fund_manager AS (
                SELECT fund_id, manager_id FROM "FUND" WHERE manager_id = {user}
            ),
            stock_prices AS (
                SELECT stock_id, current_price, ticker 
                FROM "STOCK"
            ),
            asset_values AS (
                SELECT 
                    COALESCE(sp.ticker, 'Cash') as Asset,
                    COALESCE(a.cash_balance, 0) + COALESCE(ROUND(a.stock_quantity * sp.current_price, 2), 0) as "Asset Value"
                FROM "ASSET" a
                JOIN fund_manager fm ON a.fund_id = fm.fund_id
                LEFT JOIN stock_prices sp ON a.stock_id = sp.stock_id
                WHERE a.cash_balance > 0 OR (a.stock_quantity * sp.current_price) > 0
            ),
            ranked_assets AS (
                SELECT Asset, "Asset Value",
                    ROW_NUMBER() OVER (ORDER BY "Asset Value" DESC) as rank
                FROM asset_values
            )
            SELECT 
                CASE 
                    WHEN rank <= 5 THEN Asset
                    ELSE 'Other'
                END as "Asset",
                SUM("Asset Value") as "Asset Value"
            FROM ranked_assets
            GROUP BY 
                CASE 
                    WHEN rank <= 5 THEN Asset
                    ELSE 'Other'
                END
            ORDER BY "Asset Value" DESC;
            """

    df = conn.query(query)
        
    df["percentage"] = (df["Asset Value"] / df["Asset Value"].sum()) * 100 # Calculate percentages
    
    donut_ticker = ticker
        
    if donut_ticker not in df["Asset"].tolist():
        donut_ticker = "Other"
    
    labels = [f"{donut_ticker}\n{round(df.loc[df["Asset"] == donut_ticker, "percentage"].values[0],2)}%" for donut_ticker in df["Asset"]]
    explode = [0] * len(df["Asset Value"])
    
    selected_index = df.iloc[df[df["Asset"] == donut_ticker].index[0]].name
    explode[selected_index] = 0.05
    
    fig, ax = plt.subplots(figsize=(6,6))
    
    ax.pie(df["Asset Value"], radius=1.0, explode=explode, labels=labels, startangle=90)
    ax.pie(df["Asset Value"], radius=0.75, colors=['#00172B'], explode=explode, startangle=90)
    
    centre_circle = plt.Circle((0,0), 0.75, fc='#00172B')
    ax.add_artist(centre_circle)
    ax.axis('equal')
    
    return fig

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
        
        fig = fund_value_over_time(user['emp_id'])

        st.write("##### Fund Value Over Time")
        st.plotly_chart(fig)
        
        fig = fund_industry_exposure(user['emp_id'])
        
        st.write("##### Industry Exposure (%)")
        st.pyplot(fig)
        
    with col2:
        
        ticker = st.selectbox("Your stocks:", stocks_list)
        
        fig = fund_portfolio_allocation(user['emp_id'], ticker)
        
        st.write("##### Portfolio Allocation")
        st.pyplot(fig)
        
        
        
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