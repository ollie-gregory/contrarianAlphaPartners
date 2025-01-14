import streamlit as st
import hashlib
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import mplfinance as mpf
import squarify
import numpy as np

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
    
    ax.pie(df["Asset Value"], radius=1.0, explode=explode, labels=labels, startangle=90, textprops={'color': 'white'})
    ax.pie(df["Asset Value"], radius=0.75, colors=['#00172B'], explode=explode, startangle=90)
    
    centre_circle = plt.Circle((0,0), 0.75, fc='#00172B')
    ax.add_artist(centre_circle)
    ax.axis('equal')
    
    fig.patch.set_facecolor('#00172B')
    ax.set_facecolor('#00172B')
    ax.patch.set_facecolor('#00172B')
    
    df = df.drop(columns=["percentage"])
    df.sort_values("Asset Value", ascending=False, inplace=True)
    df["Asset Value"] = df["Asset Value"].apply(lambda x: f"${x/1000000:.2f}M") # Format values as millions with dollar signs
    df = df.pivot_table(index=None, columns="Asset", values="Asset Value", aggfunc='first')
    
    return fig, df

@st.cache_resource
def get_top_performing_stocks(user):
    
    query = f"""
            WITH latest_month AS (
                SELECT DATE_TRUNC('month', MAX(date))::DATE AS latest_month
                FROM "STOCK_HISTORY"
            ), fund_manager AS (
                SELECT fund_id, manager_id
                FROM "FUND"
                WHERE manager_id = {user}
            ), stock_prices AS (
                SELECT
                    DISTINCT sh.stock_id, sh.ticker,
                    curr.close_price AS current_price,
                    prev.close_price AS previous_price,
                    curr.date AS current_price_date,
                    prev.date AS previous_price_date,
                CASE
                    WHEN prev.close_price > 0 THEN ((curr.close_price - prev.close_price) / prev.close_price * 100)
                    ELSE NULL
                END
                    AS growth_percentage
                FROM "STOCK_HISTORY" sh
                CROSS JOIN latest_month lm
                LEFT JOIN LATERAL (
                    SELECT
                        date,
                        close_price
                    FROM
                        "STOCK_HISTORY" curr
                    WHERE
                        curr.stock_id = sh.stock_id
                        AND curr.date <= lm.latest_month + INTERVAL '1 month - 1 day'
                    ORDER BY
                        date DESC
                    LIMIT 1) curr
                ON TRUE
                LEFT JOIN LATERAL (
                    SELECT
                        date,
                        close_price
                    FROM
                        "STOCK_HISTORY" prev
                    WHERE
                        prev.stock_id = sh.stock_id
                        AND prev.date <= (lm.latest_month - INTERVAL '1 day')
                    ORDER BY
                        date DESC
                    LIMIT
                        1) prev
                ON TRUE 
            ), price_growth_rates AS(
            SELECT
                stock_id,
                ticker,
                ROUND(current_price::numeric, 2) AS current_price,
                TO_CHAR(current_price_date, 'YYYY-MM-DD') AS "Current Price Date",
                ROUND(previous_price::numeric, 2) AS previous_price,
                TO_CHAR(previous_price_date, 'YYYY-MM-DD') AS "Previous Price Date",
                ROUND(growth_percentage::numeric, 2) AS growth_rate
            FROM
                stock_prices
            WHERE
                current_price IS NOT NULL
                AND previous_price IS NOT NULL
            ORDER BY
                growth_percentage DESC
            )
            SELECT
                ticker "Ticker",
                current_price "Current Price",
                growth_rate "Growth Rate"
            FROM "ASSET" a
            JOIN price_growth_rates pgr ON pgr.stock_id = a.stock_id
            JOIN fund_manager fm ON fm.fund_id = a.fund_id
            ORDER BY pgr.growth_rate DESC
            LIMIT(3);
            """
        
    df = conn.query(query)
    
    return df

def get_candlestick_chart(ticker):
    
    collection = database[ticker] # Access the relevant collection in the MongoDB database
    
    data = collection.find().sort('_id', -1)
    
    df = pd.DataFrame(list(data))
    
    df = df[['start_time', 'open_price', 'close_price', 'min_price', 'max_price']].rename(columns={'start_time': 'Datetime', 'open_price': 'Open', 'close_price': 'Close', 'min_price': 'Low', 'max_price': 'High'}, inplace=False)
    
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    df.set_index('Datetime', inplace=True)
    df.sort_index(inplace=True)
    
    mc = mpf.make_marketcolors(
        up='green',
        down='red',
        edge={'up': 'green', 'down': 'red'},
        volume='in',
        inherit=True
    )
    
    s = mpf.make_mpf_style(
        marketcolors=mc,
        gridcolor='w',
        gridstyle='--',
        facecolor='none',
        edgecolor='w',
        figcolor='none',
        y_on_right=False,
        rc={
               'text.color': 'w',
               'axes.labelcolor': 'w',
               'axes.titlecolor': 'w',
               'xtick.color': 'w',
               'ytick.color': 'w'
           }
    )
    
    fig, ax = mpf.plot(
        df,
        type='candle',
        style=s,
        ylabel_lower='Volume',
        datetime_format='%H:%M',
        returnfig=True
    )
    
    fig.patch.set_alpha(0)
    ax[0].patch.set_alpha(0)
    ax[0].set_ylabel(None)
    
    ax[0].spines["top"].set_visible(False)
    ax[0].spines["right"].set_visible(False)
    ax[0].spines["left"].set_visible(True)
    ax[0].spines["bottom"].set_visible(True)
    
    ax[0].grid(alpha=0.3)
    
    return fig

@st.cache_resource
def get_biggest_movers():
    
    query = f"""
            WITH latest_month AS (
                SELECT DATE_TRUNC('month', MAX(date))::DATE as latest_month
                FROM "STOCK_HISTORY"
            ),
            stock_prices AS (
                SELECT DISTINCT
                    sh.stock_id,
                    sh.ticker,
                    curr.close_price as current_price,
                    prev.close_price as previous_price,
                    curr.date as current_price_date,
                    prev.date as previous_price_date,
                    CASE 
                        WHEN prev.close_price > 0 THEN 
                            ((curr.close_price - prev.close_price) / prev.close_price * 100)
                        ELSE NULL 
                    END as growth_percentage
                FROM "STOCK_HISTORY" sh
                CROSS JOIN latest_month lm
                LEFT JOIN LATERAL (
                    SELECT date, close_price
                    FROM "STOCK_HISTORY" curr
                    WHERE curr.stock_id = sh.stock_id
                    AND curr.date <= lm.latest_month + INTERVAL '1 month - 1 day'
                    ORDER BY date DESC
                    LIMIT 1
                ) curr ON true
                LEFT JOIN LATERAL (
                    SELECT date, close_price
                    FROM "STOCK_HISTORY" prev
                    WHERE prev.stock_id = sh.stock_id
                    AND prev.date <= (lm.latest_month - INTERVAL '1 day')
                    ORDER BY date DESC
                    LIMIT 1
                ) prev ON true
            )
            SELECT 
                stock_id as "Stock ID",
                ticker as "Ticker",
                ROUND(current_price::numeric, 2) as "Current Price",
                TO_CHAR(current_price_date, 'YYYY-MM-DD') as "Current Price Date",
                ROUND(previous_price::numeric, 2) as "Previous Price",
                TO_CHAR(previous_price_date, 'YYYY-MM-DD') as "Previous Price Date",
                ROUND(growth_percentage::numeric, 2) as "Growth %"
            FROM stock_prices
            WHERE current_price IS NOT NULL 
            AND previous_price IS NOT NULL
            ORDER BY ABS(growth_percentage) DESC
            LIMIT(5);
            """
    
    df = conn.query(query)
    
    fig, ax = plt.subplots(figsize=(6,2.5))
    
    ax.bar(df['Ticker'], df['Growth %'], color=df['Growth %'].apply(lambda x: 'g' if x >= 0 else 'r'))
    
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(True)
    ax.spines["bottom"].set_visible(False)
    
    ax.axhline(0, color='white', linewidth=0.5)
    ax.grid(False)
    
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
        
        # Industry exposure widget
        fig = fund_industry_exposure(user['emp_id'])
        
        st.write("##### Industry Exposure (%)")
        st.pyplot(fig)
        
    with col2:
        
        ticker = st.selectbox("Your stocks:", stocks_list)
        
        fig, df1 = fund_portfolio_allocation(user['emp_id'], ticker)
        
        st.write("##### Portfolio Allocation")
        st.pyplot(fig)
        
        st.write(" ")
        
        df = get_top_performing_stocks(user['emp_id'])
        
        with st.container():
            
            st.write("##### Top Performing Stocks in Your Portfolio This Month")

            for i in range(len(df)):
                df_ticker = df.iloc[i]['Ticker']
                current_price = df.iloc[i]['Current Price']
                growth_rate = df.iloc[i]['Growth Rate']

                # Determine triangle and color
                if growth_rate >= 0:
                    triangle = f":green[{green_triangle}]"
                else:
                    triangle = f":red[{red_triangle}]"

                # Create columns for better alignment
                col10, col20, col30, col40 = st.columns([1, 2, 2, 2])  # Adjust column widths as needed
                with col10:
                    st.write(f"**{i+1}.**")
                with col20:
                    st.write(f"**{df_ticker}**")
                with col30:
                    st.write(f"${current_price:,.2f}")  # Format as currency
                with col40:
                    st.write(f"{triangle} {growth_rate:.2f}%")
        
    with col3:
        
        col4, col5 = st.columns([7,2])

        # Log out button
        with col5:
            if st.button("Logout"):
                st.session_state.auth_status = False  # Reset authentication status
                st.session_state.user = None
                st.rerun()
                
        fig = get_candlestick_chart(ticker)
                
        st.write(f"##### {ticker} Recent Price Movement ($USD)")
        st.pyplot(fig)
        
        st.dataframe(df1)
        
        fig = get_biggest_movers()
        
        st.write("##### Biggest Movers This Month")
        st.pyplot(fig)

# CEO view functions:
@st.cache_resource
def get_firm_value():
    query = f"""
            SELECT 
                ROUND(SUM(COALESCE(a.cash_balance, 0) + 
                          COALESCE(a.stock_quantity * s.current_price, 0)), 2) AS total_firm_value
            FROM 
                "ASSET" a
            LEFT JOIN 
                "STOCK" s
            ON 
                a.stock_id = s.stock_id;
            """
    
    df = conn.query(query)
    total_firm_value = df['total_firm_value'][0]
    
    st.metric("Total Firm Value", f"${total_firm_value:,}")

@st.cache_resource
def manager_performance():
    
    query = f"""
            WITH base_previous_dates AS (
                SELECT DISTINCT ON (fund_id) 
                    record_date,
                    fund_id
                FROM "HISTORIC_ASSET" 
                WHERE record_date <= NOW() - INTERVAL '1 month'
                ORDER BY fund_id, record_date DESC
            ),
            previous_assets AS (
                SELECT 
                    ha.*,
                    (
                        SELECT close_price
                        FROM "STOCK_HISTORY" sh
                        WHERE sh.stock_id = ha.h_stock_id
                        AND sh.date <= ha.record_date
                        ORDER BY sh.date DESC
                        LIMIT 1
                    ) as stock_price
                FROM "HISTORIC_ASSET" ha
                JOIN base_previous_dates bpd ON ha.record_date = bpd.record_date 
                    AND ha.fund_id = bpd.fund_id
                WHERE h_stock_quantity > 0 OR h_cash_balance > 0
            ),
            previous_values AS (
                SELECT 
                    fund_id,
                    ROUND(SUM(COALESCE(h_cash_balance, 0) + 
                              COALESCE(h_stock_quantity * stock_price, 0)), 2) AS previous_value
                FROM previous_assets
                GROUP BY fund_id
            ),
            current_values AS (
                SELECT
                    fund_id,
                    ROUND(SUM(COALESCE(a.cash_balance, 0) + 
                              COALESCE(a.stock_quantity * s.current_price, 0)), 2) AS current_value
                FROM "ASSET" a
                LEFT JOIN "STOCK" s ON a.stock_id = s.stock_id
                GROUP BY fund_id
            ),
            fund_manager AS (
                SELECT 
                    CONCAT_WS(' ', fname, lname) AS name, 
                    fund_id, 
                    manager_id 
                FROM "FUND"
                JOIN "EMPLOYEE" ON manager_id = emp_id
            )
            SELECT
                fm.name AS "Manager",
                ROUND(((cv.current_value - pv.previous_value)/pv.previous_value) * 100, 2) AS "Performance",
                cv.current_value AS "Current Value"
            FROM current_values cv
            JOIN previous_values pv ON cv.fund_id = pv.fund_id
            JOIN fund_manager fm ON fm.fund_id = cv.fund_id;
            """
    
    df = conn.query(query)
    
    return df

@st.cache_resource
def get_office_locations():
    query = """
            SELECT DISTINCT(city) FROM "OFFICE"
            """
            
    regions = ['All']
    
    regions_df = conn.query(query)
        
    for city in regions_df['city']:
        regions.append(city)
    
    return regions

@st.cache_resource
def get_region_allocations_df():
    
    query = f"""
            SELECT 
                o.city AS "Region",
                ROUND(SUM(f.current_value), 2) AS "Firm Value"
            FROM 
                "FUND" f
            JOIN 
                "OFFICE" o ON f.location_id = o.office_id
            GROUP BY 
                o.city
            ORDER BY "Firm Value" DESC;
            """
        
    df = conn.query(query)
    
    return df

def plot_region_allocations(df, region):
    
    df["percentage"] = (df["Firm Value"] / df["Firm Value"].sum()) *100

    labels = [f"{region}\n{round(df.loc[df["Region"] == region, "percentage"].values[0],2)}%" for region in df["Region"]]
    
    explode = [0] * len(df["Region"])
    
    if region != 'All':
      selected_index = df.iloc[df[df["Region"] == region].index[0]].name
      explode[selected_index] = 0.05
    
    fig, ax = plt.subplots(figsize=(6,3.5))
    
    fig.patch.set_facecolor('#00172B')
    ax.set_facecolor('#00172B')
    
    ax.pie(df["Firm Value"], labels=labels, startangle=90, explode=explode, textprops={'color': 'white'})
    ax.pie(df["Firm Value"], radius=0.75, colors=['#00172B'], explode=explode, startangle=90)
    
    centre_circle = plt.Circle((0,0), 0.75, fc='#00172B')
    ax.add_artist(centre_circle)
    ax.axis('equal')
    
    return fig

def investments_by_region(region):
    
    query = """
            SELECT 
                s.ticker AS "Ticker",
                s.company_name AS "Company",
                ROUND(SUM(a.stock_quantity * s.current_price), 2) AS "Total Investment Value"
            FROM 
                "ASSET" a
            JOIN 
                "STOCK" s ON a.stock_id = s.stock_id
            WHERE 
                a.asset_type = 'stock'
            GROUP BY 
                s.ticker, s.company_name
            ORDER BY 
                "Total Investment Value" DESC;
            """
    
    threshold = 2
    
    if region != 'All':
        query = f"""
                WITH regions AS (
                    SELECT city, fund_id FROM "FUND" f
                    JOIN "OFFICE" o ON o.office_id = f.location_id
                )
                SELECT 
                    s.ticker AS "Ticker",
                    s.company_name AS "Company",
                    ROUND(SUM(a.stock_quantity * s.current_price), 2) AS "Total Investment Value"
                FROM 
                    "ASSET" a
                JOIN 
                    "STOCK" s ON a.stock_id = s.stock_id
                JOIN
                    regions r ON r.fund_id = a.fund_id
                WHERE 
                    a.asset_type = 'stock' AND r.city = '{region}' AND a.stock_quantity > 0
                GROUP BY 
                    s.ticker, s.company_name
                ORDER BY 
                    "Total Investment Value" DESC;
                """
        
        threshold = 5

    df = conn.query(query)
        
    df["Percentage"] = (df["Total Investment Value"] / df["Total Investment Value"].sum()) * 100
        
    other = df[df["Percentage"] < threshold]["Percentage"].sum()
    
    df = df[df["Percentage"] >= threshold]
    df = pd.concat([pd.DataFrame({"Ticker": ["Other"], "Company": ["Other"], "Percentage": [other]}), df])
        
    fig, ax = plt.subplots(figsize=(6,3.5))
    ax.set_axis_off()
    
    # Treemap plot
    squarify.plot(
       sizes=df["Percentage"],
       label=df["Ticker"],
       ax=ax,
       pad=1
    )
    
    return fig

@st.cache_resource
def firm_industry_exposure():
    
    query = f"""
            SELECT 
                s.industry AS "Industry",
                ROUND(SUM(a.stock_quantity * s.current_price), 2) AS "Total Value",
                ROUND((SUM(a.stock_quantity * s.current_price) / (SELECT SUM(COALESCE(stock_quantity * current_price, 0)) 
                                                                    FROM "ASSET" a 
                                                                    JOIN "STOCK" s 
                                                                    ON a.stock_id = s.stock_id)
                                                                    ) * 100, 2) 
                AS "Percentage"
            FROM 
                "ASSET" a
            JOIN 
                "STOCK" s ON a.stock_id = s.stock_id
            WHERE 
                a.asset_type = 'stock'
            GROUP BY 
                s.industry
            ORDER BY 
                "Total Value" DESC;
            """
    
    df = conn.query(query)
        
    df.sort_values("Percentage", ascending=True, inplace=True)
    
    fig, ax = plt.subplots(figsize=(6,4.5))
    
    ax.barh(df["Industry"], df["Percentage"], zorder=100)
    
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(True)
    ax.spines["bottom"].set_visible(False)
    
    ax.spines["left"].set_position(('outward', 10))
    
    ax.xaxis.tick_top()
    ax.xaxis.set_label_position("top")
    
    ax.grid(False)
    ax.grid(axis='x', linestyle='--', alpha=0.3, zorder = -100)
    
    return fig

@st.cache_resource
def get_fund_values_over_time():
    
    fig, ax = plt.subplots(figsize=(6,3))
    
    for i in range(10):
        emp_id = 20001 + i
        
        query = f"""
                SELECT CONCAT_WS(' ', fname, lname) as Name FROM "EMPLOYEE" WHERE emp_id = {emp_id}
                """
        
        name = conn.query(query).iloc[0][0]
        
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
                    WHERE f.manager_id = {emp_id}
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
        df["Fund Value"] = df["Fund Value"]
        
        ax.plot(df["Date"], np.log(df["Fund Value"]), label=name)
    
    ax.set_ylabel("Fund Value (Log Scale)")
    
    ax.grid(False)
    ax.grid(axis='y', linestyle='--', alpha=0.3, zorder = -100)
    
    ax.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=2)
    
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(True)
    ax.spines["bottom"].set_visible(True)
    
    return fig

def ceo_view(user):

    green_triangle = ":green[▲]"
    red_triangle = ":red[▼]"

    col1, sp1, col2, sp2, col3 = st.columns([9,1,9,1,9])
    
    with col1:
        st.header(f"Welcome, {user['fname']}!")
        
        get_firm_value()
        
        df = manager_performance()
        
        st.write("##### Manager Performance (Monthly)")
        
        for i in range(len(df)):
            df_manager = df.iloc[i]['Manager']
            performance = df.iloc[i]['Performance']
            value = df.iloc[i]['Current Value']

            # Determine triangle and color
            if performance >= 0:
                triangle = f":green[{green_triangle}]"
            else:
                triangle = f":red[{red_triangle}]"

            # Create columns for better alignment
            col10, col20, col30 = st.columns([2, 2, 1])  # Adjust column widths as needed
            with col10:
                st.write(f"**{df_manager}**")
            with col20:
                st.write(f"${value:,.2f}")
            with col30:
                st.write(f"{triangle} {performance}%")
                
    with col2:
        
        regions = get_office_locations()
        
        region = st.selectbox("Regions", regions)
        
        df = get_region_allocations_df()
        fig = plot_region_allocations(df, region)
        
        st.write("##### Firm Value By Region")
        st.pyplot(fig)
        
        fig = investments_by_region(region)
        
        st.write("##### Firm Investments By Region")
        st.pyplot(fig)
        
    with col3:
        
        col4, col5 = st.columns([7,2])

        # Log out button
        with col5:
            if st.button("Logout"):
                st.session_state.auth_status = False  # Reset authentication status
                st.session_state.user = None
                st.rerun()
                
        fig = firm_industry_exposure()
        
        st.write("##### Industry Exposure (%)")
        st.pyplot(fig)
        
        fig = get_fund_values_over_time()
        
        st.write("##### Fund Values Over Time")
        st.pyplot(fig)

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