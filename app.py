import streamlit as st

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Create the connection with the URL specified
conn = st.connection('sql')

if conn:
    st.write("Connection successful!")
    
df = conn.query('SELECT * FROM "EMPLOYEE"')

st.write(conn)

st.dataframe(df)

mongo_username = st.secrets['mongo']['username']
mongo_password = st.secrets['mongo']['password']

uri = f"mongodb+srv://{mongo_username}:{mongo_password}@realtimeprices.u7enq.mongodb.net/?retryWrites=true&w=majority&appName=RealTimePrices"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

mongodb = client['Hourly_Data']

ticker = 'AAPL'

# Stock price movement widget:
collection = mongodb[ticker] # Access the relevant collection in the MongoDB database
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

st.write(f"##### {ticker} Recent Price Movement ($USD)")
st.pyplot(fig)