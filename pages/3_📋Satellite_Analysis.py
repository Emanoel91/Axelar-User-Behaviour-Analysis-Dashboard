import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import networkx as nx

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar User Behaviour Analysis Dashboard",
    page_icon="https://img.cryptorank.io/coins/axelar1663924228506.png",
    layout="wide"
)

# --- Title -----------------------------------------------------------------------------------------------------
st.title("📋Satellite Analysis")

st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Sidebar Footer Slightly Left-Aligned ---
st.sidebar.markdown(
    """
    <style>
    .sidebar-footer {
        position: fixed;
        bottom: 20px;
        width: 250px;
        font-size: 13px;
        color: gray;
        margin-left: 5px; # -- MOVE LEFT
        text-align: left;  
    }
    .sidebar-footer img {
        width: 16px;
        height: 16px;
        vertical-align: middle;
        border-radius: 50%;
        margin-right: 5px;
    }
    .sidebar-footer a {
        color: gray;
        text-decoration: none;
    }
    </style>

    <div class="sidebar-footer">
        <div>
            <a href="https://x.com/axelar" target="_blank">
                <img src="https://img.cryptorank.io/coins/axelar1663924228506.png" alt="Axelar Logo">
                Powered by Axelar
            </a>
        </div>
        <div style="margin-top: 5px;">
            <a href="https://x.com/0xeman_raz" target="_blank">
                <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz">
                Built by Eman Raz
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Snowflake Connection ----------------------------------------------------------------------------------------
snowflake_secrets = st.secrets["snowflake"]
user = snowflake_secrets["user"]
account = snowflake_secrets["account"]
private_key_str = snowflake_secrets["private_key"]
warehouse = snowflake_secrets.get("warehouse", "")
database = snowflake_secrets.get("database", "")
schema = snowflake_secrets.get("schema", "")

private_key_pem = f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----".encode("utf-8")
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
    backend=default_backend()
)
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# --- Date Inputs ---------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])

with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2022-01-01"))

with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-08-31"))

# --- Functions -------------------------------------------------------------------------------------------------------------------------------
# --- Row 1, 2 --------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def get_kpi_data(_conn, start_date, end_date):
    query = f"""
    WITH overview AS (
      WITH tab1 AS (
        SELECT block_timestamp::date AS date, tx_hash, source_chain, destination_chain, sender, token_symbol
        FROM AXELAR.DEFI.EZ_BRIDGE_SATELLITE
        WHERE block_timestamp::date >= '{start_date}'
      ),
      tab2 AS (
        SELECT 
            created_at::date AS date, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            sender_address AS user,
            CASE WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING) END AS amount,
            CASE 
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
              THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING) END AS amount_usd,
            SPLIT_PART(id, '_', 1) as tx_hash
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed' 
          AND simplified_status = 'received'
          AND created_at::date >= '{start_date}'
      )
      SELECT tab1.date, tab1.tx_hash, tab1.source_chain, tab1.destination_chain, sender, token_symbol, amount, amount_usd
      FROM tab1 
      LEFT JOIN tab2 ON tab1.tx_hash=tab2.tx_hash
    )
    SELECT 
      COUNT(DISTINCT tx_hash) AS transfers, 
      COUNT(DISTINCT sender) AS users,
      ROUND(SUM(amount_usd)) AS volume_usd,
      round(COUNT(distinct sender)/count(distinct date)) as avg_daily_users,
      round(count(distinct tx_hash)/count(distinct date)) as avg_daily_txns,
      round(sum(amount_usd)/count(distinct date)) as avg_daily_volume
    FROM overview
    WHERE date >= '{start_date}' AND date <= '{end_date}';
    """
    df = pd.read_sql(query, _conn)
    return df.iloc[0]

# --- Load KPI Data from Snowflake ---------------------------
kpi_df = get_kpi_data(conn, start_date, end_date)

# --- Display KPI (Row 1 & 2) --------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("**Total Users**")
    st.markdown(f"{kpi_df['USERS']/1000:.1f}K Wallets")
with col2:
    st.markdown("**Total Transfers**")
    st.markdown(f"{kpi_df['TRANSFERS']/1000:.1f}K Txns")
with col3:
    st.markdown("**Total Volume ($USD)**")
    st.markdown(f"${kpi_df['VOLUME_USD']/1_000_000:.1f}M")

col4, col5, col6 = st.columns(3)
with col1:
    st.markdown("**Average Daily Users**")
    st.markdown(f"{kpi_df['AVG_DAILY_USERS']:.1f} Wallets")
with col2:
    st.markdown("**Average Daily Transfers**")
    st.markdown(f"{kpi_df['AVG_DAILY_TXNS']:.1f} Txns")
with col3:
    st.markdown("**Average Daily Volume ($USD)**")
    st.markdown(f"${kpi_df['AVG_DAILY_VOLUME']/1000:.1f}K")

# --- Row 3 -----------------------------------------------------------------------------------------------------------------------------------------------------------

@st.cache_data
def load_user_time_series_data(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH UserRegistrations AS (
    SELECT
        sender,
        MIN(DATE_TRUNC('{timeframe}', block_timestamp)) AS first_registration_date
    FROM axelar.defi.ez_bridge_satellite
    GROUP BY sender
),
date_activity AS (
    SELECT
        DATE_TRUNC('{timeframe}', block_timestamp) AS activity_date,
        sender
    FROM axelar.defi.ez_bridge_satellite
    GROUP BY DATE_TRUNC('{timeframe}', block_timestamp), sender
),
NewAndReturning AS (
    SELECT
        a.activity_date,
        a.sender,
        CASE 
            WHEN a.activity_date = u.first_registration_date THEN 'New'
            ELSE 'Returning'
        END AS user_type
    FROM date_activity a
    JOIN UserRegistrations u ON a.sender = u.sender
)

SELECT
    activity_date AS "Date",
    COUNT(DISTINCT CASE WHEN user_type = 'New' THEN sender ELSE NULL END) AS "New Users",
    COUNT(DISTINCT CASE WHEN user_type = 'Returning' THEN sender ELSE NULL END) AS "Returning Users",
    COUNT(DISTINCT sender) AS "Total Users"
FROM NewAndReturning
WHERE activity_date::date >= '{start_str}' AND activity_date::date <= '{end_str}'
GROUP BY 1
ORDER BY 1
    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
df_user = load_user_time_series_data(timeframe, start_date, end_date)

# --- Charts in One Row ---------------------------------------------------------------------------------------------

col1, col2, col3= st.columns(3)

with col1:
    fig1 = px.bar(
        df_user,
        x="Date",
        y="New Users",
        title="Trend of New Users",
        labels={"New Users": "wallet count", "Date": " "},
        color_discrete_sequence=["#717aff"]
    )
    fig1.update_layout(xaxis_title="", yaxis_title="wallet count", bargap=0.2)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.bar(
        df_user,
        x="Date",
        y="Returning Users",
        title="Trend of Returning Users",
        labels={"Returning Users": "wallet count", "Date": " "},
        color_discrete_sequence=["#717aff"]
    )
    fig2.update_layout(xaxis_title="", yaxis_title="wallet count", bargap=0.2)
    st.plotly_chart(fig2, use_container_width=True)

with col3:
    fig3 = px.bar(
        df_user,
        x="Date",
        y="Total Users",
        title="Total Users Over Time",
        labels={"Total Users": "wallet count", "Date": " "},
        color_discrete_sequence=["#717aff"]
    )
    fig3.update_layout(xaxis_title="", yaxis_title="wallet count", bargap=0.2)
    st.plotly_chart(fig3, use_container_width=True)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def get_table_data(_conn, start_date, end_date):
    query = f"""
    WITH overview AS (
      WITH tab1 AS (
        SELECT block_timestamp::date AS date, tx_hash, source_chain, destination_chain, sender, token_symbol
        FROM AXELAR.DEFI.EZ_BRIDGE_SATELLITE
        WHERE block_timestamp::date >= '{start_date}' AND block_timestamp::date <= '{end_date}'
      ),
      tab2 AS (
        SELECT 
            created_at::date AS date, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            sender_address AS user,
            CASE WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING) END AS amount,
            CASE 
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
              THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING) END AS amount_usd,
            SPLIT_PART(id, '_', 1) as tx_hash
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed' 
          AND simplified_status = 'received'
          AND created_at::date >= '{start_date}' AND created_at::date <= '{end_date}'
      )
      SELECT tab1.date, tab1.tx_hash, tab1.source_chain, tab1.destination_chain, sender, token_symbol, amount, amount_usd
      FROM tab1 
      LEFT JOIN tab2 ON tab1.tx_hash=tab2.tx_hash
    )
    SELECT 
      sender as "👥Address",
      COUNT(DISTINCT tx_hash) AS "🚀Number of Transfers", 
      ROUND(SUM(amount_usd)) AS "💸Volume of Transfers ($USD)",
      count(distinct token_symbol) as "💎Number of Transferred Tokens",
      count(distinct (source_chain || '➡' || destination_chain)) as "🔀Number of Unique Paths",
      count(distinct date::date) as "📋#Activity Days"
    FROM overview
    group by 1
    order by 2 desc 
    limit 100
    """
    df = pd.read_sql(query, _conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
table_data = get_table_data(conn, start_date, end_date)

# --- Display Table ------------------------------------------------------------------------------------------------
st.subheader("🏆Top Users by Activity Level")

df_display = table_data.copy()
df_display.index = df_display.index + 1
df_display = df_display.applymap(lambda x: f"{x:,}" if isinstance(x, (int, float)) else x)
st.dataframe(df_display, use_container_width=True)
