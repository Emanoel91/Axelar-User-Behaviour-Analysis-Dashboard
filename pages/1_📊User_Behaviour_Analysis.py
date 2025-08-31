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
st.title("📊User Behaviour Analysis")

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

# --- Cached Query Runner ----------------------------------------------------------------------------
@st.cache_data(show_spinner=True, ttl=3600)
def run_query(query: str):
    return pd.read_sql(query, conn)


# --- Row 1: User Acquisition & Retention ------------------------------------------------------------
query_new_users = """
WITH tab1 AS (
    SELECT tx_from, MIN(block_timestamp::date) AS first_txn_date
    FROM AXELAR.CORE.FACT_TRANSACTIONS
    WHERE tx_succeeded='TRUE'
    GROUP BY 1
)
SELECT 
    DATE_TRUNC('day', first_txn_date) AS "Date",
    COUNT(DISTINCT tx_from) AS "New Users"
FROM tab1
GROUP BY 1
ORDER BY 1;
"""
df_new_users = run_query(query_new_users)

fig_new_users = px.bar(
    df_new_users,
    x="Date",
    y="New Users",
    title="User Acquisition Rate per Day",
    color_discrete_sequence=["orange"]
)

query_retention = """
with overview as (
WITH FirstTransaction AS (
  SELECT 
    tx_from,
    MIN(block_timestamp) AS first_transaction_time
  FROM axelar.core.fact_transactions
  where tx_succeeded='TRUE'
  GROUP BY tx_from
)

SELECT 
  DATE(FirstTransaction.first_transaction_time) AS "Cohort Date",
  DATE(transactions.block_timestamp) AS "Date",
  COUNT(DISTINCT transactions.tx_from) AS "Retained Users"
FROM axelar.core.fact_transactions AS transactions
JOIN FirstTransaction ON transactions.tx_from = FirstTransaction.tx_from
WHERE transactions.block_timestamp > FirstTransaction.first_transaction_time
GROUP BY 1, 2
ORDER BY 2)

select "Date", sum("Retained Users") as "Retained Users"
from overview 
group by 1
order by 1
"""
df_retention = run_query(query_retention)

fig_retention = px.line(
    df_retention,
    x="Date",
    y="Retained Users",
    title="User Retention per Day",
    color_discrete_sequence=["blue"]
)
fig_retention.update_traces(mode="lines")  

col1, col2 = st.columns(2)
col1.plotly_chart(fig_new_users, use_container_width=True)
col2.plotly_chart(fig_retention, use_container_width=True)


# --- Row 2: Transactions Count & Fees ---------------------------------------------------------------
query_txns_fees = """
SELECT 
    DATE_TRUNC('day', block_timestamp) AS "Date", 
    COUNT(DISTINCT tx_id) AS "Number of Transactions",
    ROUND(SUM(fee)/POW(10,6)) AS "Transaction Fees"
FROM AXELAR.CORE.FACT_TRANSACTIONS
WHERE tx_succeeded = TRUE
GROUP BY 1
ORDER BY 1;
"""
df_txns_fees = run_query(query_txns_fees)

fig_txn_count = px.bar(
    df_txns_fees,
    x="Date",
    y="Number of Transactions",
    title="Transactions Count per Day",
    color_discrete_sequence=["blue"]
)

fig_txn_fees = px.bar(
    df_txns_fees,
    x="Date",
    y="Transaction Fees",
    title="Transaction Fees per Day",
    color_discrete_sequence=["brown"]
)

col3, col4 = st.columns(2)
col3.plotly_chart(fig_txn_count, use_container_width=True)
col4.plotly_chart(fig_txn_fees, use_container_width=True)


# --- Row 3: Failed Transactions + Repeat Users Table -----------------------------------------------
query_failed = """
SELECT 
  DATE(block_timestamp) AS "Date",
  COUNT(DISTINCT tx_id) AS "Failed Transactions"
FROM axelar.core.fact_transactions
WHERE tx_succeeded = FALSE
GROUP BY 1
ORDER BY 1;
"""
df_failed = run_query(query_failed)


FIXED_HEIGHT = 500

fig_failed = px.line(
    df_failed,
    x="Date",
    y="Failed Transactions",
    title="Failed Transactions per Day",
    color_discrete_sequence=["red"]
)

fig_failed.update_traces(mode="lines")
fig_failed.update_layout(height=FIXED_HEIGHT)

query_repeat_users = """
SELECT 
  tx_from as "User",
  DATE(block_timestamp) AS "Txn Date",
  COUNT(distinct tx_id) AS "Txns Count"
FROM axelar.core.fact_transactions
GROUP BY "Txn Date", "User"
HAVING COUNT(distinct tx_id) > 1
ORDER BY "Txns Count" DESC
LIMIT 100;
"""
df_repeat_users = run_query(query_repeat_users).copy()
df_repeat_users.index = df_repeat_users.index + 1  

col5, col6 = st.columns(2)
with col5:
    st.plotly_chart(fig_failed, use_container_width=True)
with col6:
    st.subheader("Top 100 Users By No of Repeat Txns")
    
    st.dataframe(
        df_repeat_users,
        use_container_width=True,
        height=FIXED_HEIGHT
    )
