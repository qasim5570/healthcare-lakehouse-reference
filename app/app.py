import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# Databricks config
cfg = Config()

# Query the SQL warehouse with Service Principal credentials
def sql_query_with_service_principal(query: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        credentials_provider=lambda: cfg.authenticate  # Uses SP credentials from the environment variables
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# Query the SQL warehouse with the user credentials
def sql_query_with_user_token(query: str, user_token: str) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        access_token=user_token  # Pass the user token into the SQL connect to query on behalf of user
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()


# ---------------------------------------------------------------------------
# CACHING — the single most important thing to get right in Streamlit.
#
# The whole script re-runs top to bottom on EVERY interaction: a keystroke, a
# dropdown change, a slider drag. Without a cache that means one warehouse
# query per keystroke, which is both slow and billable.
#
# ttl=300 refreshes every five minutes. The marts are rebuilt by a scheduled
# job, so five-minute-old data is not a problem; a query storm is.
#
# The user token is part of the cache key ON PURPOSE: two users with different
# Unity Catalog grants must not share a cached result, or the cache silently
# defeats the row-level security that user-token auth exists to provide.
#
# The leading underscore on _token tells Streamlit NOT to hash that argument —
# a credential should not end up in a cache key. It keys on token_key instead,
# which is a short non-secret digest.
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner="Querying the lakehouse…")
def run_query(query: str, token_key: str, _token: str) -> pd.DataFrame:
    """Cached wrapper around sql_query_with_user_token."""
    return sql_query_with_user_token(query, user_token=_token)


st.set_page_config(layout="wide")

st.header("Appointments Not Attended")
col1, col2 = st.columns([3, 1])
# Extract user access token from the request headers
user_token = st.context.headers.get('X-Forwarded-Access-Token')
token_key = (user_token or "anon")[-12:]      # short, non-secret cache key

# Query the SQL data with the user credentials
# data = sql_query_with_user_token("SELECT * FROM samples.nyctaxi.trips LIMIT 5000", user_token=user_token)

# avanti_dev query
DNA_BY_CLINIC = """
    SELECT clinic_name,
           sum(appointments_dna)    AS dna_count,
           sum(appointments_total)  AS appointments_total,
           sum(revenue_lost_to_dna) AS revenue_lost
    FROM   avanti_dev.gold.mart_clinic_monthly
    GROUP  BY clinic_name
    ORDER  BY dna_count DESC
"""

data = run_query(DNA_BY_CLINIC, token_key, user_token)

# avanti_dev query
with col1:
    st.subheader("Appointments missed, by clinic")
    st.bar_chart(data, x="clinic_name", y="dna_count", height=400)

with col2:
    st.subheader("Cost of non-attendance")
    st.metric("Total DNAs", f"{int(data['dna_count'].sum()):,}")
    st.metric("Revenue lost", f"${data['revenue_lost'].sum():,.0f}")
    worst = data.iloc[0]
    st.metric("Worst clinic", worst["clinic_name"],
              f"{int(worst['dna_count'])} missed")

st.dataframe(data, height=400, use_container_width=True)

# Clears the cache so a rebuilt mart shows immediately rather than waiting ttl.
if st.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()