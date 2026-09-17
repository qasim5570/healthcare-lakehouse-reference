import os
from databricks import sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed
import streamlit as st
import pandas as pd

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# Databricks config
cfg = Config()
w = WorkspaceClient()
space_id = os.getenv("GENIE_SPACE_ID")

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

# st.write("GENIE_SPACE_ID:", os.getenv("GENIE_SPACE_ID"))

def genie_result_to_df(statement_response) -> pd.DataFrame:
    """Turn Genie's StatementResponse into a DataFrame.

    Genie returns row_count and a statement_id on the message, NOT the rows —
    the data is fetched separately and arrives in the same shape the SQL
    Statement Execution API uses: column metadata in the manifest, values as a
    list of string lists in data_array.
    """
    if statement_response is None or statement_response.manifest is None:
        return pd.DataFrame()

    columns = [c.name for c in statement_response.manifest.schema.columns]
    rows = (statement_response.result.data_array
            if statement_response.result and statement_response.result.data_array
            else [])
    df = pd.DataFrame(rows, columns=columns)

    # Everything arrives as strings. Convert a column only if EVERY value in it
    # is numeric — a partial conversion would silently turn real values into
    # NaN, which is worse than leaving the column as text.
    for col in df.columns:
        converted = pd.to_numeric(df[col], errors="coerce")
        if converted.notna().all():
            df[col] = converted

    return df


# ---------------------------------------------------------------------------
# PRESENTATION
#
# The Genie API returns text, SQL and rows — NOT the charts and metric cards
# the Genie UI renders. That formatting is a property of Databricks' own
# frontend, so it has to be rebuilt here. These helpers infer presentation from
# column NAMES, which is crude but works because the marts are named
# consistently: anything with revenue/cost/margin is money, anything with
# pct/rate is a proportion.
# ---------------------------------------------------------------------------
MONEY_HINTS = ("revenue", "cost", "margin", "fee", "lost", "value")
RATE_HINTS = ("pct", "rate", "ratio", "utilisation", "utilization")


def column_kind(name: str) -> str:
    """Classify a column by its name so it can be formatted sensibly."""
    low = name.lower()
    if any(h in low for h in RATE_HINTS):
        return "rate"
    if any(h in low for h in MONEY_HINTS):
        return "money"
    return "number"


def normalise_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Put every rate column on the same scale: percentage points.

    The marts store rates as PROPORTIONS (0.3431) while Genie's own generated
    SQL often multiplies by 100 (34.31). Mixing the two in one table is
    confusing, and a single format string cannot render both correctly.
    Anything at or below 1.5 is treated as a proportion and scaled up.
    """
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]) and column_kind(col) == "rate":
            values = out[col].dropna()
            if len(values) and values.abs().max() <= 1.5:
                out[col] = out[col] * 100
    return out


def column_config(df: pd.DataFrame) -> dict:
    """Format money as currency and rates as percentages in st.dataframe.

    Assumes normalise_rates has already run, so every rate is in percentage
    points.
    """
    cfg = {}
    for col in df.columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
        kind = column_kind(col)
        label = col.replace("_", " ").title()
        if kind == "money":
            cfg[col] = st.column_config.NumberColumn(label, format="$%.0f")
        elif kind == "rate":
            cfg[col] = st.column_config.NumberColumn(label, format="%.1f%%")
        else:
            cfg[col] = st.column_config.NumberColumn(label, format="%.0f")
    return cfg


def format_value(value, kind: str) -> str:
    """Render a single number for a metric card."""
    if pd.isna(value):
        return "—"
    if kind == "money":
        return f"${value:,.0f}"
    if kind == "rate":
        return f"{value:.1f}%"          # normalise_rates has already scaled these
    return f"{value:,.0f}"


TIME_HINTS = ("month", "date", "period", "year", "quarter", "week", "day")


def split_columns(df: pd.DataFrame):
    """Separate a result into time, category and measure columns.

    PROBLEM THIS SOLVES
      The shape of a Genie result varies by question, and code that assumes one
      shape breaks on another. "Which clinic is least profitable" returns one
      category and several measures. "Revenue by clinic by month" returns TWO
      label columns — a category AND a time axis — and charting it as bars keyed
      on the first label produces six bars per clinic stacked on one tick, which
      is meaningless.

      The fixed charts elsewhere in this app do not have this problem because
      the query is known in advance. Here it is generated, so the shape has to
      be inferred.
    """
    labels = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
    measures = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    times = [c for c in labels if any(h in c.lower() for h in TIME_HINTS)]
    categories = [c for c in labels if c not in times]
    return times, categories, measures


def render_result(df: pd.DataFrame, key: str) -> None:
    """Render a Genie result as cards, a chart and a table.

    PROBLEM THIS SOLVES
      The Genie API returns text, SQL and rows — NOT the charts and metric cards
      the Genie UI draws. That presentation belongs to Databricks' own frontend
      and has to be rebuilt here.

      More importantly: Genie's PROSE has been wrong three times while its SQL
      was right. It attributed low margin to high costs when costs were flat and
      revenue was low; it summarised three sample months in a way that read as
      "only these months have data". The table is not decoration — it is the
      correction for exactly that.
    """
    df = normalise_rates(df)
    times, categories, measures = split_columns(df)

    if df.empty or not measures:
        st.dataframe(df, use_container_width=True, hide_index=True)
        return

    # ---- headline cards --------------------------------------------------
    # The Genie UI leads with these, and for a ranked answer the top row IS the
    # answer. Skipped for time series, where "the first row" is just the
    # earliest month and means nothing on its own.
    if not times:
        top = df.iloc[0]
        heading = f" — {top[categories[0]]}" if categories else ""
        st.caption(f"Leading row{heading}")
        cards = st.columns(min(3, len(measures)))
        for card, m in zip(cards, measures[:3]):
            card.metric(m.replace("_", " ").title(),
                        format_value(top[m], column_kind(m)))

    # ---- chart -----------------------------------------------------------
    if len(df) > 1 and (times or categories):
        measure = st.selectbox(
            "Chart this measure", measures, key=f"measure_{key}"
        )

        if times:
            # A time axis means a LINE, not bars. With a category as well, one
            # line per category — which is what "revenue by clinic by month"
            # actually asks for.
            x = times[0]
            plot = df.sort_values(x)
            if categories:
                st.line_chart(plot, x=x, y=measure,
                              color=categories[0], height=380)
            else:
                st.line_chart(plot, x=x, y=measure, height=380)

        else:
            # One category, no time: horizontal bars, sorted by the measure.
            # Horizontal because clinic and practitioner names render vertically
            # on a normal bar chart and become unreadable.
            plot = df[[categories[0], measure]].dropna().sort_values(measure)
            try:
                import plotly.express as px

                fig = px.bar(
                    plot, x=measure, y=categories[0], orientation="h",
                    color=measure, color_continuous_scale="RdYlGn", text=measure,
                )
                kind = column_kind(measure)
                fmt = ("$%{text:,.0f}" if kind == "money"
                       else "%{text:.1f}%" if kind == "rate"
                       else "%{text:,.0f}")
                fig.update_traces(texttemplate=fmt, textposition="outside")
                fig.update_layout(
                    height=max(320, 60 * len(plot)),
                    margin=dict(l=10, r=10, t=30, b=10),
                    coloraxis_showscale=False,
                    xaxis_title=measure.replace("_", " ").title(),
                    yaxis_title="",
                )
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                # plotly is optional. A missing dependency should degrade the
                # PRESENTATION, not break the app.
                st.bar_chart(plot, x=categories[0], y=measure, height=350)

    # ---- the table, always ----------------------------------------------
    st.dataframe(
        df, use_container_width=True, hide_index=True,
        column_config=column_config(df),
    )
    st.caption(f"{len(df):,} row{'s' if len(df) != 1 else ''} returned")


# The viewing user's token, injected by the Apps runtime. Defined HERE rather
# than further down because the Genie block below re-runs its generated SQL as
# this user, not as the app.
user_token = st.context.headers.get('X-Forwarded-Access-Token')
token_key = (user_token or "anon")[-12:]      # short, non-secret cache key

question = st.chat_input("Ask the question about the clinics")
if question:
    with st.spinner("Thinking.."):
        try:
            response = w.genie.start_conversation_and_wait(
                space_id=space_id, content=question
            )
        except OperationFailed as exc:
            st.error(f"Genie could not answer that: {exc}")
            st.stop()

    st.markdown(f"**You asked:** {question}")

    sql_seen = []

    # Attachments come as a list, and each carries EITHER text OR a query —
    # never both, and sometimes neither. Guard rather than assume.
    for a in response.attachments or []:
        if a.text and a.text.content:
            # Streamlit's markdown treats $...$ as inline LaTeX. Genie returns
            # currency, so "$99,662 ... $136,461" gets parsed as a formula and
            # rendered italic with the text between them mangled. Escaping the
            # dollar signs stops that while keeping Genie's bold and bullets.
            st.markdown(a.text.content.replace("$", "\\$"))

        if a.query:
            if a.query.description:
                st.caption(a.query.description)
            if a.query.query:
                sql_seen.append(a.query.query)

    # The rows. Fetched with a SECOND call — the message only carries a
    # statement_id and a row count, not the data.
    # ---- the rows ---------------------------------------------------------
    # Genie's own get_message_query_result returns a manifest with the row count
    # but data_array=None and external_links=[] — the rows are not delivered
    # inline. Rather than chase chunked retrieval, RE-RUN the SQL Genie
    # generated.
    #
    # That is also the better architecture. Genie executes its query as the
    # APP's service principal, which sees everything. Running it through
    # run_query executes as the VIEWING USER, so Unity Catalog row filters and
    # column masks apply — which is the whole point of user-token auth.
    #
    # The cost is one extra execution of a query that has already run. At this
    # data volume that is irrelevant, and run_query caches it anyway.
    if sql_seen:
        try:
            df = run_query(sql_seen[0], token_key, user_token)
            if df.empty:
                st.info("The query returned no rows.")
            else:
                render_result(df, key=response.id)
        except Exception as exc:
            st.warning(f"Answer returned, but the query could not be re-run: {exc}")

    # Always show the SQL. During evaluation Genie produced one answer where the
    # figures were right and the narrative was wrong; the SQL is how a reader
    # checks. Kept in an expander so it does not clutter the answer.
    if sql_seen:
        with st.expander("Show the SQL Genie generated"):
            for q in sql_seen:
                st.code(q, language="sql")

    if not response.attachments:
        st.info("Genie returned no answer for that question. Try rephrasing it.")


st.header("Appointments Not Attended")
col1, col2 = st.columns([3, 1])
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