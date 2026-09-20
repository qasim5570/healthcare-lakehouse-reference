"""Avanti Command Centre — clinic analytics over the Gold lakehouse.

A fixed dashboard plus a natural-language question box backed by Databricks
Genie. Queries run against a SQL warehouse as the VIEWING USER, so Unity
Catalog enforces per-user grants on every path.

Design decisions, the failures behind them, and the reasoning for each
rendering helper are documented in docs/APP_AND_GENIE.md.
"""
import os

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.errors import OperationFailed

assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

cfg = Config()
w = WorkspaceClient()
space_id = os.getenv("GENIE_SPACE_ID")


# ---------------------------------------------------------------------------
# Querying
# ---------------------------------------------------------------------------
def sql_query_with_service_principal(query: str) -> pd.DataFrame:
    """Run a query as the APP. Every user sees the same data, whatever their grants."""
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()


def sql_query_with_user_token(query: str, user_token: str) -> pd.DataFrame:
    """Run a query as the VIEWING USER, so Unity Catalog applies their grants."""
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
        access_token=user_token,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()


@st.cache_data(ttl=300, show_spinner="Querying the lakehouse…")
def run_query(query: str, token_key: str, _token: str) -> pd.DataFrame:
    """Cached query. The whole Streamlit script reruns on every interaction, so
    without this there is one warehouse query per keystroke.

    The token is part of the cache key so users with different grants cannot
    share a result — but the leading underscore stops Streamlit hashing the
    credential itself. It keys on token_key, a short non-secret digest.
    """
    return sql_query_with_user_token(query, user_token=_token)

# ---------------------------------------------------------------------------
# Presentation
#
# The Genie API returns text, SQL and rows — not the charts and metric cards
# its own UI draws. All of that is rebuilt here, inferring format and chart
# type from column names.
# ---------------------------------------------------------------------------
MONEY_HINTS = ("revenue", "cost", "margin", "fee", "lost", "value")
RATE_HINTS = ("pct", "rate", "ratio", "utilisation", "utilization")
TIME_HINTS = ("month", "date", "period", "year", "quarter", "week", "day")


def column_kind(name: str) -> str:
    """Classify a column by name so it can be formatted. Works only because the
    marts are named consistently; reading Unity Catalog comments would be more
    robust."""
    low = name.lower()
    if any(h in low for h in RATE_HINTS):
        return "rate"
    if any(h in low for h in MONEY_HINTS):
        return "money"
    return "number"


def normalise_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Put every rate on the same scale: percentage points.

    The marts store rates as proportions (0.34) while Genie's generated SQL
    usually multiplies by 100 (34.31). Both appear, and no single format string
    renders both. The 1.5 threshold leaves room for rates above 100%.
    """
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]) and column_kind(col) == "rate":
            values = out[col].dropna()
            if len(values) and values.abs().max() <= 1.5:
                out[col] = out[col] * 100
    return out


def column_config(df: pd.DataFrame) -> dict:
    """Currency and percentage formatting for st.dataframe. Assumes
    normalise_rates has run."""
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
    """Render one number for a metric card."""
    if pd.isna(value):
        return "—"
    if kind == "money":
        return f"${value:,.0f}"
    if kind == "rate":
        return f"{value:.1f}%"
    return f"{value:,.0f}"


def split_columns(df: pd.DataFrame):
    """Separate a result into time, category and measure columns.

    Genie's result shape varies by question. "Which clinic is least profitable"
    gives one category and several measures; "revenue by clinic by month" gives
    a category AND a time axis, and charting that as bars keyed on the first
    label stacks six bars on one tick. The shape has to be inferred.
    """
    labels = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
    measures = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    times = [c for c in labels if any(h in c.lower() for h in TIME_HINTS)]
    categories = [c for c in labels if c not in times]
    return times, categories, measures


def render_result(df: pd.DataFrame, key: str) -> None:
    """Render a result as cards, a chart and a table.

    The table always renders. Genie's prose has been wrong several times while
    its SQL was right, so the data is the correction rather than decoration.
    """
    df = normalise_rates(df)
    times, categories, measures = split_columns(df)

    if df.empty or not measures:
        st.dataframe(df, use_container_width=True, hide_index=True)
        return

    # Cards are skipped for time series: there the first row is just the
    # earliest month, and presenting it as a headline would mislead.
    if not times:
        top = df.iloc[0]
        heading = f" — {top[categories[0]]}" if categories else ""
        st.caption(f"Leading row{heading}")
        cards = st.columns(min(3, len(measures)))
        for card, m in zip(cards, measures[:3]):
            card.metric(m.replace("_", " ").title(),
                        format_value(top[m], column_kind(m)))

    if len(df) > 1 and (times or categories):
        measure = st.selectbox("Chart this measure", measures, key=f"measure_{key}")

        if times:
            # A time axis means a line, not bars — with a category, one line each.
            x = times[0]
            plot = df.sort_values(x)
            if categories:
                st.line_chart(plot, x=x, y=measure, color=categories[0], height=380)
            else:
                st.line_chart(plot, x=x, y=measure, height=380)

        else:
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
                # A missing optional dependency should degrade the presentation,
                # not break the app.
                st.bar_chart(plot, x=categories[0], y=measure, height=350)

    st.dataframe(df, use_container_width=True, hide_index=True,
                 column_config=column_config(df))
    st.caption(f"{len(df):,} row{'s' if len(df) != 1 else ''} returned")


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------
st.set_page_config(layout="wide")

user_token = st.context.headers.get('X-Forwarded-Access-Token')
token_key = (user_token or "anon")[-12:]

question = st.chat_input("Ask a question about the clinics")
if question:
    with st.spinner("Thinking…"):
        try:
            response = w.genie.start_conversation_and_wait(
                space_id=space_id, content=question
            )
        except OperationFailed as exc:
            st.error(f"Genie could not answer that: {exc}")
            st.stop()

    st.markdown(f"**You asked:** {question}")

    sql_seen = []

    for a in response.attachments or []:
        if a.text and a.text.content:
            # Streamlit's markdown treats $...$ as LaTeX, so currency in the
            # answer gets parsed as a formula and rendered italic.
            st.markdown(a.text.content.replace("$", "\\$"))

        if a.query:
            if a.query.description:
                st.caption(a.query.description)
            if a.query.query:
                sql_seen.append(a.query.query)

    # Genie's get_message_query_result returns a manifest with the row count but
    # no data_array and no external_links — the rows are not delivered. So the
    # generated SQL is re-run here instead, which also means it executes as the
    # viewing user rather than as the app's service principal.
    if sql_seen:
        try:
            df = run_query(sql_seen[0], token_key, user_token)
            if df.empty:
                st.info("The query returned no rows.")
            else:
                render_result(df, key=response.id)
        except Exception as exc:
            st.warning(f"Answer returned, but the query could not be re-run: {exc}")

        with st.expander("Show the SQL Genie generated"):
            for q in sql_seen:
                st.code(q, language="sql")

    if not response.attachments:
        st.info("Genie returned no answer for that question. Try rephrasing it.")


# ---------------------------------------------------------------------------
# Fixed dashboard
# ---------------------------------------------------------------------------
st.header("Appointments Not Attended")
col1, col2 = st.columns([3, 1])

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

if st.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()
