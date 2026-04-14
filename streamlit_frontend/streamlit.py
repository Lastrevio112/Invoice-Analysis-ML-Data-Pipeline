# Data is refreshed once every 24 hours as per ttl=86400 seconds in the cache settings.

import streamlit as st
import polars as pl
import plotly.express as px
from streamlit_plotly_events import plotly_events

from dotenv import load_dotenv
from google.cloud import bigquery
load_dotenv("/workspace/.env")

bq = bigquery.Client(project="invoiceanalysispipeline")

st.set_page_config(layout="wide") # Better for BI-style dashboards

CACHE_TIME_SECONDS = 86400 # refresh data once per day

# Loading necessary data into cache to build up semantic model
@st.cache_data(ttl=CACHE_TIME_SECONDS)
def get_semantic_model():
    # Selecting core aggregation columns to stay under 1GB RAM
    query = """
    SELECT 
        fil.invoice_date, fil.spend, fil.item_qty,
        db.buyer_desc, dv.vendor_desc
    FROM invoiceanalysispipeline.invoices_gold.fact_invoice_line fil
    LEFT JOIN invoiceanalysispipeline.invoices_gold.dim_buyer db ON fil.buyer_no_id = db.buyer_no_id
    LEFT JOIN invoiceanalysispipeline.invoices_gold.dim_vendor dv ON fil.vendor_no_id = dv.vendor_no_id
    """
    arrow_table = bq.query(query).to_arrow()
    return pl.from_arrow(arrow_table)

@st.cache_data(ttl=CACHE_TIME_SECONDS)
def get_frequent_items():
    # Items that appear more than once
    query = """
    WITH CTE AS (
      SELECT 
        REGEXP_REPLACE(TRIM(REPLACE(item_desc, ',', '')), r'\s+', ' ') AS cleaned_desc
      FROM invoiceanalysispipeline.invoices_gold.fact_invoice_line
      GROUP BY 1
      HAVING COUNT(*) > 1
    )
    SELECT cleaned_desc FROM CTE ORDER BY cleaned_desc ASC
    """
    return bq.query(query).to_arrow().to_pydict()['cleaned_desc']

@st.cache_data(ttl=CACHE_TIME_SECONDS)
def get_item_evolution(selected_item):
    query = """
    SELECT 
        invoice_date, SUM(item_qty) as total_qty, SUM(spend) as total_spend
    FROM invoiceanalysispipeline.invoices_gold.fact_invoice_line
    WHERE REGEXP_REPLACE(TRIM(REPLACE(item_desc, ',', '')), r'\s+', ' ') = @item
    GROUP BY invoice_date
    ORDER BY invoice_date ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("item", "STRING", selected_item)]
    )
    return pl.from_arrow(bq.query(query, job_config=job_config).to_arrow())


# --- INITIALIZE BASE DATA ---
raw_data = get_semantic_model()

# SIDEBAR (Global Controls)
if st.sidebar.button("Clear All Filters"):
    # Clearing the keys triggers a rerun with default values
    for key in ["vendor_select", "buyer_select", "item_select"]:
        if key in st.session_state:
            st.session_state[key] = [] if "select" in key else None
    st.rerun()

st.sidebar.header("Global Filters")

# Vendor/Buyer Multiselects
available_vendors = sorted(raw_data["vendor_desc"].unique().to_list())
selected_vendor = st.sidebar.multiselect("Select Vendor", options=available_vendors, key="vendor_select")

available_buyers = sorted(raw_data["buyer_desc"].unique().to_list())
selected_buyer = st.sidebar.multiselect("Select Buyer", options=available_buyers, key="buyer_select")

# Calendar Filter
min_date = raw_data["invoice_date"].min()
max_date = raw_data["invoice_date"].max()
if "date_range" not in st.session_state:
    st.session_state.date_range = (min_date, max_date)
date_range = st.sidebar.date_input("Select Date Range", value=(min_date, max_date))


# Drill-down Picker
st.sidebar.divider()
st.sidebar.subheader("Drill-down Analysis")
frequent_items = get_frequent_items()
selected_item = st.sidebar.selectbox("Select Item Description", options=[None] + frequent_items)

# THE SEMANTIC ENGINE (Additive Filtering)
lf = raw_data.lazy()

if selected_vendor:
    lf = lf.filter(pl.col("vendor_desc").is_in(selected_vendor))
if selected_buyer:
    lf = lf.filter(pl.col("buyer_desc").is_in(selected_buyer))
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    lf = lf.filter(pl.col("invoice_date").is_between(date_range[0], date_range[1]))

final_df = lf.collect()

tab_overview, tab_vendorbuyer, tab_item, tab_heatmap = st.tabs(["Overview", "Vendor & Buyer Analysis", "Item Deep-Dive", "Spend Time Analysis"])

# UI: SPEND OVERVIEW
with tab_overview:
    st.subheader("Global Spend Evolution")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Spend", f"${final_df['spend'].sum():,.2f}")
    col2.metric("Total Qty", f"{final_df['item_qty'].sum():,}")
    col3.metric("Records Found", f"{len(final_df):,}")

    # Aggregate for the line chart (Grouping by date so it's not a messy scatter)
    chart_data = final_df.group_by("invoice_date").agg(pl.col("spend").sum()).sort("invoice_date")
    st.line_chart(chart_data, x="invoice_date", y="spend", x_label="Invoice Date", y_label="Spend")

# UI: Spend by vendor and buyer
with tab_vendorbuyer:
    st.subheader("Vendor & Buyer proportion of Spend:")

    treemap = px.treemap(
        final_df.to_pandas(), 
        path=['vendor_desc', 'buyer_desc'], 
        values='spend',
        title="Hierarchy of Spend: Vendor > Buyer",
        color_continuous_scale='Blues'
    )

    st.plotly_chart(treemap, use_container_width=True)

    final_df_filtered = final_df.with_columns(
        pl.when(pl.col("spend") < 10000)
        .then(pl.lit("Other"))
        .otherwise(pl.col(name))
        .alias(name)
        for name in ["vendor_desc", "buyer_desc"]
    )

    donut_vendor = px.pie(
        final_df_filtered, 
        values='spend',
        names='vendor_desc',
        title='Proportion of spend by vendor',
        hole=0.4
        )
    
    donut_buyer = px.pie(
        final_df_filtered, 
        values='spend',
        names='buyer_desc',
        title='Proportion of spend by buyer',
        hole=0.4
        )
    
    st.plotly_chart(donut_vendor)
    st.plotly_chart(donut_buyer)

# UI: ITEM DRILL DOWN
with tab_item:
    st.divider()
    if selected_item:
        st.subheader(f"Deep Dive: {selected_item}")
        item_df = get_item_evolution(selected_item)
    
        col_a, col_b = st.columns(2)
        with col_a:
            st.write("**Qty Over Time**")
            st.line_chart(item_df, x="invoice_date", y="total_qty")
        with col_b:
            st.write("**Spend Over Time**")
            st.line_chart(item_df, x="invoice_date", y="total_spend")
    else:
        st.info("💡 Tip: Select a specific Item Description in the sidebar to see its individual evolution.")


# UI: Time heatmap:

with tab_heatmap:
    heatmap_data = (
        final_df
        .with_columns([
            pl.col("invoice_date").dt.strftime("%b").alias("month"),      # e.g., 'Jan'
            pl.col("invoice_date").dt.weekday().alias("weekday_num"),     # 1-7
            pl.col("invoice_date").dt.strftime("%A").alias("weekday")     # e.g., 'Monday'
        ])
        # Group and count occurrences
        .group_by(["month", "weekday", "weekday_num"])
        .agg(pl.len().alias("invoice_count"))
        # Sort to ensure months and days appear in chronological order
        .sort("weekday_num")
    )

    pivot_df = heatmap_data.to_pandas().pivot(
        index="weekday", 
        columns="month", 
        values="invoice_count"
    ).fillna(0)

    # Ensure months are in calendar order, not alphabetical
    month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    existing_months = [m for m in month_order if m in pivot_df.columns]
    pivot_df = pivot_df[existing_months]

    # Ensure weekdays are in order
    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    existing_days = [d for d in weekday_order if d in pivot_df.index]
    pivot_df = pivot_df.reindex(existing_days)

    st.subheader("Invoice Volume Heatmap: Detect 'Dumping' Patterns")
    st.caption("Darker areas indicate higher volumes of invoices processed.")

    fig_heatmap = px.imshow(
        pivot_df,
        labels=dict(x="Month", y="Day of Week", color="No. of Invoices"),
        x=pivot_df.columns,
        y=pivot_df.index,
        color_continuous_scale="Viridis",
        aspect="auto",
        text_auto=True # Displays the numbers inside the boxes
    )

    fig_heatmap.update_layout(
        xaxis_nticks=12,
        template="plotly_dark" # Keep the BI look consistent
    )

    st.plotly_chart(fig_heatmap, use_container_width=True)