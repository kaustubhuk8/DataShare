# dashboard.py
import streamlit as st
import pandas as pd
import snowflake.connector
import altair as alt
import json
import plotly.express as px

# Load Snowflake config
with open('configs/snowflake_config.json') as f:
    config = json.load(f)

# Connect to Snowflake
try:
    conn = snowflake.connector.connect(
        user=config['username'],
        password=config['password'],
        account=config['account'],
        database=config['database'],
        schema=config['schema'],
        warehouse=config['warehouse']
    )
except Exception as e:
    st.error("❌ Failed to connect to Snowflake.")
    st.text(f"Error: {e}")
    st.stop()

# Load data from view
query = "SELECT * FROM safe_transaction_aggregates;"
try:
    df = pd.read_sql(query, conn)
    conn.close()
except Exception as e:
    st.error("❌ Failed to load data from Snowflake view.")
    st.text(f"Error: {e}")
    st.stop()

# Normalize column names to uppercase
df.columns = [col.strip().upper() for col in df.columns]
df["DAY"] = pd.to_datetime(df["DAY"], errors='coerce')

# Sidebar filters
st.sidebar.header("🔍 Filters")
regions = df['REGION'].dropna().unique().tolist()
categories = df['MERCHANT_CATEGORY'].dropna().unique().tolist()
selected_region = st.sidebar.selectbox("Select Region", ["All"] + sorted(regions))
selected_category = st.sidebar.selectbox("Select Merchant Category", ["All"] + sorted(categories))
min_date, max_date = df["DAY"].min(), df["DAY"].max()
date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date])

# Apply filters
filtered_df = df.copy()
if selected_region != "All":
    filtered_df = filtered_df[filtered_df["REGION"] == selected_region]
if selected_category != "All":
    filtered_df = filtered_df[filtered_df["MERCHANT_CATEGORY"] == selected_category]
if len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = filtered_df[
        (filtered_df["DAY"] >= pd.to_datetime(start_date)) &
        (filtered_df["DAY"] <= pd.to_datetime(end_date))
    ]

# Dashboard UI
st.title("🧊 Aggregate Spend Trends Dashboard (Clean Room Safe)")

# Executive Summary
st.markdown("## 💼 Executive Summary")
col1, col2, col3 = st.columns(3)
col1.metric("🧾 Total Spend", f"${filtered_df['TOTAL_SPEND'].sum():,.2f}")
col2.metric("👥 Unique Users", f"{filtered_df['USER_COUNT'].sum():,.0f}")
if not filtered_df.empty:
    col3.metric("📆 Date Range", f"{filtered_df['DAY'].min().date()} → {filtered_df['DAY'].max().date()}")

# Tabs
tab1, tab2 = st.tabs(["📊 Visualizations", "📎 Raw Data"])

with tab1:
    # Line chart
    st.subheader("📈 Daily Spend Trend")
    if not filtered_df.empty:
        trend = filtered_df.groupby("DAY")["TOTAL_SPEND"].sum().reset_index().sort_values("DAY")
        st.altair_chart(
            alt.Chart(trend).mark_line(point=True).encode(
                x="DAY:T", y="TOTAL_SPEND:Q", tooltip=["DAY:T", "TOTAL_SPEND:Q"]
            ).properties(height=300),
            use_container_width=True
        )
    else:
        st.warning("No data available for selected filters.")

    # Bar chart
    st.subheader("📊 Spend Breakdown by Merchant Category and Region")
    if not filtered_df.empty:
        pivot_df = filtered_df.pivot_table(
            index="MERCHANT_CATEGORY", columns="REGION", values="TOTAL_SPEND", aggfunc="sum"
        ).fillna(0)
        st.bar_chart(pivot_df)
    else:
        st.info("Adjust your filters to see bar chart data.")

    # Heatmap
        # Heatmap
    st.subheader("🗺️ US Spend Intensity by State")

    us_state_abbrs = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    }

    show_heatmap = (
        selected_region == "All" or
        selected_region in us_state_abbrs
    )

    if show_heatmap and not filtered_df.empty:
        map_data = filtered_df[filtered_df["REGION"].isin(us_state_abbrs)]
        map_data = map_data.groupby("REGION")["TOTAL_SPEND"].sum().reset_index()

        if not map_data.empty:
            fig = px.choropleth(
                map_data,
                locations="REGION",
                locationmode="USA-states",
                color="TOTAL_SPEND",
                color_continuous_scale="RdYlBu_r",
                scope="usa",
                labels={"TOTAL_SPEND": "Total Spend ($)"},
                title="State-wise Total Spend"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No U.S. state-level data available for this filter.")
    else:
        st.info("Map visualization unavailable.")


with tab2:
    st.subheader("📋 Filtered Data Table")
    st.dataframe(filtered_df)
    st.download_button(
        label="📥 Download as CSV",
        data=filtered_df.to_csv(index=False),
        file_name="filtered_spend_data.csv",
        mime="text/csv"
    )