"""
dashboard/app.py — Streamlit dashboard
Run: streamlit run dashboard/app.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
import pandas as pd

from analysis.eda import run_eda
from analysis.charts import (
    chart_affordability,
    chart_volatility,
    chart_segment,
    chart_size,
    chart_price_trend,
    chart_price_distribution,
)
from dashboard.map import choropleth

# -------------------------------------------------------
# Page config
# -------------------------------------------------------
st.set_page_config(
    page_title="Vietnam Apartment Analytics",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -------------------------------------------------------
# Load data (cached)
# -------------------------------------------------------
@st.cache_data(ttl=3600)
def load():
    results = run_eda()
    return results

with st.spinner("Loading data..."):
    results = load()

df              = results["df"]
affordability   = results["affordability"]
volatility      = results["volatility"]
segment         = results["segment_distribution"]
size_dist       = results["size_distribution"]
trend           = results["price_trend"]

# -------------------------------------------------------
# Sidebar
# -------------------------------------------------------
st.sidebar.title("🏙️ Vietnam Apartment Analytics")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    ["Overview", "District Analysis", "Market Map", "Price Trends"],
)

city_filter = st.sidebar.selectbox("City", ["Both", "HCMC", "HN"])
st.sidebar.markdown("---")
st.sidebar.caption(f"Dataset: {len(df):,} listings | HCMC: {len(df[df['city']=='HCMC']):,} | HN: {len(df[df['city']=='HN']):,}")

# apply city filter to df
if city_filter != "Both":
    df_filtered = df[df["city"] == city_filter]
else:
    df_filtered = df

# -------------------------------------------------------
# Page: Overview
# -------------------------------------------------------
if page == "Overview":
    st.title("🏙️ Vietnam Apartment Market Analytics")
    st.markdown("Scraped from **batdongsan.com.vn** | HCMC & Hanoi | Căn hộ only")
    st.markdown("---")

    # KPI metrics
    col1, col2, col3, col4 = st.columns(4)

    priced = df_filtered[df_filtered["price_per_m2_m"].notna()]

    col1.metric("Total Listings",       f"{len(df_filtered):,}")
    col2.metric("Median Price/m²",      f"{priced['price_per_m2_m'].median():.1f} tr")
    col3.metric("Median Area",          f"{df_filtered['area_m2'].median():.0f} m²")
    col4.metric("Avg Bedrooms",         f"{df_filtered['bedrooms'].mean():.1f}")

    st.markdown("---")

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Market Segmentation")
        seg_data = segment if city_filter == "Both" else segment[segment["city"] == city_filter]
        st.plotly_chart(chart_segment(seg_data), use_container_width=True)

    with col_right:
        st.subheader("Apartment Size Distribution")
        size_data = size_dist if city_filter == "Both" else size_dist[size_dist["city"] == city_filter]
        st.plotly_chart(chart_size(size_data), use_container_width=True)

# -------------------------------------------------------
# Page: District Analysis
# -------------------------------------------------------
elif page == "District Analysis":
    st.title("📊 District Analysis")
    st.markdown("---")

    tab1, tab2, tab3 = st.tabs(["Affordability", "Price Volatility", "Price Distribution"])

    with tab1:
        city_aff = None if city_filter == "Both" else city_filter
        afford_data = affordability if city_filter == "Both" else affordability[affordability["city"] == city_filter]
        st.plotly_chart(chart_affordability(afford_data, city=city_aff), use_container_width=True)

        st.markdown("**Top 5 Most Affordable Districts**")
        st.dataframe(
            afford_data.nsmallest(5, "median_price_m2")[["city", "district", "listings", "median_price_m2"]],
            hide_index=True,
        )

        st.markdown("**Top 5 Most Expensive Districts**")
        st.dataframe(
            afford_data.nlargest(5, "median_price_m2")[["city", "district", "listings", "median_price_m2"]],
            hide_index=True,
        )

    with tab2:
        vol_data = volatility if city_filter == "Both" else volatility[volatility["city"] == city_filter]
        st.plotly_chart(chart_volatility(vol_data), use_container_width=True)
        st.markdown("*CV% = Coefficient of Variation — higher means more price variance in that district*")

    with tab3:
        dist_city = city_filter if city_filter != "Both" else "HCMC"
        if city_filter == "Both":
            dist_city = st.selectbox("Select city for distribution", ["HCMC", "HN"])
        st.plotly_chart(chart_price_distribution(df_filtered, city=dist_city), use_container_width=True)

# -------------------------------------------------------
# Page: Market Map
# -------------------------------------------------------
elif page == "Market Map":
    st.title("🗺️ Market Map")
    st.markdown("---")

    map_city   = st.selectbox("City", ["HCMC", "HN"])
    map_metric = st.selectbox(
        "Metric",
        ["median_price_m2", "avg_price_m2", "listings"],
        format_func=lambda x: {
            "median_price_m2": "Median Price/m² (triệu)",
            "avg_price_m2":    "Avg Price/m² (triệu)",
            "listings":        "# Listings",
        }[x]
    )

    fig = choropleth(df, city=map_city, metric=map_metric)
    st.plotly_chart(fig, use_container_width=True)

# -------------------------------------------------------
# Page: Price Trends
# -------------------------------------------------------
elif page == "Price Trends":
    st.title("📈 Price Trends")
    st.markdown("---")

    trend_data = trend if city_filter == "Both" else trend[trend["city"] == city_filter]
    st.plotly_chart(chart_price_trend(trend_data), use_container_width=True)

    st.markdown("---")
    st.subheader("Monthly Summary")
    st.dataframe(
        trend_data.sort_values(["city", "posted_month"], ascending=[True, False]),
        hide_index=True,
    )
