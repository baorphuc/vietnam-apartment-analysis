"""
analysis/charts.py — Plotly chart functions
Each function receives a DataFrame, returns a plotly Figure
Export to HTML: fig.write_html("output.html")
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

# -------------------------------------------------------
# Color palette
# -------------------------------------------------------
COLORS = {
    "HCMC":       "#E63946",
    "HN":         "#457B9D",
    "affordable": "#2DC653",
    "mid-range":  "#F4A261",
    "luxury":     "#E63946",
}

TEMPLATE = "plotly_white"


# -------------------------------------------------------
# 1. District Affordability — horizontal bar chart
# -------------------------------------------------------

def chart_affordability(df_afford: pd.DataFrame, city: str = None) -> go.Figure:
    """
    Horizontal bar: median price/m² by district.
    df_afford: output of district_affordability()
    """
    data = df_afford.copy()
    if city:
        data = data[data["city"] == city]

    data = data.sort_values("median_price_m2", ascending=True)

    fig = px.bar(
        data,
        x="median_price_m2",
        y="district",
        color="city",
        orientation="h",
        text="median_price_m2",
        color_discrete_map=COLORS,
        title=f"District Affordability Ranking — Median Price/m² (triệu VND)" + (f" — {city}" if city else ""),
        labels={
            "median_price_m2": "Median Price/m² (triệu)",
            "district": "District",
            "city": "City",
        },
        template=TEMPLATE,
    )
    fig.update_traces(texttemplate="%{text:.1f}tr", textposition="outside")
    fig.update_layout(
        height=max(400, len(data) * 28),
        xaxis_title="Median Price/m² (triệu VND)",
        yaxis_title="",
        legend_title="City",
    )
    return fig


# -------------------------------------------------------
# 2. Price Volatility — scatter plot (CV vs median price)
# -------------------------------------------------------

def chart_volatility(df_vol: pd.DataFrame) -> go.Figure:
    """
    Scatter: x=median price/m², y=CV%, size=listings count.
    df_vol: output of price_volatility()
    """
    fig = px.scatter(
        df_vol,
        x="mean",
        y="cv",
        size="listings",
        color="city",
        text="district",
        color_discrete_map=COLORS,
        title="Price Volatility by District (CV% vs Mean Price/m²)",
        labels={
            "mean": "Mean Price/m² (triệu)",
            "cv":   "Coefficient of Variation (%)",
            "listings": "# Listings",
            "city": "City",
        },
        template=TEMPLATE,
    )
    fig.update_traces(textposition="top center", textfont_size=10)
    fig.update_layout(height=500)
    return fig


# -------------------------------------------------------
# 3. Segment Distribution — grouped bar chart
# -------------------------------------------------------

def chart_segment(df_seg: pd.DataFrame) -> go.Figure:
    """
    Grouped bar: affordable / mid-range / luxury split by city.
    df_seg: output of segment_distribution()
    """
    fig = px.bar(
        df_seg,
        x="segment",
        y="pct",
        color="city",
        barmode="group",
        text="pct",
        color_discrete_map=COLORS,
        title="Market Segmentation by City (%)",
        labels={
            "segment": "Segment",
            "pct":     "% of Listings",
            "city":    "City",
        },
        template=TEMPLATE,
    )
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(height=450, yaxis_title="% of Listings")
    return fig


# -------------------------------------------------------
# 4. Size Distribution — pie charts side by side
# -------------------------------------------------------

def chart_size(df_size: pd.DataFrame) -> go.Figure:
    """
    Side-by-side pie charts: size bucket distribution per city.
    df_size: output of size_distribution()
    """
    cities = df_size["city"].unique()
    fig = make_subplots(
        rows=1, cols=len(cities),
        specs=[[{"type": "pie"}] * len(cities)],
        subplot_titles=[f"{c}" for c in cities],
    )

    for i, city in enumerate(cities):
        data = df_size[df_size["city"] == city]
        fig.add_trace(
            go.Pie(
                labels=data["size_bucket"].astype(str),
                values=data["count"],
                name=city,
                textinfo="label+percent",
                hole=0.35,
            ),
            row=1, col=i + 1,
        )

    fig.update_layout(
        title="Apartment Size Distribution by City",
        height=420,
        template=TEMPLATE,
    )
    return fig


# -------------------------------------------------------
# 5. Price Trend — line chart
# -------------------------------------------------------

def chart_price_trend(df_trend: pd.DataFrame) -> go.Figure:
    """
    Line chart: median price/m² over time by city.
    df_trend: output of price_trend()
    """
    fig = px.line(
        df_trend,
        x="posted_month",
        y="median_price_m2",
        color="city",
        markers=True,
        color_discrete_map=COLORS,
        title="Median Price/m² Trend by Month",
        labels={
            "posted_month":    "Month",
            "median_price_m2": "Median Price/m² (triệu)",
            "city":            "City",
        },
        template=TEMPLATE,
    )
    fig.update_layout(height=420, xaxis_title="Month", yaxis_title="Median Price/m² (triệu VND)")
    return fig


# -------------------------------------------------------
# 6. Price Distribution — box plot by district
# -------------------------------------------------------

def chart_price_distribution(df: pd.DataFrame, city: str) -> go.Figure:
    """
    Box plot: price/m² distribution per district for one city.
    df: full cleaned DataFrame from feature_engineering()
    """
    data = df[(df["city"] == city) & df["price_per_m2_m"].notna()].copy()

    # sort districts by median
    order = (
        data.groupby("district")["price_per_m2_m"]
        .median()
        .sort_values()
        .index.tolist()
    )

    fig = px.box(
        data,
        x="price_per_m2_m",
        y="district",
        color="segment",
        category_orders={"district": order},
        title=f"Price/m² Distribution by District — {city}",
        labels={
            "price_per_m2_m": "Price/m² (triệu VND)",
            "district":       "District",
            "segment":        "Segment",
        },
        template=TEMPLATE,
    )
    fig.update_layout(
        height=max(400, len(order) * 30),
        xaxis_title="Price/m² (triệu VND)",
        yaxis_title="",
    )
    return fig


# -------------------------------------------------------
# Export all charts to HTML
# -------------------------------------------------------

def export_all(results: dict, output_dir: str = "data/charts"):
    """
    Export all charts as standalone HTML files.
    results: output of eda.run_eda()
    """
    import os
    os.makedirs(output_dir, exist_ok=True)

    df          = results["df"]
    affordability = results["affordability"]
    volatility    = results["volatility"]
    segment       = results["segment_distribution"]
    size          = results["size_distribution"]
    trend         = results["price_trend"]

    charts = {
        "affordability_all":   chart_affordability(affordability),
        "affordability_hcmc":  chart_affordability(affordability, city="HCMC"),
        "affordability_hn":    chart_affordability(affordability, city="HN"),
        "volatility":          chart_volatility(volatility),
        "segment":             chart_segment(segment),
        "size":                chart_size(size),
        "price_trend":         chart_price_trend(trend),
        "distribution_hcmc":   chart_price_distribution(df, city="HCMC"),
        "distribution_hn":     chart_price_distribution(df, city="HN"),
    }

    for name, fig in charts.items():
        path = os.path.join(output_dir, f"{name}.html")
        fig.write_html(path)
        print(f"Exported: {path}")

    return charts


# -------------------------------------------------------
# Quick test
# -------------------------------------------------------

if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    from analysis.eda import run_eda
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    results = run_eda()
    charts  = export_all(results, output_dir="data/charts")
    print(f"\nDone — {len(charts)} charts exported to data/charts/")
