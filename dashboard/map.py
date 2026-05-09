"""
dashboard/map.py — Choropleth map for apartment price by district
Uses: plotly + GeoJSON from data/geojson/
"""

import json
import os
import logging
import pandas as pd
import plotly.graph_objects as go

logger = logging.getLogger(__name__)

GEOJSON_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "geojson")

CITY_CONFIG = {
    "HCMC": {
        "geojson":  os.path.join(GEOJSON_DIR, "hcmc.geojson"),
        "center":   {"lat": 10.776, "lon": 106.700},
        "zoom":     9.5,
    },
    "HN": {
        "geojson":  os.path.join(GEOJSON_DIR, "hn.geojson"),
        "center":   {"lat": 21.028, "lon": 105.834},
        "zoom":     9.0,
    },
}


def load_geojson(city: str) -> dict:
    path = CITY_CONFIG[city]["geojson"]
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_district_stats(df: pd.DataFrame, city: str) -> pd.DataFrame:
    """
    Aggregate apartment data by district for one city.
    Returns DataFrame with: district, median_price_m2, avg_price_m2, listings
    """
    data = df[
        (df["city"] == city) &
        df["price_per_m2_m"].notna() &
        df["district"].notna()
    ]

    stats = (
        data.groupby("district")
        .agg(
            median_price_m2 = ("price_per_m2_m", "median"),
            avg_price_m2    = ("price_per_m2_m", "mean"),
            listings        = ("id", "count"),
        )
        .round(1)
        .reset_index()
    )
    return stats


def choropleth(df: pd.DataFrame, city: str, metric: str = "median_price_m2") -> go.Figure:
    """
    Choropleth map of apartment prices by district.

    Args:
        df:     full cleaned DataFrame from feature_engineering()
        city:   'HCMC' | 'HN'
        metric: 'median_price_m2' | 'avg_price_m2' | 'listings'
    """
    geojson = load_geojson(city)
    stats   = build_district_stats(df, city)
    config  = CITY_CONFIG[city]

    metric_labels = {
        "median_price_m2": "Median Price/m² (triệu)",
        "avg_price_m2":    "Avg Price/m² (triệu)",
        "listings":        "# Listings",
    }
    label = metric_labels.get(metric, metric)

    # custom hover text
    stats["hover"] = stats.apply(
        lambda r: (
            f"<b>{r['district']}</b><br>"
            f"Median: {r['median_price_m2']:.1f} tr/m²<br>"
            f"Avg: {r['avg_price_m2']:.1f} tr/m²<br>"
            f"Listings: {r['listings']}"
        ),
        axis=1,
    )

    fig = go.Figure(
        go.Choroplethmapbox(
            geojson=geojson,
            locations=stats["district"],
            featureidkey="properties.name",
            z=stats[metric],
            text=stats["hover"],
            hoverinfo="text",
            colorscale="RdYlGn_r",   # red = expensive, green = affordable
            colorbar=dict(
                title=dict(text=label, side="right"),
                thickness=15,
                len=0.6,
            ),
            marker_opacity=0.75,
            marker_line_width=1,
            marker_line_color="white",
        )
    )

    fig.update_layout(
        title=dict(
            text=f"{label} by District — {city}",
            x=0.5,
        ),
        mapbox=dict(
            style="carto-positron",
            center=config["center"],
            zoom=config["zoom"],
        ),
        height=600,
        margin={"r": 0, "t": 50, "l": 0, "b": 0},
    )

    return fig


def choropleth_both(df: pd.DataFrame, metric: str = "median_price_m2"):
    """Return dict of figures for both cities."""
    return {
        "HCMC": choropleth(df, "HCMC", metric),
        "HN":   choropleth(df, "HN",   metric),
    }


def export_maps(df: pd.DataFrame, output_dir: str = "data/charts"):
    """Export choropleth maps as HTML files."""
    os.makedirs(output_dir, exist_ok=True)

    metrics = ["median_price_m2", "avg_price_m2", "listings"]
    cities  = ["HCMC", "HN"]

    for city in cities:
        for metric in metrics:
            fig  = choropleth(df, city, metric)
            path = os.path.join(output_dir, f"map_{city.lower()}_{metric}.html")
            fig.write_html(path)
            print(f"Exported: {path}")


# -------------------------------------------------------
# Quick test
# -------------------------------------------------------
if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    from analysis.eda import run_eda
    results = run_eda()
    df      = results["df"]

    export_maps(df, output_dir="data/charts")
    print("\nDone — open data/charts/map_hcmc_median_price_m2.html to preview")
