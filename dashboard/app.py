"""Streamlit dashboard for PulseFlow weather data."""

from pathlib import Path

import duckdb
import streamlit as st

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "weather.duckdb"

st.set_page_config(page_title="PulseFlow", page_icon="\U0001F324️", layout="wide")


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=True)


def load_cities(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [row[0] for row in con.execute("SELECT DISTINCT city FROM weather_raw ORDER BY city").fetchall()]


def load_city_trend(con: duckdb.DuckDBPyConnection, city: str):
    return con.execute(
        """
        SELECT
            CAST("timestamp" AS TIMESTAMP) AS reading_time,
            temperature,
            humidity
        FROM weather_raw
        WHERE city = ?
        ORDER BY reading_time
        """,
        [city],
    ).df()


def load_last_updated(con: duckdb.DuckDBPyConnection):
    return con.execute("SELECT MAX(loaded_at) FROM weather_raw").fetchone()[0]


st.title("PulseFlow")
st.caption("Weather trends from the automated Open-Meteo ETL pipeline")

if not DB_PATH.exists():
    st.warning("No data yet — run the extract and transform pipeline first.")
    st.stop()

con = get_connection()
cities = load_cities(con)

if not cities:
    st.warning("weather_raw has no rows yet — run the extract and transform pipeline first.")
    st.stop()

last_updated = load_last_updated(con)
st.caption(f"Last updated: {last_updated}")

city = st.selectbox("City", cities, format_func=lambda c: c.replace("_", " ").title())

df = load_city_trend(con, city).set_index("reading_time")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Temperature (°C)")
    st.line_chart(df[["temperature"]])

with col2:
    st.subheader("Humidity (%)")
    st.line_chart(df[["humidity"]])
