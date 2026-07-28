"""Read the latest raw weather JSON per city, validate, flatten, and upsert into DuckDB."""

import json
import logging
import re
import sys
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "weather.duckdb"

RAW_FILENAME_RE = re.compile(r"^(?P<city>.+)_(?P<timestamp>\d{8}T\d{6}Z)\.json$")

HOURLY_FIELDS = ("temperature_2m", "relative_humidity_2m", "wind_speed_10m", "weather_code")
REQUIRED_FIELDS = ("time", "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "weather_code")


def find_latest_raw_files() -> dict[str, Path]:
    """Return the most recent raw JSON file path per city (filenames sort lexically by timestamp)."""
    latest: dict[str, Path] = {}
    for path in RAW_DATA_DIR.glob("*.json"):
        match = RAW_FILENAME_RE.match(path.name)
        if not match:
            continue
        city = match.group("city")
        if city not in latest or path.name > latest[city].name:
            latest[city] = path
    return latest


def validate_record(city: str, record: dict, context: str) -> bool:
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None:
            logger.warning("Validation failed for %s (%s): missing/null field '%s'", city, context, field)
            return False
    return True


def flatten_city_data(city: str, data: dict) -> tuple[list[dict], int]:
    """Flatten current + hourly blocks into rows; return (rows, failure_count)."""
    rows = []
    failures = 0

    current = data.get("current")
    if current is None:
        logger.warning("Validation failed for %s: missing 'current' block", city)
        failures += 1
    elif validate_record(city, current, "current"):
        rows.append({
            "city": city,
            "timestamp": current["time"],
            "record_type": "current",
            "temperature": current["temperature_2m"],
            "humidity": current["relative_humidity_2m"],
            "wind_speed": current["wind_speed_10m"],
            "conditions": current["weather_code"],
        })
    else:
        failures += 1

    hourly = data.get("hourly")
    if hourly is None or "time" not in hourly:
        logger.warning("Validation failed for %s: missing 'hourly' block", city)
        failures += 1
    else:
        times = hourly["time"]
        for i, ts in enumerate(times):
            entry = {"time": ts}
            for field in HOURLY_FIELDS:
                values = hourly.get(field)
                entry[field] = values[i] if values and i < len(values) else None

            if validate_record(city, entry, f"hourly[{i}]"):
                rows.append({
                    "city": city,
                    "timestamp": entry["time"],
                    "record_type": "hourly",
                    "temperature": entry["temperature_2m"],
                    "humidity": entry["relative_humidity_2m"],
                    "wind_speed": entry["wind_speed_10m"],
                    "conditions": entry["weather_code"],
                })
            else:
                failures += 1

    return rows, failures


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS weather_raw (
            city VARCHAR,
            timestamp VARCHAR,
            record_type VARCHAR,
            temperature DOUBLE,
            humidity DOUBLE,
            wind_speed DOUBLE,
            conditions INTEGER,
            loaded_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (city, timestamp, record_type)
        )
    """)


def upsert_rows(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    con.executemany("""
        INSERT INTO weather_raw (city, timestamp, record_type, temperature, humidity, wind_speed, conditions)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (city, timestamp, record_type) DO UPDATE SET
            temperature = excluded.temperature,
            humidity = excluded.humidity,
            wind_speed = excluded.wind_speed,
            conditions = excluded.conditions,
            loaded_at = now()
    """, [
        (r["city"], r["timestamp"], r["record_type"], r["temperature"], r["humidity"], r["wind_speed"], r["conditions"])
        for r in rows
    ])


def main() -> None:
    latest_files = find_latest_raw_files()
    if not latest_files:
        logger.warning("No raw JSON files found in %s", RAW_DATA_DIR)
        return

    con = duckdb.connect(str(DB_PATH))
    ensure_table(con)

    total_rows = 0
    total_failures = 0

    for city, path in sorted(latest_files.items()):
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            logger.error("Failed to parse %s: %s. Skipping.", path, exc)
            continue

        rows, failures = flatten_city_data(city, data)
        total_failures += failures

        if rows:
            upsert_rows(con, rows)

        logger.info("%s: upserted %d row(s) from %s (%d validation failure(s))", city, len(rows), path.name, failures)
        total_rows += len(rows)

    logger.info("Done. Total rows upserted: %d. Total validation failures: %d.", total_rows, total_failures)
    con.close()


if __name__ == "__main__":
    main()
