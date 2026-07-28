"""Pull current weather + 24hr hourly forecast from Open-Meteo for fixed cities."""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://api.open-meteo.com/v1/forecast"
REQUEST_TIMEOUT = 10
RETRY_DELAY_SECONDS = 2

RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

CITIES = {
    "chennai": (13.0827, 80.2707),
    "mumbai": (19.0760, 72.8777),
    "delhi": (28.6139, 77.2090),
    "bangalore": (12.9716, 77.5946),
    "london": (51.5074, -0.1278),
    "new_york": (40.7128, -74.0060),
    "tokyo": (35.6762, 139.6503),
    "singapore": (1.3521, 103.8198),
    "dubai": (25.2048, 55.2708),
    "sydney": (-33.8688, 151.2093),
}

CURRENT_FIELDS = "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m"
HOURLY_FIELDS = "temperature_2m,relative_humidity_2m,precipitation_probability,precipitation,weather_code,wind_speed_10m"


def fetch_weather(city: str, lat: float, lon: float) -> dict | None:
    """Fetch current + 24hr hourly forecast for a city, retrying once on failure."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": CURRENT_FIELDS,
        "hourly": HOURLY_FIELDS,
        "forecast_days": 1,
        "timezone": "auto",
    }

    for attempt in (1, 2):
        try:
            response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            if attempt == 1:
                logger.warning("Request failed for %s (attempt %d): %s. Retrying...", city, attempt, exc)
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logger.error("Request failed for %s on retry: %s. Skipping.", city, exc)
                return None


def save_raw(city: str, data: dict) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = RAW_DATA_DIR / f"{city}_{timestamp}.json"
    out_path.write_text(json.dumps(data, indent=2))
    return out_path


def main() -> None:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    for city, (lat, lon) in CITIES.items():
        data = fetch_weather(city, lat, lon)
        if data is None:
            continue
        out_path = save_raw(city, data)
        logger.info("Saved %s", out_path)


if __name__ == "__main__":
    main()
