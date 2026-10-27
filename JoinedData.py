import json
from urllib import parse, request

import pandas as pd

try:
    import requests
except ImportError:  # pragma: no cover - optional dependency fallback
    requests = None


def _fetch_json(url: str, params=None, timeout=10):
    """Fetch JSON using requests when available, otherwise fall back to urllib."""
    if requests is not None:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()

    query = ""
    if params:
        query = "?" + parse.urlencode(params)

    try:
        with request.urlopen(f"{url}{query}", timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as exc:  # pragma: no cover - diagnostic fallback
        print(f"Error fetching {url}: {exc}")
    return {}


def get_live_route_10_buses():
    """Fetches real-time vehicle positions and delays for Metro Transit Route 10."""
    url = "https://metrotransit.org"
    try:
        data = _fetch_json(url, timeout=10)
        if not isinstance(data, list):
            return pd.DataFrame()

        bus_list = []
        for bus in data:
            bus_list.append({
                "vehicle_id": bus.get("vehicle_id"),
                "latitude": bus.get("latitude"),
                "longitude": bus.get("longitude"),
                "delay_seconds": bus.get("delay"),  # Positive = late, Negative = early
                "speed_mph": bus.get("speed"),
                "trip_id": bus.get("trip_id"),
            })
        return pd.DataFrame(bus_list)
    except Exception as e:
        print(f"Error fetching Metro Transit data: {e}")
    return pd.DataFrame()  # Return empty DataFrame on failure


def get_mndot_active_incidents():
    """Fetches live incidents (crashes, stalls, hazards) from MnDOT 511 API."""
    # Official MnDOT 511 Live Incident Feature Layer Server
    url = "https://arcgis.com"
    params = {
        "where": "1=1",  # Get all active records
        "outFields": "ID,EventClass,EventHeadline",
        "f": "json",
        "returnGeometry": "true",
    }
    try:
        data = _fetch_json(url, params=params, timeout=10)
        incident_list = []
        for feature in data.get("features", []):
            attrs = feature.get("attributes", {})
            geom = feature.get("geometry", {})
            # Guard clauses to skip records missing coordinates
            if not geom or "y" not in geom or "x" not in geom:
                continue
            incident_list.append({
                "incident_id": attrs.get("ID"),
                "cause": attrs.get("EventClass"),  # e.g., Accident, Weather
                "headline": attrs.get("EventHeadline"),  # e.g., "Crash on Central Ave"
                "latitude": geom.get("y"),
                "longitude": geom.get("x"),
            })
        return pd.DataFrame(incident_list)
    except Exception as e:
        print(f"Error fetching MnDOT Incident data: {e}")
    return pd.DataFrame()


def get_mndot_realtime_speeds():
    """Fetches live highway sensor speeds and congestion metrics around the twin cities."""
    # Official MnDOT Traffic Management Center Speed Sensor Layer
    url = "https://arcgis.com"
    params = {
        "where": "1=1",
        "outFields": "StationID,CurrentSpeed,CongestionLevel",
        "f": "json",
        "returnGeometry": "true",
    }
    try:
        data = _fetch_json(url, params=params, timeout=10)
        sensor_list = []
        for feature in data.get("features", []):
            attrs = feature.get("attributes", {})
            geom = feature.get("geometry", {})
            if not geom or "y" not in geom or "x" not in geom:
                continue
            sensor_list.append({
                "station_id": attrs.get("StationID"),
                "speed_mph": attrs.get("CurrentSpeed"),
                "congestion_level": attrs.get("CongestionLevel"),
                "latitude": geom.get("y"),
                "longitude": geom.get("x"),
            })
        return pd.DataFrame(sensor_list)
    except Exception as e:
        print(f"Error fetching MnDOT Speed data: {e}")
    return pd.DataFrame()


# =============================================================
# WORKFLOW EXECUTION EXAMPLE
# =============================================================
if __name__ == "__main__":
    print("Pulling live data streams...")

    bus_df = get_live_route_10_buses()
    incident_df = get_mndot_active_incidents()
    traffic_df = get_mndot_realtime_speeds()

    print(f"✓ Found {len(bus_df)} active Route 10 buses.")
    print(f"✓ Found {len(incident_df)} regional incidents.")
    print(f"✓ Found {len(traffic_df)} speed sensors.")

    # Now these three variables are formatted cleanly and ready to feed right into:
    # final_analysis_gdf = analyze_route_10_delays(bus_df, incident_df, traffic_df)
