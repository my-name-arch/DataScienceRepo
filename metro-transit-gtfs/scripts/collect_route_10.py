import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.transit import gtfs_realtime_pb2

PROJECT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_DIR / "metro_transit.duckdb"
OUTPUT_DIR = PROJECT_DIR / "data" / "clean" / "trip_updates_enriched"
FEED_URL = "https://svc.metrotransit.org/mtgtfs/tripupdates.pb"

ROUTE_ID = "10"
POLL_SECONDS = 15
WRITE_EVERY_SECONDS = 300

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "")
TRAFFIC_CACHE_TTL = 180
WEATHER_CACHE_TTL = 600

CORRIDOR_CHECKPOINTS = {
    "downtown_st_paul": (44.9537, -93.0900),
    "university_snelling": (44.9555, -93.1668),
    "central_northeast": (45.0112, -93.2471),
    "north_suburbs": (45.0683, -93.2477),
}

ENRICHED_SCHEMA = pa.schema([
    ("observed_at_utc", pa.string()),
    ("feed_timestamp_epoch", pa.int64()),
    ("route_id", pa.string()),
    ("route_short_name", pa.string()),
    ("trip_id", pa.string()),
    ("trip_headsign", pa.string()),
    ("direction_id", pa.string()),
    ("vehicle_id", pa.string()),
    ("stop_id", pa.string()),
    ("stop_sequence", pa.string()),
    ("stop_name", pa.string()),
    ("stop_lat", pa.float64()),
    ("stop_lon", pa.float64()),
    ("scheduled_arrival", pa.string()),
    ("scheduled_departure", pa.string()),
    ("arrival_delay_sec", pa.int64()),
    ("departure_delay_sec", pa.int64()),
    ("effective_delay_sec", pa.int64()),
    ("corridor_segment", pa.string()),
    ("traffic_current_speed_mph", pa.float64()),
    ("traffic_free_flow_speed_mph", pa.float64()),
    ("traffic_jam_factor", pa.float64()),
    ("traffic_road_closure", pa.bool_()),
    ("weather_temp_f", pa.float64()),
    ("weather_precip_in", pa.float64()),
    ("weather_snowfall_in", pa.float64()),
    ("weather_code", pa.int64()),
    ("weather_wind_mph", pa.float64()),
])

traffic_cache = {"last_fetch": 0, "data": {}}
weather_cache = {}


def get_static_lookups():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    schedule_rows = con.execute("""
        SELECT
            TRIM(CAST(t.trip_id AS VARCHAR)) AS trip_id,
            r.route_short_name,
            t.trip_headsign,
            t.direction_id,
            TRIM(CAST(st.stop_id AS VARCHAR)) AS stop_id,
            TRIM(CAST(st.stop_sequence AS VARCHAR)) AS stop_sequence,
            s.stop_name,
            TRY_CAST(s.stop_lat AS DOUBLE) AS stop_lat,
            TRY_CAST(s.stop_lon AS DOUBLE) AS stop_lon,
            st.arrival_time AS scheduled_arrival,
            st.departure_time AS scheduled_departure
        FROM trips AS t
        JOIN routes AS r ON t.route_id = r.route_id
        JOIN stop_times AS st ON t.trip_id = st.trip_id
        JOIN stops AS s ON st.stop_id = s.stop_id
        WHERE t.route_id = ?
    """, [ROUTE_ID]).fetchall()

    stop_rows = con.execute("""
        SELECT 
            TRIM(CAST(stop_id AS VARCHAR)) AS stop_id, 
            TRIM(stop_name) AS stop_name, 
            TRY_CAST(stop_lat AS DOUBLE) AS stop_lat, 
            TRY_CAST(stop_lon AS DOUBLE) AS stop_lon
        FROM stops
        WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL
    """).fetchall()

    con.close()

    schedule_lookup = {}
    for r in schedule_rows:
        key = (str(r[0]), str(r[4]), str(r[5]))
        schedule_lookup[key] = {
            "route_short_name": r[1],
            "trip_headsign": r[2],
            "direction_id": str(r[3]) if r[3] is not None else None,
            "stop_name": r[6],
            "stop_lat": float(r[7]) if r[7] is not None else None,
            "stop_lon": float(r[8]) if r[8] is not None else None,
            "scheduled_arrival": r[9],
            "scheduled_departure": r[10],
        }

    stops_dict = {
        str(s[0]): {
            "stop_name": s[1],
            "stop_lat": float(s[2]),
            "stop_lon": float(s[3]),
        }
        for s in stop_rows
    }

    return schedule_lookup, stops_dict


def find_nearest_corridor(lat, lon):
    if lat is None or lon is None:
        return "central_northeast"

    best_corridor = "central_northeast"
    min_dist = float("inf")
    for name, (c_lat, c_lon) in CORRIDOR_CHECKPOINTS.items():
        dist = math.hypot(lat - c_lat, lon - c_lon)
        if dist < min_dist:
            min_dist = dist
            best_corridor = name
    return best_corridor


def update_traffic_cache(session):
    global traffic_cache
    now = time.monotonic()

    if now - traffic_cache["last_fetch"] < TRAFFIC_CACHE_TTL and traffic_cache["data"]:
        return traffic_cache["data"]

    if not TOMTOM_API_KEY:
        return {}

    new_data = {}
    base_url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json"

    for segment_name, (lat, lon) in CORRIDOR_CHECKPOINTS.items():
        params = {
            "point": f"{lat},{lon}",
            "unit": "mph",
            "key": TOMTOM_API_KEY,
        }
        try:
            res = session.get(base_url, params=params, timeout=5)
            if res.status_code == 200:
                flow = res.json().get("flowSegmentData", {})
                current_speed = flow.get("currentSpeed")
                free_flow = flow.get("freeFlowSpeed", 1)

                jam_factor = 0.0
                if current_speed is not None and free_flow:
                    jam_factor = max(
                        0.0,
                        min(1.0, round(1.0 - (float(current_speed) / float(free_flow)), 2))
                    )

                new_data[segment_name] = {
                    "current_speed_mph": float(current_speed) if current_speed is not None else None,
                    "free_flow_speed_mph": float(free_flow) if free_flow is not None else None,
                    "jam_factor": float(jam_factor),
                    "road_closure": bool(flow.get("roadClosure", False)),
                }
        except requests.RequestException:
            continue

    if new_data:
        traffic_cache["last_fetch"] = now
        traffic_cache["data"] = new_data

    return traffic_cache["data"]


def get_current_weather(session, lat, lon):
    if lat is None or lon is None:
        return {}

    grid_key = (round(lat, 2), round(lon, 2))
    now = time.monotonic()

    if grid_key in weather_cache:
        cached_time, val = weather_cache[grid_key]
        if now - cached_time < WEATHER_CACHE_TTL:
            return val

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": grid_key[0],
        "longitude": grid_key[1],
        "current": "temperature_2m,precipitation,snowfall,weather_code,wind_speed_10m",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
    }

    try:
        res = session.get(url, params=params, timeout=5)
        if res.status_code == 200:
            current = res.json().get("current", {})
            w_data = {
                "weather_temp_f": float(current["temperature_2m"]) if current.get("temperature_2m") is not None else None,
                "weather_precip_in": float(current["precipitation"]) if current.get("precipitation") is not None else None,
                "weather_snowfall_in": float(current["snowfall"]) if current.get("snowfall") is not None else None,
                "weather_code": int(current["weather_code"]) if current.get("weather_code") is not None else None,
                "weather_wind_mph": float(current["wind_speed_10m"]) if current.get("wind_speed_10m") is not None else None,
            }
            weather_cache[grid_key] = (now, w_data)
            return w_data
    except requests.RequestException:
        pass

    return {}


def parse_route_10_updates(feed, observed_at_utc, schedule_lookup, stops_dict, session):
    rows = []
    traffic_data = update_traffic_cache(session)

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        update = entity.trip_update
        if update.trip.route_id != ROUTE_ID:
            continue

        trip_id = str(update.trip.trip_id).strip()
        vehicle_id = update.vehicle.id if update.HasField("vehicle") else None

        for stop_update in update.stop_time_update:
            stop_id = str(stop_update.stop_id).strip()
            stop_sequence = str(stop_update.stop_sequence).strip()

            arrival_delay_sec = (
                stop_update.arrival.delay
                if stop_update.HasField("arrival")
                else None
            )
            departure_delay_sec = (
                stop_update.departure.delay
                if stop_update.HasField("departure")
                else None
            )

            effective_delay_sec = (
                arrival_delay_sec
                if arrival_delay_sec is not None
                else departure_delay_sec
            )

            key = (trip_id, stop_id, stop_sequence)
            static = schedule_lookup.get(key)
            if not static:
                static = stops_dict.get(stop_id, {})

            lat = static.get("stop_lat")
            lon = static.get("stop_lon")

            corridor = find_nearest_corridor(lat, lon)
            seg_traffic = traffic_data.get(corridor, {})

            w_lat = lat if lat is not None else CORRIDOR_CHECKPOINTS[corridor][0]
            w_lon = lon if lon is not None else CORRIDOR_CHECKPOINTS[corridor][1]
            weather = get_current_weather(session, w_lat, w_lon)

            rows.append({
                "observed_at_utc": observed_at_utc.isoformat(),
                "feed_timestamp_epoch": int(feed.header.timestamp),
                "route_id": str(ROUTE_ID),
                "route_short_name": static.get("route_short_name", "10"),
                "trip_id": trip_id,
                "trip_headsign": static.get("trip_headsign"),
                "direction_id": static.get("direction_id"),
                "vehicle_id": str(vehicle_id) if vehicle_id else None,
                "stop_id": stop_id,
                "stop_sequence": stop_sequence,
                "stop_name": static.get("stop_name"),
                "stop_lat": lat,
                "stop_lon": lon,
                "scheduled_arrival": static.get("scheduled_arrival"),
                "scheduled_departure": static.get("scheduled_departure"),
                "arrival_delay_sec": arrival_delay_sec,
                "departure_delay_sec": departure_delay_sec,
                "effective_delay_sec": effective_delay_sec,
                "corridor_segment": corridor,
                "traffic_current_speed_mph": seg_traffic.get("current_speed_mph"),
                "traffic_free_flow_speed_mph": seg_traffic.get("free_flow_speed_mph"),
                "traffic_jam_factor": seg_traffic.get("jam_factor"),
                "traffic_road_closure": seg_traffic.get("road_closure"),
                "weather_temp_f": weather.get("weather_temp_f"),
                "weather_precip_in": weather.get("weather_precip_in"),
                "weather_snowfall_in": weather.get("weather_snowfall_in"),
                "weather_code": weather.get("weather_code"),
                "weather_wind_mph": weather.get("weather_wind_mph"),
            })

    return rows


def write_parquet(rows, write_time_utc):
    if not rows:
        print("No Route 10 records to write in this batch.")
        return

    service_date = write_time_utc.strftime("%Y-%m-%d")
    hour = write_time_utc.strftime("%H")

    partition_dir = (
        OUTPUT_DIR
        / f"route_id={ROUTE_ID}"
        / f"service_date={service_date}"
        / f"hour={hour}"
    )
    partition_dir.mkdir(parents=True, exist_ok=True)

    file_name = write_time_utc.strftime("trip_updates_enriched_%Y%m%d_%H%M%S.parquet")
    output_path = partition_dir / file_name

    table = pa.Table.from_pylist(rows, schema=ENRICHED_SCHEMA)
    pq.write_table(table, output_path, compression="zstd")

    print(
        f"Wrote {len(rows):,} enriched records to "
        f"{output_path.relative_to(PROJECT_DIR)}"
    )


def main():
    print("Loading static Route 10 schedule & stop lookups from DuckDB...")
    schedule_lookup, stops_dict = get_static_lookups()
    print(f"Loaded {len(schedule_lookup):,} schedule keys and {len(stops_dict):,} physical stops.")

    if not TOMTOM_API_KEY:
        print("[WARNING] TOMTOM_API_KEY environment variable is not set. Traffic data will be empty.")
    else:
        print("TomTom API integration active.")

    print(f"Polling GTFS-RT every {POLL_SECONDS}s. Writing Parquet every {WRITE_EVERY_SECONDS // 60}m.")
    print("Press Control + C to stop safely.\n")

    session = requests.Session()
    batch_rows = []
    last_write_time = time.monotonic()

    try:
        while True:
            observed_at_utc = datetime.now(timezone.utc)

            try:
                response = session.get(FEED_URL, timeout=30)
                response.raise_for_status()

                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(response.content)

                snapshot_rows = parse_route_10_updates(
                    feed=feed,
                    observed_at_utc=observed_at_utc,
                    schedule_lookup=schedule_lookup,
                    stops_dict=stops_dict,
                    session=session,
                )

                batch_rows.extend(snapshot_rows)

                print(
                    f"{observed_at_utc.isoformat()} | "
                    f"Captured {len(snapshot_rows):,} Route 10 stop records | "
                    f"Queue: {len(batch_rows):,}"
                )

            except requests.RequestException as error:
                print(f"{observed_at_utc.isoformat()} | Request error: {error}")

            if time.monotonic() - last_write_time >= WRITE_EVERY_SECONDS:
                write_parquet(batch_rows, datetime.now(timezone.utc))
                batch_rows = []
                last_write_time = time.monotonic()

            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print("\nStopping collector...")
        if batch_rows:
            write_parquet(batch_rows, datetime.now(timezone.utc))
        print("Collector stopped safely.")


if __name__ == "__main__":
    main()
