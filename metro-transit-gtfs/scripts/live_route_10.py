from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests
from google.transit import gtfs_realtime_pb2

project_dir = Path(__file__).resolve().parents[1]
db_path = project_dir / "metro_transit.duckdb"

feed_url = "https://svc.metrotransit.org/mtgtfs/tripupdates.pb"
route_id = "10"

response = requests.get(feed_url, timeout=30)
response.raise_for_status()

feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(response.content)

retrieved_at = datetime.now(timezone.utc).isoformat()

live_updates = []

for entity in feed.entity:
    if not entity.HasField("trip_update"):
        continue

    update = entity.trip_update

    if update.trip.route_id != route_id:
        continue

    vehicle_id = update.vehicle.id if update.HasField("vehicle") else None

    for stop_update in update.stop_time_update:
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

        if arrival_delay_sec is not None:
            effective_delay_sec = arrival_delay_sec
        else:
            effective_delay_sec = departure_delay_sec

        live_updates.append(
            {
                "retrieved_at_utc": retrieved_at,
                "trip_id": update.trip.trip_id,
                "route_id": update.trip.route_id,
                "vehicle_id": vehicle_id,
                "stop_id": stop_update.stop_id,
                "stop_sequence": str(stop_update.stop_sequence),
                "arrival_delay_sec": arrival_delay_sec,
                "departure_delay_sec": departure_delay_sec,
                "effective_delay_sec": effective_delay_sec,
            }
        )

if not live_updates:
    print(f"No live updates found for Route {route_id}.")
else:
    con = duckdb.connect(str(db_path), read_only=True)

    con.execute("""
        CREATE TEMP TABLE live_updates (
            retrieved_at_utc VARCHAR,
            trip_id VARCHAR,
            route_id VARCHAR,
            vehicle_id VARCHAR,
            stop_id VARCHAR,
            stop_sequence VARCHAR,
            arrival_delay_sec BIGINT,
            departure_delay_sec BIGINT,
            effective_delay_sec BIGINT
        )
    """)

    con.executemany("""
        INSERT INTO live_updates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (
            row["retrieved_at_utc"],
            row["trip_id"],
            row["route_id"],
            row["vehicle_id"],
            row["stop_id"],
            row["stop_sequence"],
            row["arrival_delay_sec"],
            row["departure_delay_sec"],
            row["effective_delay_sec"],
        )
        for row in live_updates
    ])

    joined_updates = con.execute("""
        SELECT
            live.retrieved_at_utc,
            r.route_short_name,
            t.trip_headsign,
            live.trip_id,
            live.vehicle_id,
            live.stop_sequence,
            s.stop_name,
            st.arrival_time AS scheduled_arrival,
            st.departure_time AS scheduled_departure,
            live.arrival_delay_sec,
            live.departure_delay_sec,
            live.effective_delay_sec,
            ROUND(live.effective_delay_sec / 60.0, 1) AS effective_delay_minutes
        FROM live_updates AS live
        LEFT JOIN trips AS t
            ON live.trip_id = t.trip_id
        LEFT JOIN routes AS r
            ON t.route_id = r.route_id
        LEFT JOIN stop_times AS st
            ON live.trip_id = st.trip_id
            AND live.stop_id = st.stop_id
            AND live.stop_sequence = st.stop_sequence
        LEFT JOIN stops AS s
            ON live.stop_id = s.stop_id
        ORDER BY
            live.trip_id,
            CAST(live.stop_sequence AS INTEGER)
    """).fetchall()

    print(f"\nRetrieved at UTC: {retrieved_at}")
    print(f"Route {route_id}: live stop-level updates joined to static GTFS\n")

    for row in joined_updates:
        (
            retrieved_at_utc,
            route_short_name,
            trip_headsign,
            trip_id,
            vehicle_id,
            stop_sequence,
            stop_name,
            scheduled_arrival,
            scheduled_departure,
            arrival_delay_sec,
            departure_delay_sec,
            effective_delay_sec,
            effective_delay_minutes,
        ) = row

        print(
            f"Route {route_short_name} | "
            f"trip {trip_id} | "
            f"vehicle {vehicle_id} | "
            f"stop {stop_sequence}: {stop_name}\n"
            f"  Scheduled arrival: {scheduled_arrival} | "
            f"Scheduled departure: {scheduled_departure}\n"
            f"  Arrival delay: {arrival_delay_sec} sec | "
            f"Departure delay: {departure_delay_sec} sec | "
            f"Selected delay: {effective_delay_sec} sec "
            f"({effective_delay_minutes} min)\n"
        )

    con.close()