from datetime import datetime, timezone
from pathlib import Path
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.transit import gtfs_realtime_pb2

PROJECT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_DIR / "metro_transit.duckdb"
OUTPUT_DIR = PROJECT_DIR / "data" / "clean" / "trip_updates"
FEED_URL = "https://svc.metrotransit.org/mtgtfs/tripupdates.pb"

ROUTE_ID = "10"
POLL_SECONDS = 15
WRITE_EVERY_SECONDS = 300

OUTPUT_COLUMNS = [
    "observed_at_utc",
    "feed_timestamp_epoch",
    "route_id",
    "route_short_name",
    "trip_id",
    "trip_headsign",
    "direction_id",
    "vehicle_id",
    "stop_id",
    "stop_sequence",
    "stop_name",
    "stop_lat",
    "stop_lon",
    "scheduled_arrival",
    "scheduled_departure",
    "arrival_delay_sec",
    "departure_delay_sec",
    "effective_delay_sec",
]


def get_static_lookup():
    con = duckdb.connect(str(DB_PATH), read_only=True)

    rows = con.execute("""
        SELECT
            t.trip_id,
            t.route_id,
            r.route_short_name,
            t.trip_headsign,
            t.direction_id,
            st.stop_id,
            st.stop_sequence,
            s.stop_name,
            s.stop_lat,
            s.stop_lon,
            st.arrival_time AS scheduled_arrival,
            st.departure_time AS scheduled_departure
        FROM trips AS t
        JOIN routes AS r
            ON t.route_id = r.route_id
        JOIN stop_times AS st
            ON t.trip_id = st.trip_id
        JOIN stops AS s
            ON st.stop_id = s.stop_id
        WHERE t.route_id = ?
    """, [ROUTE_ID]).fetchall()

    con.close()

    lookup = {}

    for row in rows:
        (
            trip_id,
            route_id,
            route_short_name,
            trip_headsign,
            direction_id,
            stop_id,
            stop_sequence,
            stop_name,
            stop_lat,
            stop_lon,
            scheduled_arrival,
            scheduled_departure,
        ) = row

        key = (str(trip_id), str(stop_id), str(stop_sequence))

        lookup[key] = {
            "route_short_name": route_short_name,
            "trip_headsign": trip_headsign,
            "direction_id": direction_id,
            "stop_name": stop_name,
            "stop_lat": float(stop_lat) if stop_lat is not None else None,
            "stop_lon": float(stop_lon) if stop_lon is not None else None,
            "scheduled_arrival": scheduled_arrival,
            "scheduled_departure": scheduled_departure,
        }

    return lookup


def parse_route_10_updates(feed, observed_at_utc, static_lookup):
    rows = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        update = entity.trip_update

        if update.trip.route_id != ROUTE_ID:
            continue

        trip_id = update.trip.trip_id
        vehicle_id = update.vehicle.id if update.HasField("vehicle") else None

        for stop_update in update.stop_time_update:
            stop_id = stop_update.stop_id
            stop_sequence = str(stop_update.stop_sequence)

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

            key = (str(trip_id), str(stop_id), stop_sequence)
            static = static_lookup.get(key, {})

            rows.append({
                "observed_at_utc": observed_at_utc.isoformat(),
                "feed_timestamp_epoch": feed.header.timestamp,
                "route_id": ROUTE_ID,
                "route_short_name": static.get("route_short_name"),
                "trip_id": str(trip_id),
                "trip_headsign": static.get("trip_headsign"),
                "direction_id": static.get("direction_id"),
                "vehicle_id": vehicle_id,
                "stop_id": str(stop_id),
                "stop_sequence": stop_sequence,
                "stop_name": static.get("stop_name"),
                "stop_lat": static.get("stop_lat"),
                "stop_lon": static.get("stop_lon"),
                "scheduled_arrival": static.get("scheduled_arrival"),
                "scheduled_departure": static.get("scheduled_departure"),
                "arrival_delay_sec": arrival_delay_sec,
                "departure_delay_sec": departure_delay_sec,
                "effective_delay_sec": effective_delay_sec,
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

    file_name = write_time_utc.strftime("trip_updates_%Y%m%d_%H%M%S.parquet")
    output_path = partition_dir / file_name

    table = pa.Table.from_pylist(rows)

    pq.write_table(
        table,
        output_path,
        compression="zstd"
    )

    print(
        f"Wrote {len(rows):,} records to "
        f"{output_path.relative_to(PROJECT_DIR)}"
    )


def main():
    print("Loading static Route 10 lookup from DuckDB...")
    static_lookup = get_static_lookup()

    print(f"Loaded {len(static_lookup):,} Route 10 trip-stop schedule records.")
    print(f"Polling every {POLL_SECONDS} seconds.")
    print(f"Writing a Parquet batch every {WRITE_EVERY_SECONDS // 60} minutes.")
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
                    static_lookup=static_lookup,
                )

                batch_rows.extend(snapshot_rows)

                print(
                    f"{observed_at_utc.isoformat()} | "
                    f"captured {len(snapshot_rows):,} Route 10 stop updates | "
                    f"batch total: {len(batch_rows):,}"
                )

            except requests.RequestException as error:
                print(f"{observed_at_utc.isoformat()} | request error: {error}")

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