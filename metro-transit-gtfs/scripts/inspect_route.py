from pathlib import Path
import duckdb

project_dir = Path(__file__).resolve().parents[1]
db_path = project_dir / "metro_transit.duckdb"

con = duckdb.connect(str(db_path), read_only=True)

route_short_name = "10"

print(f"\nRoute {route_short_name}: trip summary\n")

trip_summary = con.execute("""
    SELECT
        r.route_id,
        r.route_short_name,
        t.direction_id,
        t.trip_headsign,
        COUNT(*) AS trip_count
    FROM routes AS r
    JOIN trips AS t
        ON r.route_id = t.route_id
    WHERE r.route_short_name = ?
    GROUP BY
        r.route_id,
        r.route_short_name,
        t.direction_id,
        t.trip_headsign
    ORDER BY trip_count DESC
""", [route_short_name]).fetchall()

for row in trip_summary:
    print(row)

print(f"\nOne Route {route_short_name} daytime trip\n")

sample_trip = con.execute("""
    SELECT
        t.trip_id,
        t.trip_headsign,
        t.direction_id,
        MIN(st.departure_time) AS first_departure_time
    FROM trips AS t
    JOIN routes AS r
        ON t.route_id = r.route_id
    JOIN stop_times AS st
        ON t.trip_id = st.trip_id
    WHERE r.route_short_name = ?
      AND st.stop_sequence = '1'
      AND st.departure_time BETWEEN '08:00:00' AND '08:59:59'
    GROUP BY
        t.trip_id,
        t.trip_headsign,
        t.direction_id
    ORDER BY first_departure_time
    LIMIT 1
""", [route_short_name]).fetchone()

if sample_trip is None:
    print("No Route 10 trip was found departing between 08:00 and 08:59.")
else:
    trip_id, trip_headsign, direction_id, first_departure_time = sample_trip

    print(f"Trip ID: {trip_id}")
    print(f"Headsign: {trip_headsign}")
    print(f"Direction ID: {direction_id}")
    print(f"First scheduled departure: {first_departure_time}\n")

    stop_sequence = con.execute("""
        SELECT
            st.stop_sequence,
            s.stop_id,
            s.stop_name,
            st.arrival_time,
            st.departure_time
        FROM stop_times AS st
        JOIN stops AS s
            ON st.stop_id = s.stop_id
        WHERE st.trip_id = ?
        ORDER BY CAST(st.stop_sequence AS INTEGER)
    """, [trip_id]).fetchall()

    for row in stop_sequence:
        print(row)

con.close()