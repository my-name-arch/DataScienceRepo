from datetime import datetime, timezone

import requests
from google.transit import gtfs_realtime_pb2

FEED_URL = "https://svc.metrotransit.org/mtgtfs/tripupdates.pb"
ROUTE_ID = "10"

response = requests.get(FEED_URL, timeout=30)
response.raise_for_status()

feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(response.content)

retrieved_at = datetime.now(timezone.utc).isoformat()

print(f"\nRetrieved at UTC: {retrieved_at}")
print(f"HTTP status: {response.status_code}")
print(f"Feed timestamp: {feed.header.timestamp}")
print(f"Total feed entities: {len(feed.entity)}")

route_10_updates = []

for entity in feed.entity:
    if not entity.HasField("trip_update"):
        continue

    trip_update = entity.trip_update

    if trip_update.trip.route_id != ROUTE_ID:
        continue

    route_10_updates.append(trip_update)

print(f"Route {ROUTE_ID} trip updates: {len(route_10_updates)}\n")

for update in route_10_updates[:5]:
    trip_id = update.trip.trip_id
    route_id = update.trip.route_id
    vehicle_id = update.vehicle.id if update.HasField("vehicle") else None

    print(
        f"trip_id={trip_id} | "
        f"route_id={route_id} | "
        f"vehicle_id={vehicle_id}"
    )

    for stop_update in update.stop_time_update[:3]:
        stop_id = stop_update.stop_id
        stop_sequence = stop_update.stop_sequence

        arrival_delay = (
            stop_update.arrival.delay
            if stop_update.HasField("arrival")
            else None
        )

        departure_delay = (
            stop_update.departure.delay
            if stop_update.HasField("departure")
            else None
        )

        print(
            f"  stop_id={stop_id} | "
            f"stop_sequence={stop_sequence} | "
            f"arrival_delay_sec={arrival_delay} | "
            f"departure_delay_sec={departure_delay}"
        )

    print()