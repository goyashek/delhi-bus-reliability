import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2


COLUMNS = [
    "collection_timestamp",
    "entity_id",
    "vehicle_id",
    "trip_id",
    "route_id",
    "latitude",
    "longitude",
    "speed",
    "bearing",
    "current_stop_sequence",
    "current_status",
    "feed_timestamp",
]


def parse_feed(content, collected_at):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)
    rows = []

    for entity in feed.entity:
        if not entity.HasField("vehicle") or not entity.vehicle.HasField("position"):
            continue
        vehicle = entity.vehicle
        rows.append(
            {
                "collection_timestamp": collected_at,
                "entity_id": entity.id,
                "vehicle_id": vehicle.vehicle.id,
                "trip_id": vehicle.trip.trip_id,
                "route_id": vehicle.trip.route_id,
                "latitude": vehicle.position.latitude,
                "longitude": vehicle.position.longitude,
                "speed": vehicle.position.speed if vehicle.position.HasField("speed") else "",
                "bearing": vehicle.position.bearing if vehicle.position.HasField("bearing") else "",
                "current_stop_sequence": (
                    vehicle.current_stop_sequence
                    if vehicle.HasField("current_stop_sequence")
                    else ""
                ),
                "current_status": (
                    gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(
                        vehicle.current_status
                    )
                    if vehicle.HasField("current_status")
                    else ""
                ),
                "feed_timestamp": feed.header.timestamp,
            }
        )
    return rows


def fetch_rows():
    load_dotenv()
    api_key = os.getenv("OTD_API_KEY", "").strip()
    base_url = os.getenv("OTD_BASE_URL", "https://otd.delhi.gov.in").rstrip("/")
    if not api_key:
        raise RuntimeError("Add OTD_API_KEY to .env before collecting data")

    try:
        response = requests.get(
            f"{base_url}/api/realtime/VehiclePositions.pb",
            params={"key": api_key},
            timeout=30,
        )
    except requests.RequestException:
        raise RuntimeError("Could not reach the OTD real-time feed") from None
    if response.status_code != 200:
        raise RuntimeError(f"OTD returned HTTP {response.status_code}")

    collected_at = datetime.now(timezone.utc).isoformat()
    try:
        return parse_feed(response.content, collected_at), collected_at
    except Exception:
        raise RuntimeError("OTD returned an invalid GTFS-Realtime feed") from None


def save_rows(rows, collected_at, output_dir):
    day = collected_at[:10]
    path = Path(output_dir) / f"{day}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=COLUMNS)
        if new_file:
            writer.writeheader()
        writer.writerows(rows)
    return path


def collect_once(output_dir):
    rows, collected_at = fetch_rows()
    path = save_rows(rows, collected_at, output_dir)
    print(f"saved {len(rows)} vehicles to {path}")


def self_test():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 123
    entity = feed.entity.add()
    entity.id = "entity-1"
    entity.vehicle.vehicle.id = "bus-1"
    entity.vehicle.trip.trip_id = "trip-1"
    entity.vehicle.trip.route_id = "route-1"
    entity.vehicle.position.latitude = 28.61
    entity.vehicle.position.longitude = 77.21

    rows = parse_feed(feed.SerializeToString(), "2026-07-29T00:00:00+00:00")
    assert len(rows) == 1
    assert rows[0]["vehicle_id"] == "bus-1"
    assert rows[0]["feed_timestamp"] == 123
    assert list(rows[0]) == COLUMNS
    print("self-test passed")


def main():
    parser = argparse.ArgumentParser(description="Collect Delhi bus vehicle positions")
    parser.add_argument("--interval", type=int, help="seconds between collections")
    parser.add_argument(
        "--output-dir",
        default="data/raw/vehicle_positions",
        help="directory for daily CSV files",
    )
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0
    if args.interval is not None and args.interval < 10:
        parser.error("--interval must be at least 10 seconds")

    try:
        while True:
            try:
                collect_once(args.output_dir)
            except RuntimeError as error:
                print(error, file=sys.stderr)
                if args.interval is None:
                    return 1
            if args.interval is None:
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nstopped")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
