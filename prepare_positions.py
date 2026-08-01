import argparse
import csv
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from collect import feed_rows


UTC = timezone.utc
SNAPSHOT_TIME_FORMAT = "%Y-%m-%dT%H-%M-%S.%fZ"
POSITIONS_SCHEMA = pa.schema(
    [
        ("collection_timestamp", pa.timestamp("us", tz="UTC")),
        ("entity_id", pa.string()),
        ("vehicle_id", pa.string()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("speed", pa.float32()),
        ("bearing", pa.float32()),
        ("current_stop_sequence", pa.uint32()),
        ("current_status", pa.string()),
        ("feed_timestamp", pa.timestamp("us", tz="UTC")),
    ]
)
SNAPSHOTS_SCHEMA = pa.schema(
    [
        ("request_timestamp", pa.timestamp("us", tz="UTC")),
        ("response_status", pa.string()),
        ("response_bytes", pa.int64()),
        ("entity_count", pa.int32()),
        ("selected_route_vehicle_count", pa.int32()),
        ("feed_timestamp", pa.timestamp("us", tz="UTC")),
        ("source_file", pa.string()),
        ("parse_status", pa.string()),
    ]
)
HEALTH_COLUMNS = [
    "date",
    "snapshot_count",
    "expected_snapshot_count",
    "coverage_percent",
    "parse_error_count",
    "empty_feed_count",
    "entity_count",
    "selected_position_count",
    "maximum_gap_seconds",
    "median_feed_lag_seconds",
    "repeated_feed_timestamp_count",
    "selected_route_ids",
]


def load_route_ids(schedule_path):
    with Path(schedule_path).open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames or "route_id" not in reader.fieldnames:
            raise ValueError(f"{schedule_path} has no route_id column")
        route_ids = {row["route_id"].strip() for row in reader if row["route_id"].strip()}
    if not route_ids:
        raise ValueError(f"{schedule_path} contains no route IDs")
    return route_ids


def snapshot_time(path):
    return datetime.strptime(path.stem, SNAPSHOT_TIME_FORMAT).replace(tzinfo=UTC)


def feed_time(timestamp):
    return datetime.fromtimestamp(timestamp, UTC) if timestamp else None


def write_parquet(rows, schema, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), temporary)
    temporary.replace(path)


def process_day(day, paths, route_ids, input_dir, output_dir):
    positions = []
    snapshots = []
    feed_timestamps = []
    feed_lags = []

    for path in paths:
        requested_at = snapshot_time(path)
        snapshot = {
            "request_timestamp": requested_at,
            "response_status": "2xx",
            "response_bytes": path.stat().st_size,
            "entity_count": None,
            "selected_route_vehicle_count": None,
            "feed_timestamp": None,
            "source_file": str(path.relative_to(input_dir)),
            "parse_status": "ok",
        }
        try:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(path.read_bytes())
            timestamp = feed_time(feed.header.timestamp)
            rows = feed_rows(feed, requested_at)
            selected_rows = [row for row in rows if row["route_id"] in route_ids]
            for row in selected_rows:
                row["feed_timestamp"] = timestamp
            positions.extend(selected_rows)
            snapshot["entity_count"] = len(feed.entity)
            snapshot["selected_route_vehicle_count"] = sum(
                entity.HasField("vehicle")
                and entity.vehicle.trip.route_id in route_ids
                for entity in feed.entity
            )
            snapshot["feed_timestamp"] = timestamp
            if timestamp:
                feed_timestamps.append(timestamp)
                feed_lags.append((requested_at - timestamp).total_seconds())
        except (DecodeError, OSError, OverflowError, ValueError) as error:
            snapshot["parse_status"] = type(error).__name__
        snapshots.append(snapshot)

    output = output_dir / f"date={day}"
    write_parquet(positions, POSITIONS_SCHEMA, output / "positions.parquet")
    write_parquet(snapshots, SNAPSHOTS_SCHEMA, output / "snapshots.parquet")

    times = [snapshot_time(path) for path in paths]
    gaps = [(later - earlier).total_seconds() for earlier, later in zip(times, times[1:])]
    expected = round((times[-1] - times[0]).total_seconds() / 60) + 1
    valid_entity_counts = [
        snapshot["entity_count"]
        for snapshot in snapshots
        if snapshot["entity_count"] is not None
    ]
    health = {
        "date": day,
        "snapshot_count": len(paths),
        "expected_snapshot_count": expected,
        "coverage_percent": round(100 * len(paths) / expected, 2),
        "parse_error_count": sum(snapshot["parse_status"] != "ok" for snapshot in snapshots),
        "empty_feed_count": sum(count == 0 for count in valid_entity_counts),
        "entity_count": sum(valid_entity_counts),
        "selected_position_count": len(positions),
        "maximum_gap_seconds": int(max(gaps, default=0)),
        "median_feed_lag_seconds": round(median(feed_lags), 1) if feed_lags else "",
        "repeated_feed_timestamp_count": sum(
            current == previous
            for previous, current in zip(feed_timestamps, feed_timestamps[1:])
        ),
        "selected_route_ids": ";".join(sorted(route_ids)),
    }
    print(
        f"{day}: {len(paths)}/{expected} snapshots, "
        f"{len(positions):,} selected positions, "
        f"{health['empty_feed_count']} empty feeds"
    )
    return health


def write_health(rows, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=HEALTH_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def process_archive(input_dir, schedule_path, output_dir, health_path):
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    route_ids = load_route_ids(schedule_path)
    days = defaultdict(list)
    for path in sorted(input_dir.glob("*/*.pb")):
        days[path.parent.name].append(path)
    if not days:
        raise ValueError(f"No protobuf snapshots found under {input_dir}")

    health = [
        process_day(day, paths, route_ids, input_dir, output_dir)
        for day, paths in sorted(days.items())
    ]
    write_health(health, Path(health_path))
    print(f"wrote health summary to {health_path}")
    return health


def self_test():
    with TemporaryDirectory() as temporary:
        root = Path(temporary)
        raw = root / "raw/2026-08-01"
        raw.mkdir(parents=True)
        schedule = root / "selected.csv"
        schedule.write_text("route_id\nselected\n", encoding="utf-8")

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = 1785542450
        entity = feed.entity.add()
        entity.id = "entity-1"
        entity.vehicle.vehicle.id = "bus-1"
        entity.vehicle.trip.route_id = "selected"
        entity.vehicle.position.latitude = 28.61
        entity.vehicle.position.longitude = 77.21
        (raw / "2026-08-01T00-00-50.000Z.pb").write_bytes(feed.SerializeToString())

        feed.Clear()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = 1785542570
        (raw / "2026-08-01T00-02-50.000Z.pb").write_bytes(feed.SerializeToString())

        output = root / "processed"
        health_path = output / "collection_health.csv"
        health = process_archive(raw.parent, schedule, output / "positions", health_path)
        day = output / "positions/date=2026-08-01"
        assert pq.read_metadata(day / "positions.parquet").num_rows == 1
        assert pq.read_metadata(day / "snapshots.parquet").num_rows == 2
        assert pq.read_schema(day / "positions.parquet") == POSITIONS_SCHEMA
        assert health[0]["expected_snapshot_count"] == 3
        assert health[0]["empty_feed_count"] == 1
        assert health[0]["maximum_gap_seconds"] == 120
    print("self-test passed")


def main():
    parser = argparse.ArgumentParser(
        description="Prepare selected Delhi bus positions from raw protobuf snapshots"
    )
    parser.add_argument("--input-dir", default="data/raw/vehicle_positions_pb")
    parser.add_argument(
        "--schedule", default="data/interim/selected_route_schedule.csv"
    )
    parser.add_argument(
        "--output-dir", default="data/processed/vehicle_positions"
    )
    parser.add_argument(
        "--health", default="data/processed/collection_health.csv"
    )
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return
    try:
        process_archive(args.input_dir, args.schedule, args.output_dir, args.health)
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
