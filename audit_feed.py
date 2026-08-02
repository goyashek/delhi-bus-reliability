import argparse
import csv
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

import pyarrow as pa
import pyarrow.parquet as pq

from prepare_positions import load_route_ids


EARTH_RADIUS_M = 6_371_008.8
MAX_JUMP_METERS = 2_000.0
MAX_IMPLAUSIBLE_SPEED_KMH = 120.0
MISSING_FIELDS = [
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
FLAG_COLUMNS = [
    "is_duplicate",
    "is_stale",
    "is_invalid_coordinate",
    "is_large_jump",
    "is_implausible_speed",
    "is_missing_trip_id",
]
FLAG_SCHEMA = pa.schema(
    [
        ("collection_timestamp", pa.timestamp("us", tz="UTC")),
        ("entity_id", pa.string()),
        ("vehicle_id", pa.string()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("feed_timestamp", pa.timestamp("us", tz="UTC")),
        ("update_interval_seconds", pa.float64()),
        ("distance_since_previous_m", pa.float64()),
        ("implied_speed_kmh", pa.float64()),
        *((name, pa.bool_()) for name in FLAG_COLUMNS),
    ]
)


def valid_coordinate(latitude, longitude):
    return (
        latitude is not None
        and longitude is not None
        and math.isfinite(latitude)
        and math.isfinite(longitude)
        and -90 <= latitude <= 90
        and -180 <= longitude <= 180
        and latitude != 0
        and longitude != 0
    )


def haversine_m(latitude1, longitude1, latitude2, longitude2):
    if not all(valid_coordinate(*pair) for pair in [(latitude1, longitude1), (latitude2, longitude2)]):
        return None
    lat1, lat2 = math.radians(latitude1), math.radians(latitude2)
    dlat = lat2 - lat1
    dlon = math.radians(longitude2 - longitude1)
    value = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(value))


def stream_key(row):
    return row["vehicle_id"] or f"entity:{row['entity_id']}"


def audit_rows(rows):
    rows = sorted(
        rows,
        key=lambda row: (
            row["route_id"] or "",
            stream_key(row),
            row["collection_timestamp"],
            row["entity_id"] or "",
        ),
    )
    previous = {}
    seen = set()
    audited = []

    for row in rows:
        current = dict(row)
        key = (
            current["route_id"],
            stream_key(current),
            current["collection_timestamp"],
            current["latitude"],
            current["longitude"],
            current["trip_id"],
        )
        current["is_duplicate"] = key in seen
        seen.add(key)
        current["is_invalid_coordinate"] = not valid_coordinate(
            current["latitude"], current["longitude"]
        )
        current["is_missing_trip_id"] = not current["trip_id"]
        current["update_interval_seconds"] = None
        current["distance_since_previous_m"] = None
        current["implied_speed_kmh"] = None
        current["is_large_jump"] = False
        current["is_implausible_speed"] = False
        current["is_stale"] = False

        prior = previous.get((current["route_id"], stream_key(current)))
        if prior is not None:
            interval = (
                current["collection_timestamp"] - prior["collection_timestamp"]
            ).total_seconds()
            if interval > 0:
                current["update_interval_seconds"] = interval
                distance = haversine_m(
                    prior["latitude"],
                    prior["longitude"],
                    current["latitude"],
                    current["longitude"],
                )
                if distance is not None:
                    current["distance_since_previous_m"] = distance
                    current["is_large_jump"] = distance > MAX_JUMP_METERS
                    current["implied_speed_kmh"] = distance / interval * 3.6
                    current["is_implausible_speed"] = (
                        current["implied_speed_kmh"] > MAX_IMPLAUSIBLE_SPEED_KMH
                    )
            if (
                current["feed_timestamp"] is not None
                and prior["feed_timestamp"] is not None
                and current["feed_timestamp"] <= prior["feed_timestamp"]
            ):
                current["is_stale"] = True

        previous[(current["route_id"], stream_key(current))] = current
        audited.append(current)
    return audited


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * fraction))
    return ordered[index]


def load_position_rows(input_dir):
    paths = sorted(Path(input_dir).glob("date=*/positions.parquet"))
    if not paths:
        raise ValueError(f"No position partitions found under {input_dir}")
    rows = []
    for path in paths:
        rows.extend(pq.read_table(path).to_pylist())
    return rows


def route_summary(rows, route_id, static_route_ids):
    route_rows = [row for row in rows if row["route_id"] == route_id]
    vehicles = Counter(row["vehicle_id"] for row in route_rows if row["vehicle_id"])
    trips = {row["trip_id"] for row in route_rows if row["trip_id"]}
    previous_trips = {}
    trip_transitions = 0
    for row in route_rows:
        key = stream_key(row)
        if row["trip_id"] and previous_trips.get(key) not in (None, row["trip_id"]):
            trip_transitions += 1
        if row["trip_id"]:
            previous_trips[key] = row["trip_id"]
    intervals = [
        row["update_interval_seconds"]
        for row in route_rows
        if row["update_interval_seconds"] is not None
    ]
    speeds = [
        row["implied_speed_kmh"]
        for row in route_rows
        if row["implied_speed_kmh"] is not None
    ]
    summary = {
        "route_id": route_id,
        "observation_count": len(route_rows),
        "unique_vehicle_count": len(vehicles),
        "unique_trip_id_count": len(trips),
        "trip_id_transition_count": trip_transitions,
        "mean_observations_per_vehicle": round(
            len(route_rows) / len(vehicles), 2
        )
        if vehicles
        else None,
        "median_observations_per_vehicle": median(vehicles.values()) if vehicles else None,
        "median_update_interval_seconds": median(intervals) if intervals else None,
        "implied_speed_observation_count": len(speeds),
        "median_implied_speed_kmh": round(median(speeds), 2) if speeds else None,
        "p95_implied_speed_kmh": round(percentile(speeds, 0.95), 2) if speeds else None,
        "maximum_implied_speed_kmh": round(max(speeds), 2) if speeds else None,
        "duplicate_count": sum(row["is_duplicate"] for row in route_rows),
        "zero_coordinate_count": sum(
            row["latitude"] == 0 or row["longitude"] == 0
            for row in route_rows
        ),
        "stale_timestamp_count": sum(row["is_stale"] for row in route_rows),
        "large_jump_count": sum(row["is_large_jump"] for row in route_rows),
        "implausible_speed_count": sum(
            row["is_implausible_speed"] for row in route_rows
        ),
        "missing_trip_id_count": sum(row["is_missing_trip_id"] for row in route_rows),
        "static_route_ids_absent": ";".join(
            sorted(static_route_ids - {row["route_id"] for row in rows})
        ),
    }
    for field in MISSING_FIELDS:
        missing = sum(row[field] is None or row[field] == "" for row in route_rows)
        summary[f"missing_{field}_count"] = missing
        summary[f"missing_{field}_percent"] = round(100 * missing / len(route_rows), 2)
    for name in FLAG_COLUMNS:
        summary[f"{name}_percent"] = round(
            100 * sum(row[name] for row in route_rows) / len(route_rows), 2
        )
    return summary


def write_parquet(rows, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=FLAG_SCHEMA), temporary)
    temporary.replace(path)


def write_csv(rows, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    fields = list(rows[0]) if rows else ["route_id"]
    with temporary.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def audit(input_dir, schedule_path, flags_output, summary_output):
    static_route_ids = load_route_ids(schedule_path)
    rows = audit_rows(load_position_rows(input_dir))
    live_route_ids = {row["route_id"] for row in rows if row["route_id"]}
    summaries = [
        route_summary(rows, route_id, static_route_ids)
        for route_id in sorted(live_route_ids)
    ]
    write_parquet(rows, Path(flags_output))
    write_csv(summaries, Path(summary_output))
    for summary in summaries:
        print(
            f"route {summary['route_id']}: "
            f"{summary['observation_count']:,} observations, "
            f"{summary['unique_vehicle_count']} vehicles, "
            f"{summary['median_update_interval_seconds']}s median update, "
            f"{summary['implausible_speed_count']} implausible speeds"
        )
    print(f"wrote quality flags to {flags_output}")
    print(f"wrote route summary to {summary_output}")
    return summaries


def self_test():
    timestamp = datetime(2026, 8, 1, tzinfo=timezone.utc)
    rows = [
        {
            "collection_timestamp": timestamp,
            "entity_id": "entity-1",
            "vehicle_id": "bus-1",
            "trip_id": "trip-1",
            "route_id": "route-1",
            "latitude": 28.61,
            "longitude": 77.21,
            "feed_timestamp": timestamp,
        },
        {
            "collection_timestamp": timestamp.replace(minute=1),
            "entity_id": "entity-1",
            "vehicle_id": "bus-1",
            "trip_id": "trip-1",
            "route_id": "route-1",
            "latitude": 28.61,
            "longitude": 77.21,
            "feed_timestamp": timestamp,
        },
        {
            "collection_timestamp": timestamp.replace(minute=2),
            "entity_id": "entity-1",
            "vehicle_id": "bus-1",
            "trip_id": "",
            "route_id": "route-1",
            "latitude": 29.0,
            "longitude": 77.21,
            "feed_timestamp": timestamp.replace(minute=2),
        },
        {
            "collection_timestamp": timestamp.replace(minute=2),
            "entity_id": "entity-1",
            "vehicle_id": "bus-1",
            "trip_id": "",
            "route_id": "route-1",
            "latitude": 29.0,
            "longitude": 77.21,
            "feed_timestamp": timestamp.replace(minute=2),
        },
    ]
    audited = audit_rows(rows)
    assert audited[1]["is_stale"]
    assert audited[1]["update_interval_seconds"] == 60
    assert audited[2]["is_missing_trip_id"]
    assert audited[2]["is_large_jump"]
    assert audited[2]["is_implausible_speed"]
    assert audited[3]["is_duplicate"]
    print("self-test passed")


def main():
    parser = argparse.ArgumentParser(description="Audit selected Delhi bus feed quality")
    parser.add_argument(
        "--input-dir", default="data/processed/vehicle_positions"
    )
    parser.add_argument(
        "--schedule", default="data/interim/selected_route_schedule.csv"
    )
    parser.add_argument(
        "--flags-output", default="data/processed/feed_quality_flags.parquet"
    )
    parser.add_argument("--summary-output", default="data/processed/feed_quality.csv")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0
    try:
        audit(
            args.input_dir,
            args.schedule,
            args.flags_output,
            args.summary_output,
        )
    except (OSError, ValueError, pa.ArrowException) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
