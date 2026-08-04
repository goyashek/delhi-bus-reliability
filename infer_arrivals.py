import argparse
import csv
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from audit_feed import stream_key


DEFAULT_RADIUS_M = 50.0
RADIUS_OPTIONS_M = (25.0, 40.0, 50.0, 75.0)
HIGH_CONFIDENCE_GAP_S = 120.0
MEDIUM_CONFIDENCE_GAP_S = 180.0

ARRIVAL_SCHEMA = pa.schema(
    [
        ("vehicle_id", pa.string()),
        ("route_id", pa.string()),
        ("direction_id", pa.string()),
        ("trip_id", pa.string()),
        ("stop_id", pa.string()),
        ("stop_sequence", pa.int32()),
        ("inferred_arrival_time", pa.timestamp("us", tz="UTC")),
        ("minimum_distance_m", pa.float64()),
        ("sampling_gap_seconds", pa.float64()),
        ("arrival_confidence", pa.string()),
        ("radius_m", pa.float64()),
        ("has_observation_before", pa.bool_()),
        ("has_observation_after", pa.bool_()),
        ("sequence_consistent", pa.bool_()),
    ]
)


def load_progress(path):
    table = pq.read_table(path)
    required = {
        "collection_timestamp",
        "entity_id",
        "vehicle_id",
        "trip_id",
        "route_id",
        "nearest_stop_id",
        "nearest_stop_sequence",
        "distance_to_nearest_stop_m",
        "estimated_direction",
        "is_far_from_route",
        "is_sequence_backstep",
    }
    missing = required - set(table.column_names)
    if missing:
        raise ValueError(f"route-progress file is missing columns: {sorted(missing)}")
    return table.to_pylist()


def grouped_streams(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(row["route_id"], stream_key(row), row["trip_id"] or "")].append(row)
    for stream in groups.values():
        yield sorted(stream, key=lambda row: row["collection_timestamp"])


def confidence(distance_m, radius_m, sampling_gap_s, before, after, sequence_consistent):
    if (
        sequence_consistent
        and before
        and after
        and sampling_gap_s is not None
        and sampling_gap_s <= HIGH_CONFIDENCE_GAP_S
        and distance_m <= radius_m / 2
    ):
        return "high"
    if (
        sequence_consistent
        and sampling_gap_s is not None
        and sampling_gap_s <= MEDIUM_CONFIDENCE_GAP_S
        and (before or after)
    ):
        return "medium"
    return "low"


def neighboring_valid_rows(stream, index):
    before = None
    for before_index in range(index - 1, -1, -1):
        if (
            not stream[before_index]["is_far_from_route"]
            and stream[before_index]["nearest_stop_sequence"] is not None
        ):
            before = stream[before_index]
            break
    after = None
    for after_index in range(index + 1, len(stream)):
        if (
            not stream[after_index]["is_far_from_route"]
            and stream[after_index]["nearest_stop_sequence"] is not None
        ):
            after = stream[after_index]
            break
    return before, after


def infer_stream(stream, radius_m):
    # ponytail: nearest-stop geofence, upgrade to road map matching if validation fails.
    candidates = defaultdict(list)
    last_sequence = None
    eligible_indexes = set()

    for index, row in enumerate(stream):
        if row["is_far_from_route"]:
            continue
        sequence = row["nearest_stop_sequence"]
        distance = row["distance_to_nearest_stop_m"]
        if sequence is None or distance is None:
            continue
        if last_sequence is not None and sequence < last_sequence:
            continue
        last_sequence = sequence
        eligible_indexes.add(index)
        if distance <= radius_m:
            candidates[sequence].append((index, row))

    selected = []
    used_indexes = set()
    last_index = -1
    for sequence in sorted(candidates):
        choices = sorted(
            candidates[sequence],
            key=lambda item: (item[1]["distance_to_nearest_stop_m"], item[0]),
        )
        choice = next(
            (
                item
                for item in choices
                if item[0] > last_index and item[0] not in used_indexes
            ),
            None,
        )
        if choice is None:
            continue
        index, row = choice
        selected.append((sequence, index, row))
        used_indexes.add(index)
        last_index = index

    arrivals = []
    for sequence, index, row in selected:
        before_row, after_row = neighboring_valid_rows(stream, index)
        before = before_row is not None
        after = after_row is not None
        sampling_gap_s = None
        if before and after:
            sampling_gap_s = (
                after_row["collection_timestamp"]
                - before_row["collection_timestamp"]
            ).total_seconds()
        sequence_consistent = (
            not row["is_sequence_backstep"]
            and (
                before_row is None
                or not before_row["is_sequence_backstep"]
                and before_row["nearest_stop_sequence"] <= sequence
            )
            and (
                after_row is None
                or not after_row["is_sequence_backstep"]
                and after_row["nearest_stop_sequence"] >= sequence
            )
        )
        arrivals.append(
            {
                "vehicle_id": row["vehicle_id"],
                "route_id": row["route_id"],
                "direction_id": row["estimated_direction"],
                "trip_id": row["trip_id"],
                "stop_id": row["nearest_stop_id"],
                "stop_sequence": sequence,
                "inferred_arrival_time": row["collection_timestamp"],
                "minimum_distance_m": round(
                    row["distance_to_nearest_stop_m"], 3
                ),
                "sampling_gap_seconds": (
                    round(sampling_gap_s, 3) if sampling_gap_s is not None else None
                ),
                "arrival_confidence": confidence(
                    row["distance_to_nearest_stop_m"],
                    radius_m,
                    sampling_gap_s,
                    before,
                    after,
                    sequence_consistent,
                ),
                "radius_m": radius_m,
                "has_observation_before": before,
                "has_observation_after": after,
                "sequence_consistent": sequence_consistent,
            }
        )
    return arrivals, len(eligible_indexes)


def infer_arrivals(rows, radius_m):
    arrivals = []
    eligible_count = 0
    far_count = 0
    for row in rows:
        far_count += bool(row["is_far_from_route"])
    for stream in grouped_streams(rows):
        stream_arrivals, stream_eligible_count = infer_stream(stream, radius_m)
        arrivals.extend(stream_arrivals)
        eligible_count += stream_eligible_count
    return arrivals, eligible_count, far_count


def write_parquet(rows, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=ARRIVAL_SCHEMA), temporary)
    temporary.replace(path)


def write_csv(rows, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    fields = list(rows[0]) if rows else ["radius_m"]
    with temporary.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def sensitivity_rows(rows, radii):
    output = []
    routes = defaultdict(list)
    for row in rows:
        routes[row["route_id"]].append(row)

    for radius_m in radii:
        arrivals, _, _ = infer_arrivals(rows, radius_m)
        arrivals_by_route = defaultdict(list)
        for arrival in arrivals:
            arrivals_by_route[arrival["route_id"]].append(arrival)
        for route_id, route_rows in sorted(routes.items()):
            route_arrivals = arrivals_by_route[route_id]
            direction = next(
                row["estimated_direction"]
                for row in route_rows
                if row["estimated_direction"]
            )
            route_eligible_count = sum(
                not row["is_far_from_route"] for row in route_rows
            )
            route_far_count = sum(row["is_far_from_route"] for row in route_rows)
            output.append(
                {
                    "radius_m": radius_m,
                    "route_id": route_id,
                    "direction_id": direction,
                    "eligible_observation_count": route_eligible_count,
                    "far_route_excluded_count": route_far_count,
                    "arrival_count": len(route_arrivals),
                    "unique_vehicle_count": len(
                        {row["vehicle_id"] for row in route_arrivals if row["vehicle_id"]}
                    ),
                    "high_confidence_count": sum(
                        row["arrival_confidence"] == "high" for row in route_arrivals
                    ),
                    "medium_confidence_count": sum(
                        row["arrival_confidence"] == "medium" for row in route_arrivals
                    ),
                    "low_confidence_count": sum(
                        row["arrival_confidence"] == "low" for row in route_arrivals
                    ),
                }
            )
    return output


def self_test():
    timestamp = datetime(2026, 8, 1, tzinfo=timezone.utc)
    rows = [
        {
            "collection_timestamp": timestamp,
            "entity_id": "entity-1",
            "vehicle_id": "bus-1",
            "trip_id": "trip-1",
            "route_id": "route-1",
            "nearest_stop_id": "a",
            "nearest_stop_sequence": 0,
            "distance_to_nearest_stop_m": 20.0,
            "estimated_direction": "UP",
            "is_far_from_route": False,
            "is_sequence_backstep": False,
        },
        {
            "collection_timestamp": timestamp.replace(minute=1),
            "entity_id": "entity-1",
            "vehicle_id": "bus-1",
            "trip_id": "trip-1",
            "route_id": "route-1",
            "nearest_stop_id": "b",
            "nearest_stop_sequence": 1,
            "distance_to_nearest_stop_m": 10.0,
            "estimated_direction": "UP",
            "is_far_from_route": False,
            "is_sequence_backstep": False,
        },
        {
            "collection_timestamp": timestamp.replace(minute=2),
            "entity_id": "entity-1",
            "vehicle_id": "bus-1",
            "trip_id": "trip-1",
            "route_id": "route-1",
            "nearest_stop_id": "c",
            "nearest_stop_sequence": 2,
            "distance_to_nearest_stop_m": 100.0,
            "estimated_direction": "UP",
            "is_far_from_route": False,
            "is_sequence_backstep": False,
        },
        {
            "collection_timestamp": timestamp.replace(minute=3),
            "entity_id": "entity-1",
            "vehicle_id": "bus-1",
            "trip_id": "trip-1",
            "route_id": "route-1",
            "nearest_stop_id": "c",
            "nearest_stop_sequence": 3,
            "distance_to_nearest_stop_m": 10.0,
            "estimated_direction": "UP",
            "is_far_from_route": True,
            "is_sequence_backstep": False,
        },
    ]
    arrivals, eligible_count, far_count = infer_arrivals(rows, 40.0)
    assert eligible_count == 3
    assert far_count == 1
    assert [row["stop_id"] for row in arrivals] == ["a", "b"]
    assert arrivals[0]["arrival_confidence"] == "low"
    assert arrivals[1]["arrival_confidence"] == "high"
    assert len({(row["stop_id"], row["inferred_arrival_time"]) for row in arrivals}) == 2
    print("self-test passed")


def process(progress_path, output_path, sensitivity_path, radius_m, radii):
    rows = load_progress(progress_path)
    if radius_m not in radii:
        radii = tuple(sorted(set(radii) | {radius_m}))
    arrivals, eligible_count, far_count = infer_arrivals(rows, radius_m)
    write_parquet(arrivals, Path(output_path))
    write_csv(sensitivity_rows(rows, radii), Path(sensitivity_path))
    print(
        f"radius {radius_m:.0f} m: {len(arrivals):,} arrivals, "
        f"{eligible_count:,} eligible observations, "
        f"{far_count:,} far-route observations excluded"
    )
    print(f"wrote stop arrivals to {output_path}")
    print(f"wrote radius sensitivity to {sensitivity_path}")


def main():
    parser = argparse.ArgumentParser(description="Infer stop arrivals from route progress")
    parser.add_argument(
        "--progress", default="data/processed/route_progress.parquet"
    )
    parser.add_argument(
        "--output", default="data/processed/stop_arrivals.parquet"
    )
    parser.add_argument(
        "--sensitivity", default="data/processed/stop_arrival_sensitivity.csv"
    )
    parser.add_argument("--radius", type=float, default=DEFAULT_RADIUS_M)
    parser.add_argument(
        "--radii", type=float, nargs="+", default=RADIUS_OPTIONS_M
    )
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    try:
        process(
            args.progress,
            args.output,
            args.sensitivity,
            args.radius,
            tuple(args.radii),
        )
    except (OSError, ValueError, pa.ArrowException) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
