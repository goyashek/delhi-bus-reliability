import argparse
import csv
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from audit_feed import haversine_m, load_position_rows, stream_key, valid_coordinate
from prepare_positions import POSITIONS_SCHEMA


MAX_DISTANCE_TO_REFERENCE_STOP_M = 1_500.0
PROGRESS_SCHEMA = pa.schema(
    list(POSITIONS_SCHEMA)
    + [
        ("nearest_stop_id", pa.string()),
        ("nearest_stop_sequence", pa.int32()),
        ("temporal_stop_sequence", pa.int32()),
        ("distance_to_nearest_stop_m", pa.float64()),
        ("route_variant_label", pa.string()),
        ("direction_id", pa.string()),
        ("route_progress_fraction", pa.float64()),
        ("route_distance_m", pa.float64()),
        ("is_far_from_route", pa.bool_()),
        ("is_sequence_backstep", pa.bool_()),
    ]
)


def load_geometry(schedule_path):
    groups = defaultdict(dict)
    labels = defaultdict(set)
    with Path(schedule_path).open(newline="", encoding="utf-8") as file:
        for row in csv.DictReader(file):
            route_id = row["route_id"].strip()
            route_variant_label = row["route_long_name"].strip() or route_id
            direction_id = row["direction_id"].strip() or None
            sequence = int(float(row["stop_sequence"]))
            stop = {
                "route_id": route_id,
                "route_variant_label": route_variant_label,
                "direction_id": direction_id,
                "stop_id": row["stop_id"].strip(),
                "stop_name": row["stop_name"].strip(),
                "stop_sequence": sequence,
                "stop_lat": float(row["stop_lat"]),
                "stop_lon": float(row["stop_lon"]),
            }
            labels[route_id].add(route_variant_label)
            previous = groups[(route_id, route_variant_label)].get(sequence)
            if previous is not None and previous["stop_id"] != stop["stop_id"]:
                raise ValueError(
                    f"stop sequence {sequence} changes within {route_id} {route_variant_label}"
                )
            groups[(route_id, route_variant_label)][sequence] = stop

    geometries = {}
    geometry_rows = []
    for route_id in sorted(labels):
        if len(labels[route_id]) != 1:
            raise ValueError(f"{route_id} has multiple direction labels")
        route_variant_label = next(iter(labels[route_id]))
        stops = [
            groups[(route_id, route_variant_label)][sequence]
            for sequence in sorted(groups[(route_id, route_variant_label)])
        ]
        if len(stops) < 2:
            raise ValueError(f"{route_id} has fewer than two stops")
        distance_so_far = 0.0
        for index, stop in enumerate(stops):
            segment_distance = 0.0
            if index:
                segment_distance = haversine_m(
                    stops[index - 1]["stop_lat"],
                    stops[index - 1]["stop_lon"],
                    stop["stop_lat"],
                    stop["stop_lon"],
                )
                if segment_distance is None:
                    raise ValueError(f"invalid coordinates in {route_id} {direction}")
                distance_so_far += segment_distance
            output = {
                **stop,
                "segment_distance_m": round(segment_distance, 3),
                "route_distance_m": round(distance_so_far, 3),
                "route_progress_fraction": 0.0,
            }
            geometry_rows.append(output)
        total_distance = distance_so_far
        for output in geometry_rows[-len(stops) :]:
            output["route_progress_fraction"] = round(
                output["route_distance_m"] / total_distance, 6
            )
        geometries[route_id] = [dict(row) for row in geometry_rows[-len(stops) :]]
    return geometries, geometry_rows


def nearest_stop(row, stops):
    if not valid_coordinate(row["latitude"], row["longitude"]):
        return None, None
    distances = [
        (
            haversine_m(
                row["latitude"],
                row["longitude"],
                stop["stop_lat"],
                stop["stop_lon"],
            ),
            stop,
        )
        for stop in stops
    ]
    distance, stop = min(distances, key=lambda pair: pair[0])
    return stop, distance


def assign_nearest(rows, geometries):
    assigned = []
    for row in rows:
        current = dict(row)
        stop, distance = nearest_stop(current, geometries[current["route_id"]])
        current["nearest_stop_id"] = stop["stop_id"] if stop else None
        current["nearest_stop_sequence"] = stop["stop_sequence"] if stop else None
        current["temporal_stop_sequence"] = None
        current["distance_to_nearest_stop_m"] = distance
        current["route_variant_label"] = (
            stop["route_variant_label"] if stop else None
        )
        current["direction_id"] = stop["direction_id"] if stop else None
        current["route_progress_fraction"] = None
        current["route_distance_m"] = None
        current["is_far_from_route"] = (
            distance is not None and distance > MAX_DISTANCE_TO_REFERENCE_STOP_M
        )
        current["is_sequence_backstep"] = False
        assigned.append(current)
    return assigned


def apply_temporal_consistency(rows, geometries):
    grouped = defaultdict(list)
    for index, row in enumerate(rows):
        grouped[(row["route_id"], stream_key(row), row["trip_id"] or "")].append(index)

    for indexes in grouped.values():
        indexes.sort(key=lambda index: rows[index]["observation_timestamp"])
        previous_sequence = None
        for index in indexes:
            row = rows[index]
            raw_sequence = row["nearest_stop_sequence"]
            if raw_sequence is None:
                continue
            row["is_sequence_backstep"] = (
                previous_sequence is not None and raw_sequence < previous_sequence
            )
            row["temporal_stop_sequence"] = (
                max(raw_sequence, previous_sequence)
                if previous_sequence is not None
                else raw_sequence
            )
            stop = next(
                stop
                for stop in geometries[row["route_id"]]
                if stop["stop_sequence"] == row["temporal_stop_sequence"]
            )
            row["route_progress_fraction"] = stop["route_progress_fraction"]
            row["route_distance_m"] = stop["route_distance_m"]
            previous_sequence = row["temporal_stop_sequence"]
    return rows


def route_summary(rows, geometries):
    by_route = defaultdict(list)
    for row in rows:
        by_route[row["route_id"]].append(row)
    summaries = []
    for route_id, route_rows in sorted(by_route.items()):
        distances = sorted(
            row["distance_to_nearest_stop_m"]
            for row in route_rows
            if row["distance_to_nearest_stop_m"] is not None
        )
        stop_counts = Counter(
            row["vehicle_id"] for row in route_rows if row["vehicle_id"]
        )
        static = geometries[route_id]
        p95_index = min(len(distances) - 1, round((len(distances) - 1) * 0.95))
        summaries.append(
            {
                "route_id": route_id,
                "route_variant_label": static[0]["route_variant_label"],
                "direction_id": static[0]["direction_id"],
                "stop_count": len(static),
                "route_length_m": static[-1]["route_distance_m"],
                "observation_count": len(route_rows),
                "unique_vehicle_count": len(stop_counts),
                "median_distance_to_nearest_stop_m": round(
                    distances[len(distances) // 2], 2
                ),
                "p95_distance_to_nearest_stop_m": round(distances[p95_index], 2),
                "maximum_distance_to_nearest_stop_m": round(distances[-1], 2),
                "far_from_route_count": sum(
                    row["is_far_from_route"] for row in route_rows
                ),
                "far_from_route_percent": round(
                    100
                    * sum(row["is_far_from_route"] for row in route_rows)
                    / len(route_rows),
                    2,
                ),
                "sequence_backstep_count": sum(
                    row["is_sequence_backstep"] for row in route_rows
                ),
            }
        )
    return summaries


def write_parquet(rows, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(pa.Table.from_pylist(rows, schema=PROGRESS_SCHEMA), temporary)
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


def plot_trajectories(rows, geometries, path):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    route_ids = sorted(geometries)
    figure, axes = plt.subplots(2, 2, figsize=(12, 10), squeeze=False)
    for axis, route_id in zip(axes.flat, route_ids):
        static = geometries[route_id]
        axis.plot(
            [stop["stop_lon"] for stop in static],
            [stop["stop_lat"] for stop in static],
            color="black",
            linewidth=1,
            marker="o",
            markersize=2,
            label="static stops",
        )
        vehicle_rows = defaultdict(list)
        for row in rows:
            if row["route_id"] == route_id and row["vehicle_id"]:
                vehicle_rows[row["vehicle_id"]].append(row)
        selected = sorted(vehicle_rows.items(), key=lambda item: len(item[1]), reverse=True)[:3]
        for vehicle_id, vehicle_points in selected:
            vehicle_points.sort(key=lambda row: row["observation_timestamp"])
            axis.plot(
                [row["longitude"] for row in vehicle_points],
                [row["latitude"] for row in vehicle_points],
                linewidth=0.8,
                alpha=0.7,
                label=vehicle_id,
            )
        axis.set_title(f"{route_id} {static[0]['route_variant_label']}")
        axis.set_xlabel("longitude")
        axis.set_ylabel("latitude")
        axis.legend(fontsize=7)
    figure.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(path, dpi=150)
    plt.close(figure)


def process(input_dir, schedule_path, output_path, geometry_path, summary_path, plot_path):
    geometries, geometry_rows = load_geometry(schedule_path)
    rows = apply_temporal_consistency(
        assign_nearest(load_position_rows(input_dir), geometries), geometries
    )
    summaries = route_summary(rows, geometries)
    write_parquet(rows, Path(output_path))
    write_csv(geometry_rows, Path(geometry_path))
    write_csv(summaries, Path(summary_path))
    plot_trajectories(rows, geometries, Path(plot_path))
    for summary in summaries:
        print(
            f"route {summary['route_id']}: {summary['stop_count']} stops, "
            f"{summary['route_length_m'] / 1000:.2f} km, "
            f"{summary['far_from_route_percent']:.2f}% far from reference stops"
        )
    print(f"wrote route progress to {output_path}")
    print(f"wrote route geometry to {geometry_path}")
    print(f"wrote route summary to {summary_path}")
    print(f"wrote trajectory plot to {plot_path}")
    return summaries


def self_test():
    timestamp = datetime(2026, 8, 1, tzinfo=timezone.utc)
    geometries = {
        "route-1": [
            {
                "route_id": "route-1",
                "route_variant_label": "test",
                "direction_id": None,
                "stop_id": "a",
                "stop_sequence": 0,
                "stop_lat": 28.6,
                "stop_lon": 77.2,
                "route_distance_m": 0.0,
                "route_progress_fraction": 0.0,
            },
            {
                "route_id": "route-1",
                "route_variant_label": "test",
                "direction_id": None,
                "stop_id": "b",
                "stop_sequence": 1,
                "stop_lat": 28.6,
                "stop_lon": 77.21,
                "route_distance_m": 1.0,
                "route_progress_fraction": 1.0,
            },
        ]
    }
    rows = [
        {
            "collection_timestamp": timestamp,
            "observation_timestamp": timestamp,
            "entity_id": "entity-1",
            "vehicle_id": "bus-1",
            "trip_id": "trip-1",
            "route_id": "route-1",
            "latitude": 28.6,
            "longitude": 77.21,
        },
        {
            "collection_timestamp": timestamp.replace(minute=1),
            "observation_timestamp": timestamp.replace(minute=1),
            "entity_id": "entity-1",
            "vehicle_id": "bus-1",
            "trip_id": "trip-1",
            "route_id": "route-1",
            "latitude": 28.6,
            "longitude": 77.2,
        },
    ]
    rows = apply_temporal_consistency(assign_nearest(rows, geometries), geometries)
    assert rows[0]["nearest_stop_sequence"] == 1
    assert rows[1]["is_sequence_backstep"]
    assert rows[1]["temporal_stop_sequence"] == 1
    assert rows[1]["route_progress_fraction"] == 1.0
    print("self-test passed")


def main():
    parser = argparse.ArgumentParser(description="Build route geometry and GPS progress")
    parser.add_argument("--input-dir", default="data/processed/vehicle_positions")
    parser.add_argument(
        "--schedule", default="data/interim/selected_route_schedule.csv"
    )
    parser.add_argument(
        "--output", default="data/processed/route_progress.parquet"
    )
    parser.add_argument(
        "--geometry", default="data/processed/route_geometry.csv"
    )
    parser.add_argument(
        "--summary", default="data/processed/route_progress_summary.csv"
    )
    parser.add_argument(
        "--plot", default="artifacts/route_progress_trajectories.png"
    )
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    try:
        process(
            args.input_dir,
            args.schedule,
            args.output,
            args.geometry,
            args.summary,
            args.plot,
        )
    except (OSError, ValueError, pa.ArrowException) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
