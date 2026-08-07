"""Reconstruct bus trips from vehicle position streams.

Splits each vehicle's chronological stream into trip segments using:
1. change in live trip_id (the feed provides a trip identifier per observation)
2. route-variant change (route_id or route_variant_label changes)
3. long temporal gap (default 30 minutes between consecutive observations)
4. stop-sequence reset near a terminal (sequence drops to near start after
   being near the end, within the gap threshold)

Produces a trip table and optionally links existing stop arrivals to
reconstructed trip IDs.
"""

import argparse
import csv
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from audit_feed import stream_key


LONG_GAP_SECONDS = 1800.0  # 30 minutes
TERMINAL_LOW_FRACTION = 0.15  # stop sequence in the bottom 15% counts as near-start
TERMINAL_HIGH_FRACTION = 0.85  # stop sequence in the top 15% counts as near-end

# Trip quality thresholds. These are judgement calls tuned against the current
# archive, not measured constants, so they are named here to keep them reviewable.
MINIMUM_TRIP_OBSERVATIONS = 3  # fewer observations than this is a fragment
COMPLETE_COMPLETION_RATIO = 0.7  # share of route stops seen to call a trip complete
COMPLETE_MAXIMUM_GAP_SECONDS = 600.0  # a complete trip may not hide a gap longer than this
PARTIAL_COMPLETION_RATIO = 0.4  # share of route stops seen to call a trip partial
PARTIAL_MAXIMUM_VIOLATIONS = 2  # sequence backsteps tolerated in a partial trip

TRIP_SCHEMA = pa.schema(
    [
        ("reconstructed_trip_id", pa.string()),
        ("vehicle_id", pa.string()),
        ("route_id", pa.string()),
        ("route_variant_label", pa.string()),
        ("direction_id", pa.string()),
        ("original_trip_id", pa.string()),
        ("start_time", pa.timestamp("us", tz="UTC")),
        ("end_time", pa.timestamp("us", tz="UTC")),
        ("duration_seconds", pa.float64()),
        ("observation_count", pa.int32()),
        ("observed_stop_count", pa.int32()),
        ("expected_stop_count", pa.int32()),
        ("completion_ratio", pa.float64()),
        ("maximum_sampling_gap_seconds", pa.float64()),
        ("sequence_violation_count", pa.int32()),
        ("split_reason", pa.string()),
        ("trip_quality", pa.string()),
    ]
)


def load_progress(path):
    table = pq.read_table(path)
    required = {
        "observation_timestamp",
        "vehicle_id",
        "trip_id",
        "route_id",
        "route_variant_label",
        "direction_id",
        "nearest_stop_sequence",
        "is_far_from_route",
        "is_sequence_backstep",
    }
    missing = required - set(table.column_names)
    if missing:
        raise ValueError(f"route-progress file is missing columns: {sorted(missing)}")
    return table.to_pylist()


def load_route_terminals(schedule_path):
    """Load expected stop counts per route from the selected schedule."""
    route_max_seq = {}
    with open(schedule_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            route_id = row["route_id"]
            seq = int(row["stop_sequence"])
            if route_id not in route_max_seq:
                route_max_seq[route_id] = seq
            else:
                route_max_seq[route_id] = max(route_max_seq[route_id], seq)
    # expected stop count = max_sequence + 1 (zero-indexed)
    return {route_id: max_seq + 1 for route_id, max_seq in route_max_seq.items()}


def is_terminal_reset(prev_seq, curr_seq, expected_stops):
    """Check if stop sequence reset from near-end to near-start."""
    if prev_seq is None or curr_seq is None or expected_stops is None:
        return False
    if expected_stops <= 1:
        return False
    high_threshold = expected_stops * TERMINAL_HIGH_FRACTION
    low_threshold = expected_stops * TERMINAL_LOW_FRACTION
    return prev_seq >= high_threshold and curr_seq <= low_threshold


def segment_vehicle_stream(rows, expected_stops_by_route, gap_threshold_s):
    """Split one vehicle's sorted stream into trip segments.

    Returns a list of (segment_rows, split_reason) tuples.
    """
    if not rows:
        return []

    segments = []
    current_segment = [rows[0]]
    current_reason = "start"

    for i in range(1, len(rows)):
        prev = rows[i - 1]
        curr = rows[i]
        reason = None

        # Check split conditions in priority order
        gap_s = (
            curr["observation_timestamp"] - prev["observation_timestamp"]
        ).total_seconds()

        if curr["route_id"] != prev["route_id"] or curr["route_variant_label"] != prev["route_variant_label"]:
            reason = "route_change"
        elif curr["trip_id"] != prev["trip_id"]:
            # trip_id changed but route stayed the same
            expected = expected_stops_by_route.get(curr["route_id"])
            if is_terminal_reset(
                prev["nearest_stop_sequence"],
                curr["nearest_stop_sequence"],
                expected,
            ):
                reason = "terminal_reset"
            else:
                reason = "trip_id_change"
        elif gap_s > gap_threshold_s:
            reason = "long_gap"
        else:
            # Same trip_id, same route, short gap: check terminal reset
            expected = expected_stops_by_route.get(curr["route_id"])
            if is_terminal_reset(
                prev["nearest_stop_sequence"],
                curr["nearest_stop_sequence"],
                expected,
            ):
                reason = "terminal_reset"

        if reason:
            segments.append((current_segment, current_reason))
            current_segment = [curr]
            current_reason = reason
        else:
            current_segment.append(curr)

    segments.append((current_segment, current_reason))
    return segments


def build_trip_record(segment_rows, reconstructed_trip_id, split_reason, expected_stops_by_route):
    """Build a single trip summary from segment observations."""
    first = segment_rows[0]
    last = segment_rows[-1]
    route_id = first["route_id"]
    expected_stops = expected_stops_by_route.get(route_id)

    # Count unique non-null stop sequences visited (non-far-route only)
    stop_sequences_seen = set()
    for r in segment_rows:
        if not r["is_far_from_route"] and r["nearest_stop_sequence"] is not None:
            stop_sequences_seen.add(r["nearest_stop_sequence"])
    observed_stop_count = len(stop_sequences_seen)

    # Maximum sampling gap
    max_gap_s = 0.0
    for i in range(1, len(segment_rows)):
        gap = (
            segment_rows[i]["observation_timestamp"]
            - segment_rows[i - 1]["observation_timestamp"]
        ).total_seconds()
        max_gap_s = max(max_gap_s, gap)

    # Sequence violations: count backsteps in non-far observations
    sequence_violations = 0
    last_seq = None
    for r in segment_rows:
        if r["is_far_from_route"]:
            continue
        seq = r["nearest_stop_sequence"]
        if seq is None:
            continue
        if last_seq is not None and seq < last_seq:
            sequence_violations += 1
        last_seq = seq

    # Completion ratio
    completion_ratio = None
    if expected_stops and expected_stops > 0:
        completion_ratio = round(observed_stop_count / expected_stops, 4)

    # Duration
    duration_s = (last["observation_timestamp"] - first["observation_timestamp"]).total_seconds()

    # Trip quality
    trip_quality = classify_quality(
        completion_ratio, max_gap_s, sequence_violations, len(segment_rows)
    )

    return {
        "reconstructed_trip_id": reconstructed_trip_id,
        "vehicle_id": first["vehicle_id"],
        "route_id": route_id,
        "route_variant_label": first["route_variant_label"],
        "direction_id": first["direction_id"],
        "original_trip_id": first["trip_id"],
        "start_time": first["observation_timestamp"],
        "end_time": last["observation_timestamp"],
        "duration_seconds": round(duration_s, 1),
        "observation_count": len(segment_rows),
        "observed_stop_count": observed_stop_count,
        "expected_stop_count": expected_stops,
        "completion_ratio": completion_ratio,
        "maximum_sampling_gap_seconds": round(max_gap_s, 1),
        "sequence_violation_count": sequence_violations,
        "split_reason": split_reason,
        "trip_quality": trip_quality,
    }


def classify_quality(completion_ratio, max_gap_s, violations, obs_count):
    """Assign a quality label based on stop coverage, gaps, and sequence order.

    The thresholds are the module constants above. A trip is complete when it
    covers most of the route cleanly, partial when it covers a usable stretch,
    and low when it covers too little to trust.
    """
    if obs_count < MINIMUM_TRIP_OBSERVATIONS:
        return "fragment"
    if completion_ratio is None:
        return "low"
    if (
        completion_ratio >= COMPLETE_COMPLETION_RATIO
        and violations == 0
        and max_gap_s <= COMPLETE_MAXIMUM_GAP_SECONDS
    ):
        return "complete"
    if (
        completion_ratio >= PARTIAL_COMPLETION_RATIO
        and violations <= PARTIAL_MAXIMUM_VIOLATIONS
    ):
        return "partial"
    return "low"


def reconstruct_trips(rows, expected_stops_by_route, gap_threshold_s):
    """Reconstruct trips from all observations.

    Returns (trip_records, observation_trip_map) where observation_trip_map
    maps (vehicle_id, observation_timestamp) to reconstructed_trip_id.
    """
    # Group by vehicle
    vehicles = defaultdict(list)
    for row in rows:
        vehicles[row["vehicle_id"]].append(row)

    trip_records = []
    observation_trip_map = {}
    trip_counter = 0

    for vehicle_id in sorted(vehicles):
        vehicle_rows = sorted(
            vehicles[vehicle_id], key=lambda r: r["observation_timestamp"]
        )
        segments = segment_vehicle_stream(
            vehicle_rows, expected_stops_by_route, gap_threshold_s
        )
        for segment_rows, split_reason in segments:
            trip_counter += 1
            reconstructed_trip_id = f"rt_{trip_counter:05d}"
            record = build_trip_record(
                segment_rows, reconstructed_trip_id, split_reason, expected_stops_by_route
            )
            trip_records.append(record)
            for r in segment_rows:
                observation_trip_map[
                    (r["vehicle_id"], r["observation_timestamp"])
                ] = reconstructed_trip_id

    return trip_records, observation_trip_map


def link_arrivals(arrivals_path, observation_trip_map, output_path):
    """Add reconstructed_trip_id to existing stop arrivals."""
    table = pq.read_table(arrivals_path)
    arrival_rows = table.to_pylist()
    linked = 0
    for row in arrival_rows:
        key = (row["vehicle_id"], row["inferred_arrival_time"])
        row["reconstructed_trip_id"] = observation_trip_map.get(key)
        if row["reconstructed_trip_id"] is not None:
            linked += 1

    # Write linked arrivals
    schema = table.schema.append(pa.field("reconstructed_trip_id", pa.string()))
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    pq.write_table(pa.Table.from_pylist(arrival_rows, schema=schema), tmp)
    tmp.replace(output_path)
    return len(arrival_rows), linked


def write_trip_table(records, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(pa.Table.from_pylist(records, schema=TRIP_SCHEMA), tmp)
    tmp.replace(path)


def self_test():
    """Verify segmentation on a synthetic vehicle stream."""
    base = datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc)

    def make_row(minute, trip_id, route_id, variant, seq, far=False):
        ts = base.replace(minute=minute) if minute < 60 else base.replace(
            hour=base.hour + minute // 60, minute=minute % 60
        )
        return {
            "observation_timestamp": ts,
            "vehicle_id": "bus-1",
            "trip_id": trip_id,
            "route_id": route_id,
            "route_variant_label": variant,
            "direction_id": None,
            "nearest_stop_sequence": seq,
            "is_far_from_route": far,
            "is_sequence_backstep": False,
            "entity_id": "e1",
        }

    # Segment 1: normal trip, seq 0-10
    rows = [make_row(m, "trip_A", "R1", "UP", m) for m in range(0, 11)]
    # Segment 2: trip_id changes (trip_B), continues from seq 0
    rows += [make_row(m, "trip_B", "R1", "UP", m - 11) for m in range(11, 21)]
    # Segment 3: long gap (>30 min), same trip_id but gap triggers split
    rows += [make_row(m, "trip_B", "R1", "UP", m - 55) for m in range(55, 60)]
    # Segment 4: route change
    rows += [make_row(m, "trip_C", "R2", "DOWN", m - 60) for m in range(60, 65)]

    expected_stops_by_route = {"R1": 48, "R2": 30}

    segments = segment_vehicle_stream(rows, expected_stops_by_route, LONG_GAP_SECONDS)
    assert len(segments) == 4, f"expected 4 segments, got {len(segments)}"
    assert segments[0][1] == "start"
    assert segments[1][1] == "trip_id_change"
    assert segments[2][1] == "long_gap"
    assert segments[3][1] == "route_change"
    assert len(segments[0][0]) == 11
    assert len(segments[1][0]) == 10
    assert len(segments[2][0]) == 5
    assert len(segments[3][0]) == 5

    # Test terminal reset: prev seq near end (45 out of 48), next seq near start (1)
    reset_rows = [
        make_row(0, "trip_X", "R1", "UP", 44),
        make_row(1, "trip_X", "R1", "UP", 45),
        make_row(2, "trip_Y", "R1", "UP", 1),
        make_row(3, "trip_Y", "R1", "UP", 3),
    ]
    reset_segments = segment_vehicle_stream(reset_rows, expected_stops_by_route, LONG_GAP_SECONDS)
    assert len(reset_segments) == 2, f"expected 2 reset segments, got {len(reset_segments)}"
    assert reset_segments[1][1] == "terminal_reset"

    # Test full reconstruction pipeline
    trip_records, obs_map = reconstruct_trips(rows, expected_stops_by_route, LONG_GAP_SECONDS)
    assert len(trip_records) == 4
    assert all(r["reconstructed_trip_id"].startswith("rt_") for r in trip_records)
    assert trip_records[0]["trip_quality"] in ("complete", "partial", "low", "fragment")
    assert trip_records[0]["vehicle_id"] == "bus-1"
    # Observation map should cover all rows
    assert len(obs_map) == len(rows)

    # Quality classification, including the boundary values of each threshold
    assert classify_quality(0.83, 120, 0, 40) == "complete"
    assert classify_quality(COMPLETE_COMPLETION_RATIO, 120, 0, 40) == "complete"
    # a hidden gap over the limit demotes an otherwise complete trip
    assert classify_quality(0.83, COMPLETE_MAXIMUM_GAP_SECONDS + 1, 0, 40) == "partial"
    # a sequence violation also demotes it
    assert classify_quality(0.83, 120, 1, 40) == "partial"
    assert classify_quality(0.42, 120, 1, 20) == "partial"
    assert classify_quality(0.42, 120, PARTIAL_MAXIMUM_VIOLATIONS + 1, 20) == "low"
    assert classify_quality(0.10, 120, 0, 20) == "low"
    assert classify_quality(None, 120, 0, 20) == "low"
    # observation count outranks coverage
    assert classify_quality(0.83, 60, 0, MINIMUM_TRIP_OBSERVATIONS - 1) == "fragment"

    print("self-test passed")


def process(progress_path, schedule_path, output_path, arrivals_path, linked_arrivals_path, gap_threshold_s):
    rows = load_progress(progress_path)
    expected_stops_by_route = load_route_terminals(schedule_path)
    trip_records, obs_map = reconstruct_trips(rows, expected_stops_by_route, gap_threshold_s)
    write_trip_table(trip_records, output_path)

    # Summary
    quality_counts = defaultdict(int)
    for r in trip_records:
        quality_counts[r["trip_quality"]] += 1
    split_counts = defaultdict(int)
    for r in trip_records:
        split_counts[r["split_reason"]] += 1

    print(f"reconstructed {len(trip_records):,} trips from {len(rows):,} observations")
    print(f"  quality: {dict(sorted(quality_counts.items()))}")
    print(f"  split reasons: {dict(sorted(split_counts.items()))}")
    print(f"wrote trip table to {output_path}")

    # Link arrivals if the file exists
    if Path(arrivals_path).exists():
        total, linked = link_arrivals(arrivals_path, obs_map, linked_arrivals_path)
        print(f"linked {linked:,} of {total:,} arrivals to reconstructed trips")
        print(f"wrote linked arrivals to {linked_arrivals_path}")


def main():
    parser = argparse.ArgumentParser(description="Reconstruct bus trips from vehicle streams")
    parser.add_argument("--progress", default="data/processed/route_progress.parquet")
    parser.add_argument("--schedule", default="data/interim/selected_route_schedule.csv")
    parser.add_argument("--output", default="data/processed/reconstructed_trips.parquet")
    parser.add_argument("--arrivals", default="data/processed/stop_arrivals.parquet")
    parser.add_argument("--linked-arrivals", default="data/processed/stop_arrivals_linked.parquet")
    parser.add_argument("--gap-threshold", type=float, default=LONG_GAP_SECONDS)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--inspect", type=int, default=0, help="print N sample trips")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    try:
        process(
            args.progress,
            args.schedule,
            args.output,
            args.arrivals,
            args.linked_arrivals,
            args.gap_threshold,
        )
        if args.inspect > 0:
            inspect_trips(args.output, args.inspect)
    except (OSError, ValueError, pa.ArrowException) as error:
        parser.error(str(error))
    return 0


def inspect_trips(path, count):
    """Print a sample of reconstructed trips for manual review."""
    table = pq.read_table(path)
    records = table.to_pylist()
    # Pick a mix: some complete, some partial, some fragments
    by_quality = defaultdict(list)
    for r in records:
        by_quality[r["trip_quality"]].append(r)

    selected = []
    chosen_ids = set()
    per_quality = max(1, count // len(by_quality)) if by_quality else count
    for quality in ("complete", "partial", "low", "fragment"):
        for record in by_quality.get(quality, [])[:per_quality]:
            selected.append(record)
            chosen_ids.add(record["reconstructed_trip_id"])

    # Fill any remaining slots from whatever is left, in order
    for record in records:
        if len(selected) >= count:
            break
        if record["reconstructed_trip_id"] not in chosen_ids:
            selected.append(record)
            chosen_ids.add(record["reconstructed_trip_id"])

    print(f"\n--- inspecting {len(selected)} reconstructed trips ---")
    for r in selected[:count]:
        print(
            f"  {r['reconstructed_trip_id']:>10} | "
            f"vehicle={r['vehicle_id']:>12} | "
            f"route={r['route_id']:>5}/{r['route_variant_label']:>8} | "
            f"trip_id={r['original_trip_id']:>15} | "
            f"{str(r['start_time'])[:19]} to {str(r['end_time'])[:19]} | "
            f"obs={r['observation_count']:>4} | "
            f"stops={r['observed_stop_count']:>3}/{r['expected_stop_count'] or '?':>3} | "
            f"ratio={r['completion_ratio'] or 0:.2f} | "
            f"max_gap={r['maximum_sampling_gap_seconds']:>6.0f}s | "
            f"violations={r['sequence_violation_count']} | "
            f"quality={r['trip_quality']:>8} | "
            f"split={r['split_reason']}"
        )


if __name__ == "__main__":
    raise SystemExit(main())
