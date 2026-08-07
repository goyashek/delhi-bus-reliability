# Delhi Bus Reliability Intelligence

I am building a geospatial machine learning project around Delhi's official static GTFS and live bus positions. The main question is whether GPS snapshots can show where Delhi buses become unreliable and support useful travel-time and bunching predictions.

The student version will reconstruct journeys and stop arrivals, measure headways and excess waiting, train leakage-safe travel-time models, estimate prediction uncertainty, and show the results in a small Streamlit dashboard. It covers two route families rather than the full Delhi network. Bunching classification will be included only if the collected data contains enough independent events.

## Current state

The pipeline covers 9,444 raw protobuf snapshots from July 30 through August 7, which is 98.1% of the expected one-minute slots across nine collection dates. Those snapshots produce 102,016 selected observations, feed-quality flags, official route geometry, 11,409 inferred stop passages, and 1,763 reconstructed trips. The arrival-coverage check found usable passages at 192 of the 193 static stops on the selected routes. Reliability analysis, travel-time modelling, and the dashboard are the remaining project stages.

Collection is still running, so the date count grows. The chronological modelling split needs at least ten usable dates, and two of the nine current dates are short partial days, so the final metrics stay provisional until more full days are collected.

## Setup and local collection

Create the environment and install the collector dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Copy your private OTD key into `.env`, then test and run the collector:

```bash
.venv/bin/python collect.py --self-test
.venv/bin/python collect.py
.venv/bin/python collect.py --interval 30
```

The collector writes one append-only CSV per UTC day under `data/raw/vehicle_positions/`. Stop the continuous collector with `Ctrl+C`.

The official static GTFS ZIP is stored under `data/external/delhi_buses_static_gtfs/`. Its `feed_info.txt` identifies Delhi Transport Corporation and Delhi Integrated Multi-Modal Transit System Ltd., version `v28`, with feed dates from 2025-01-01 through 2040-01-01. The accompanying PDFs describe the Delhi Transport Stack registration and download flow. Their summary counts are older than the ZIP, so the actual GTFS files are the source of truth.

## Process archived snapshots

After the R2 backup has downloaded raw protobuf snapshots, prepare the selected routes:

```bash
.venv/bin/python prepare_positions.py --self-test
.venv/bin/python prepare_positions.py
```

The script reads route IDs from `data/interim/selected_route_schedule.csv`. It writes one UTC partition per day under `data/processed/vehicle_positions/`, with `positions.parquet` for selected observations and `snapshots.parquet` for request and feed metadata. Each position keeps the collection time and feed time, then uses the feed time as `observation_timestamp` when it exists and the collection time otherwise. Archived responses are marked `2xx` because the Worker saves only successful, non-empty responses. `data/processed/collection_health.csv` records collection coverage, empty feeds, parse errors, feed lag, and missing intervals.

## Audit feed quality

Run the quality audit after processing new snapshots:

```bash
.venv/bin/python audit_feed.py --self-test
.venv/bin/python audit_feed.py
```

The audit writes row-level flags to `data/processed/feed_quality_flags.parquet` and route totals to `data/processed/feed_quality.csv`. It keeps every observation. It marks repeated stream timestamps as stale, exact repeated points as duplicates, missing or out-of-range coordinates as invalid, movements over 2 km as large jumps, implied speeds over 120 km/h as implausible, and missing trip IDs separately. Large jumps are review flags because long collection gaps can produce a large but plausible displacement.

## Build route progress

Build the static stop geometry and compare it with current GPS tracks:

```bash
.venv/bin/python route_progress.py --self-test
.venv/bin/python route_progress.py
```

The script writes cumulative stop geometry, nearest-stop assignments, route progress, and a trajectory plot. It labels each stream with the official `route_variant_label` from the static schedule. `direction_id` remains nullable because the selected official trips do not provide it. Across the current archive, median nearest-stop distances are about 160 m for route `1411`, 149 m for `1788`, 261 m for `1881`, and 125 m for `32`. The over-1.5 km flags are 4.68%, 0.48%, 17.11%, and 13.91%; the flags remain visible for later cleaning.

## Infer stop passages

Infer arrivals from the official route-progress file:

```bash
.venv/bin/python infer_arrivals.py --self-test
.venv/bin/python infer_arrivals.py
```

The script excludes observations flagged as more than 1.5 km from their reference stops, keeps chronological stop movement within each vehicle and trip stream, and chooses the closest observation for each stop passage. It writes `data/processed/stop_arrivals.parquet` at a 50 m radius and compares 25 m, 40 m, 50 m, and 75 m in `data/processed/stop_arrival_sensitivity.csv`. `route_variant_label` comes from the official static schedule, while `direction_id` stays nullable because the live feed has no direction field and the selected trips do not provide one. Arrival confidence uses distance, the time gap around the selected observation, and the availability of observations before and after it.

The 50 m run produced 11,409 passages: 1,608 high confidence, 8,562 medium confidence, and 1,239 low confidence. The radius sensitivity produced 6,071 passages at 25 m, 9,440 at 40 m, and 15,556 at 75 m. The 50 m setting keeps the wider 75 m geofence from adding too many weak matches while retaining more passages than 25 m or 40 m. The run excluded 9,492 far-route observations.

Check coverage and sampling limits in `notebooks/02_arrival_coverage.ipynb`. The 50 m table spans all nine collection dates and 241 vehicles. The largest vehicle share is 2.32% overall, although route `1881` has only 17 contributing vehicles and should be treated as a narrower sample. Widening to 75 m adds 4,147 passages, 90.4% of them medium confidence and 9.6% low confidence, so I kept 50 m and did not add crossing-time interpolation. The inferred time is still the timestamp of a sampled GPS observation, not the exact stop-crossing time, and the one stop with no passages remains visible in the coverage table.

## Reconstruct trips

Group the inferred passages into journeys:

```bash
.venv/bin/python reconstruct_trips.py --self-test
.venv/bin/python reconstruct_trips.py
```

The script sorts each vehicle's observations by `observation_timestamp` and cuts the stream into trip segments on four signals: a change in the live `trip_id`, a change of route variant, a sampling gap over 30 minutes, and a stop-sequence reset from the end of the route back to the start. The live `trip_id` is used only as a segmentation hint, not as a join key into the static schedule, because its values do not match the official trip IDs.

Each segment gets a `reconstructed_trip_id` plus the observation count, distinct stops seen, expected stop count for the route, completion ratio, largest internal sampling gap, and a count of stop-sequence backsteps. A quality label summarises those numbers: `complete` covers at least 70% of the route stops with no backsteps and no internal gap over 10 minutes, `partial` covers at least 40% with at most two backsteps, `fragment` has fewer than three observations, and `low` is everything else. Those cutoffs are judgement calls tuned against the current archive rather than measured constants, so they are named at the top of the script.

The run produced 1,763 trips from 102,016 observations: 288 complete, 519 partial, 780 low, and 176 fragments. The segmentation fired 898 times on a `trip_id` change, 586 times on a terminal reset, and 21 times on a long gap, with 258 segments starting a vehicle's record. Median durations for complete trips are 152 minutes on `107UP` (73 stops), 115 on `392DOWN`, 106 on `274UP`, and 64 on `ML06UP` (26 stops). `data/processed/stop_arrivals_linked.parquet` repeats the arrival table with the owning `reconstructed_trip_id` attached; all 11,409 passages linked.

The large `low` bucket is mostly buses idling at a terminal after finishing a journey, where the feed issues a fresh `trip_id` while the vehicle barely moves. Checking consecutive segments, only 32 of the 898 `trip_id` splits leave under five minutes between one segment ending and the next beginning, so the rule is not breaking many real journeys into pieces. Passages inside complete or partial trips account for 9,595 of 11,409, or 84.1%.

## Cloud collection

The Cloudflare Worker under `cloudflare/` saves one raw protobuf snapshot per minute to a private R2 bucket. From that directory:

```bash
# Enable R2 under Storage & databases in the Cloudflare dashboard first.
npx wrangler@latest login
npx wrangler@latest r2 bucket create delhi-bus-vehicle-positions
npx wrangler@latest r2 bucket lifecycle add delhi-bus-vehicle-positions delete-after-10-days --expire-days 10 --force
npx wrangler@latest secret put OTD_API_KEY
npx wrangler@latest deploy
```

The 10-day expiry keeps the bucket inside R2's free storage allowance at the current feed size. Copy the bucket to local storage at least once a week before Cloudflare deletes older snapshots.

For automatic local backups, create a read-only R2 API token scoped to `delhi-bus-vehicle-positions`, then add its access key and secret to `.env` using the names in `.env.example`. The included macOS job runs every six hours while the machine is available. Its runtime files live under the user Application Support folder because macOS blocks background jobs from opening scripts inside `Documents`.

## Data and secrets

API keys belong in `.env` and must never be committed. Raw transit snapshots and generated artifacts also stay out of Git. Scheduled times in `stop_times.txt` are rough estimates, so the project will not treat them as observed arrivals.
