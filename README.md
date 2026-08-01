# Delhi Bus Reliability Intelligence

I am building a geospatial machine learning project around Delhi's official static GTFS and live bus positions. The main question is whether GPS snapshots can show where Delhi buses become unreliable and support useful travel-time and bunching predictions.

The first version will reconstruct journeys and stop arrivals, measure headways and excess waiting, train leakage-safe travel-time and bunching models, estimate prediction uncertainty, and show the results in a small Streamlit dashboard. It covers two or three routes rather than the full Delhi network.

## Day 1 setup

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

Download the official static GTFS ZIP from the [Delhi Open Transit Data page](https://otd.delhi.gov.in/data/static/) and extract it under `data/external/gtfs_static/`.

## Day 3 processing

After the R2 backup has downloaded raw protobuf snapshots, prepare the selected routes:

```bash
.venv/bin/python prepare_positions.py --self-test
.venv/bin/python prepare_positions.py
```

The script reads route IDs from `data/interim/selected_route_schedule.csv`. It writes one UTC partition per day under `data/processed/vehicle_positions/`, with `positions.parquet` for selected observations and `snapshots.parquet` for request and feed metadata. Archived responses are marked `2xx` because the Worker saves only successful, non-empty responses. `data/processed/collection_health.csv` records collection coverage, empty feeds, parse errors, feed lag, and missing intervals.

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
