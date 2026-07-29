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

## Data and secrets

API keys belong in `.env` and must never be committed. Raw transit snapshots and generated artifacts also stay out of Git. Scheduled times in `stop_times.txt` are rough estimates, so the project will not treat them as observed arrivals.
