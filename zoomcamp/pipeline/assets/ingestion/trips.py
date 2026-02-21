"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default


materialization:
  type: table
  strategy: append


@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    import os
    import json
    from datetime import datetime
    from io import BytesIO

    import pandas as pd
    import requests

    BRUIN_START_DATE = os.environ.get("BRUIN_START_DATE")
    BRUIN_END_DATE = os.environ.get("BRUIN_END_DATE")
    BRUIN_VARS = os.environ.get("BRUIN_VARS")

    # Default taxi types if not provided via pipeline variables
    taxi_types = ["yellow"]
    if BRUIN_VARS:
      try:
        vars_json = json.loads(BRUIN_VARS)
        taxi_types = vars_json.get("taxi_types", taxi_types)
      except Exception:
        # malformed BRUIN_VARS -> fall back to default
        pass

    # Validate dates
    if not BRUIN_START_DATE or not BRUIN_END_DATE:
      raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be set for this ingestion")

    start = pd.to_datetime(BRUIN_START_DATE)
    end = pd.to_datetime(BRUIN_END_DATE)

    # generate month starts between start (inclusive) and end (exclusive)
    months = pd.date_range(start=start, end=end, freq="MS")

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    frames = []

    for taxi in taxi_types:
      for dt in months:
        year = dt.year
        month = dt.month
        filename = f"{taxi}_tripdata_{year}-{month:02}.parquet"
        url = base_url + filename
        try:
          resp = requests.get(url, stream=True, timeout=60)
          resp.raise_for_status()
          bio = BytesIO(resp.content)
          # read parquet into pandas DataFrame (requires pyarrow)
          df = pd.read_parquet(bio, engine="pyarrow")
          df["_extracted_at"] = datetime.utcnow().isoformat()
          df["_source_file"] = filename

          # Normalize timezone-aware datetimes to naive UTC to avoid
          # pyarrow timezone DB errors during downstream normalization.
          for col in df.columns:
            try:
              if pd.api.types.is_datetime64_any_dtype(df[col].dtype):
                # 1. Convert to UTC 
                # 2. Floor to microseconds (us) to avoid the "lose data" error
                # 3. Make it naive (None) to keep it simple for DuckDB
                df[col] = pd.to_datetime(df[col], utc=True).dt.floor("us").dt.tz_localize(None)
            except Exception:
              pass

          frames.append(df)
        except Exception:
          # best effort: skip missing or transient failures
          continue

    if not frames:
      # return empty DataFrame with no columns
      return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    return result


