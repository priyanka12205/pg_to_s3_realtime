# etl/etl_s3.py
import os, sys, time, tempfile, shutil
from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(os.path.dirname(BASE_DIR), "state")
os.makedirs(STATE_DIR, exist_ok=True)
WATERMARK_FILE = os.path.join(STATE_DIR, "last_watermark.txt")

def read_env():
    load_dotenv(os.path.join(BASE_DIR, ".env"))
    cfg = {
        "pg_host": os.getenv("PG_HOST", "localhost"),
        "pg_port": int(os.getenv("PG_PORT", "5432")),
        "pg_db": os.getenv("PG_DB", "postgres"),
        "pg_user": os.getenv("PG_USER", "postgres"),
        "pg_password": os.getenv("PG_PASSWORD", "postgres"),
        "aws_region": os.getenv("AWS_REGION", "us-east-1"),
        "s3_bucket": os.getenv("S3_BUCKET"),
        "s3_prefix": os.getenv("S3_PREFIX", "raw").strip("/"),
        "table_name": os.getenv("TABLE_NAME", "public.orders"),
        "watermark_col": os.getenv("WATERMARK_COLUMN", "updated_at"),
        "usd_to_inr": float(os.getenv("USD_TO_INR", "83.0")),
    }
    if not cfg["s3_bucket"]:
        raise RuntimeError("S3_BUCKET must be set in .env")
    return cfg

def get_last_watermark():
    if os.path.exists(WATERMARK_FILE):
        v = open(WATERMARK_FILE, "r").read().strip()
        if v:
            return v
    # default: 1970 for first run
    return "1970-01-01 00:00:00"

def save_watermark(ts_str):
    open(WATERMARK_FILE, "w").write(ts_str)

def fetch_new_rows(conn, table, ts_col, last_ts):
    q = f"""
        SELECT *, {ts_col} AS _wm_col
        FROM {table}
        WHERE {ts_col} > %s
        ORDER BY {ts_col} ASC
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, (last_ts,))
        rows = cur.fetchall()
        return rows

def transform(df, usd_to_inr):
    if df.empty:
        return df

    # Standardize datetime columns to ISO strings (UTC naive for demo)
    for col in df.columns:
        if "ts" in col or "time" in col or "date" in col or col == "_wm_col":
            try:
                df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass

    # Light clean up
    df = df.dropna(subset=["amount_usd"])

    # Add derived fields
    df["amount_inr"] = (df["amount_usd"].astype(float) * usd_to_inr).round(2)

    # Partition columns (by order date)
    if "order_ts" in df.columns:
        dt = pd.to_datetime(df["order_ts"])
    else:
        dt = pd.to_datetime(df["_wm_col"])
    df["year"] = dt.dt.year.astype(int)
    df["month"] = dt.dt.month.astype(int)
    df["day"] = dt.dt.day.astype(int)
    return df

def write_partitioned_parquet_and_upload(df, bucket, prefix, s3_client):
    if df.empty:
        print("No new data to write.")
        return None

    tmpdir = tempfile.mkdtemp(prefix="pg_s3_")
    try:
        written_files = []
        # group by partitions and write one parquet per partition
        for (y, m, d), g in df.groupby(["year", "month", "day"]):
            # file name with run timestamp
            run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            fname = f"orders_{run_ts}.parquet"
            local_path = os.path.join(tmpdir, fname)
            g.drop(columns=["year", "month", "day"]).to_parquet(local_path, index=False, engine="pyarrow")

            s3_key = f"{prefix}/{y:04d}/{m:02d}/{d:02d}/{fname}"
            s3_client.upload_file(local_path, bucket, s3_key)
            written_files.append((bucket, s3_key))
            print(f"Uploaded s3://{bucket}/{s3_key}")
        return written_files
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

def main():
    cfg = read_env()
    last_wm = get_last_watermark()
    print(f"Using watermark > {last_wm}")

    conn = psycopg2.connect(
        host=cfg["pg_host"], port=cfg["pg_port"], dbname=cfg["pg_db"],
        user=cfg["pg_user"], password=cfg["pg_password"]
    )
    try:
        rows = fetch_new_rows(conn, cfg["table_name"], cfg["watermark_col"], last_wm)
        if not rows:
            print("No new/updated rows.")
            return

        df = pd.DataFrame(rows)
        df = transform(df, cfg["usd_to_inr"])

        s3 = boto3.client("s3", region_name=cfg["aws_region"])
        uploaded = write_partitioned_parquet_and_upload(df, cfg["s3_bucket"], cfg["s3_prefix"], s3)

        # Advance watermark to max seen
        max_wm = pd.to_datetime(df["_wm_col"]).max().strftime("%Y-%m-%d %H:%M:%S")
        save_watermark(max_wm)
        print(f"Advanced watermark to {max_wm}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()