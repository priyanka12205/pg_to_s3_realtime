# Real-Time-ish ETL: PostgreSQL → Pandas → Parquet → S3

## What you get
- SQL to create and seed a demo `orders` table with an `updated_at` column
- A Python ETL (`etl/etl_s3.py`) that:
  - Pulls only new/updated rows since the last run (watermark on `updated_at`)
  - Does tiny transforms (standardize dates, filter nulls, add `amount_inr`)
  - Writes Parquet locally, uploads to S3 under `s3://<bucket>/<prefix>/YYYY/MM/DD/file.parquet`
  - Advances watermark (stored in `state/last_watermark.txt`)
- Optional Airflow DAG to run the ETL every 5 minutes

## Quickstart (Windows PowerShell)
```powershell
# 1) Unzip this folder
cd pg_to_s3_realtime

# 2) Install Python deps
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r etl\requirements.txt

# 3) Create Postgres table & sample data (adjust connection flags as needed)
psql -h localhost -U postgres -d postgres -f sql/create_tables.sql
psql -h localhost -U postgres -d postgres -f sql/seed_orders.sql

# 4) Configure AWS & .env
aws configure   # or rely on EC2 instance role
copy etl\.env.example etl\.env
notepad etl\.env  # fill S3_BUCKET etc.

# 5) Run ETL
python etl/etl_s3.py
```

## Quickstart (Ubuntu / macOS)
```bash
cd pg_to_s3_realtime
python3 -m venv venv
source venv/bin/activate
pip install -r etl/requirements.txt

psql -h localhost -U postgres -d postgres -f sql/create_tables.sql
psql -h localhost -U postgres -d postgres -f sql/seed_orders.sql

aws configure  # or use instance profile
cp etl/.env.example etl/.env && nano etl/.env

python etl/etl_s3.py
```

## S3 layout
```
s3://<your-bucket>/<prefix>/YYYY/MM/DD/orders_YYYYMMDDThhmmss.parquet
```

## Re-running
Each run pulls rows with `updated_at > last_watermark`. To test, update a row:
```sql
UPDATE public.orders SET status='DELIVERED' WHERE id=2;
```
Then run `python etl/etl_s3.py` again and a new Parquet will land in today's partition.

## Airflow (optional)
- Put `airflow/dag_postgres_to_s3.py` into your `$AIRFLOW_HOME/dags/`
- Ensure the repo path is on PYTHONPATH or tweak imports
- Start the scheduler; the DAG runs every 5 minutes

## Notes
- This is a simple polling CDC. For true real-time you could use Debezium or
  logical decoding → Kafka → Spark/NiFi — but this keeps things beginner-friendly.