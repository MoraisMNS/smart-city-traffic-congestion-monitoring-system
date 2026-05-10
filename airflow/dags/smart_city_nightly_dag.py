"""
Airflow DAG — Nightly Traffic Analysis & Intervention Report
=============================================================
Schedule : Daily at 23:55 (just before midnight)
Purpose  :
  1. Aggregate the day's congestion_index records from Postgres.
  2. Identify "Peak Traffic Hour" per junction (hour with highest avg vehicle count).
  3. Identify which junction needs physical traffic police intervention tomorrow.
  4. Write a structured report (CSV + plain-text summary) to /reports/.

DAG ID   : smart_city_nightly_report
Owner    : abda_team
"""

from __future__ import annotations

import os
import csv
import logging
from datetime import datetime, timedelta, date
from pathlib import Path

import psycopg2
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

# ── Configuration ──────────────────────────────────────────────────────────────
PG_CONN = {
    "host":     os.getenv("PG_HOST",   "postgres"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",     "smartcity"),
    "user":     os.getenv("PG_USER",   "admin"),
    "password": os.getenv("PG_PASS",   "admin123"),
}
REPORT_DIR = Path(os.getenv("REPORT_DIR", "/opt/airflow/reports"))
REPORT_DIR.mkdir(parents=True, exist_ok=True)

POLICE_CI_THRESHOLD = 60.0   # Junctions with CI >= this need police presence
JUNCTION_CAPACITY   = 100

logger = logging.getLogger("smart_city_dag")

# ── Default Args ───────────────────────────────────────────────────────────────
default_args = {
    "owner":            "abda_team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── Task Functions ─────────────────────────────────────────────────────────────

def fetch_daily_data(**context) -> None:
    """Pull today's congestion records from Postgres, push to XCom as JSON."""
    target_date = context["ds"]  # e.g. "2025-07-15"
    logger.info(f"Fetching congestion data for date: {target_date}")

    conn = psycopg2.connect(**PG_CONN)
    try:
        query = """
            SELECT
                sensor_id,
                junction_name,
                DATE_TRUNC('hour', window_start)  AS hour_slot,
                AVG(mean_speed_kmph)              AS avg_speed,
                SUM(total_vehicles)               AS total_vehicles,
                AVG(congestion_index)             AS avg_ci
            FROM congestion_index
            WHERE DATE(window_start) = %s
            GROUP BY sensor_id, junction_name, hour_slot
            ORDER BY sensor_id, hour_slot;
        """
        df = pd.read_sql(query, conn, params=(target_date,))
    finally:
        conn.close()

    if df.empty:
        logger.warning("No data found for today. Using empty dataset.")

    logger.info(f"Fetched {len(df)} hourly records for {len(df['sensor_id'].unique())} junctions.")
    context["ti"].xcom_push(key="daily_df", value=df.to_json())


def compute_peak_hours(**context) -> None:
    """Identify peak traffic hour per junction and flag police interventions."""
    import json

    daily_json = context["ti"].xcom_pull(key="daily_df", task_ids="fetch_daily_data")
    df = pd.read_json(daily_json)

    if df.empty:
        logger.warning("Empty dataframe — skipping peak hour computation.")
        context["ti"].xcom_push(key="peak_df", value="[]")
        context["ti"].xcom_push(key="intervention_df", value="[]")
        return

    df["hour_slot"] = pd.to_datetime(df["hour_slot"])
    df["hour"]      = df["hour_slot"].dt.hour

    # Peak hour = hour with highest total vehicle count per junction
    peak_df = (
        df.sort_values("total_vehicles", ascending=False)
          .groupby("sensor_id")
          .first()
          .reset_index()
    )[["sensor_id", "junction_name", "hour", "total_vehicles", "avg_speed", "avg_ci"]]

    peak_df.rename(columns={"hour": "peak_hour"}, inplace=True)
    peak_df["peak_hour_label"] = peak_df["peak_hour"].apply(
        lambda h: f"{h:02d}:00–{(h+1)%24:02d}:00"
    )

    # Intervention flag: CI >= threshold OR avg_speed < 15 km/h at peak
    intervention_df = peak_df[
        (peak_df["avg_ci"] >= POLICE_CI_THRESHOLD) |
        (peak_df["avg_speed"] < 15)
    ].copy()
    intervention_df["reason"] = intervention_df.apply(
        lambda r: (
            f"CI={r['avg_ci']:.1f} (≥{POLICE_CI_THRESHOLD})"
            if r["avg_ci"] >= POLICE_CI_THRESHOLD
            else f"Avg speed {r['avg_speed']:.1f} km/h (<15)"
        ),
        axis=1,
    )

    logger.info(f"Peak hours computed for {len(peak_df)} junctions.")
    logger.info(f"Intervention required at {len(intervention_df)} junction(s).")

    context["ti"].xcom_push(key="peak_df",         value=peak_df.to_json())
    context["ti"].xcom_push(key="intervention_df", value=intervention_df.to_json())


def generate_report(**context) -> None:
    """Write CSV + human-readable text report to /reports/ directory."""
    import json

    target_date  = context["ds"]
    peak_json    = context["ti"].xcom_pull(key="peak_df",         task_ids="compute_peak_hours")
    inter_json   = context["ti"].xcom_pull(key="intervention_df", task_ids="compute_peak_hours")

    peak_df  = pd.read_json(peak_json)
    inter_df = pd.read_json(inter_json)

    # ── CSV Report ─────────────────────────────────────────────────────────────
    csv_path = REPORT_DIR / f"traffic_report_{target_date}.csv"
    if not peak_df.empty:
        peak_df.to_csv(csv_path, index=False)
        logger.info(f"CSV report written: {csv_path}")

    # ── Text Summary Report ────────────────────────────────────────────────────
    txt_path = REPORT_DIR / f"police_intervention_{target_date}.txt"
    with open(txt_path, "w") as f:
        f.write("=" * 65 + "\n")
        f.write(f"  SMART CITY TRAFFIC REPORT — {target_date}\n")
        f.write(f"  Generated by: Airflow DAG | smart_city_nightly_report\n")
        f.write("=" * 65 + "\n\n")

        f.write("PEAK TRAFFIC HOURS PER JUNCTION\n")
        f.write("-" * 65 + "\n")
        if peak_df.empty:
            f.write("  No data available for today.\n")
        else:
            for _, row in peak_df.iterrows():
                f.write(
                    f"  {row['sensor_id']} | {row['junction_name']}\n"
                    f"    Peak Hour  : {row['peak_hour_label']}\n"
                    f"    Vehicles   : {int(row['total_vehicles'])}\n"
                    f"    Avg Speed  : {row['avg_speed']:.1f} km/h\n"
                    f"    Cong. Index: {row['avg_ci']:.1f}\n\n"
                )

        f.write("\nPOLICE INTERVENTION RECOMMENDED TOMORROW\n")
        f.write("-" * 65 + "\n")
        if inter_df.empty:
            f.write("  ✓ No junctions require police intervention tomorrow.\n")
        else:
            for _, row in inter_df.iterrows():
                f.write(
                    f"  ⚠  {row['sensor_id']} — {row['junction_name']}\n"
                    f"      Peak Hour : {row['peak_hour_label']}\n"
                    f"      Reason    : {row['reason']}\n\n"
                )
        f.write("=" * 65 + "\n")
        f.write("  END OF REPORT\n")
        f.write("=" * 65 + "\n")

    logger.info(f"Text report written: {txt_path}")
    context["ti"].xcom_push(key="report_path", value=str(txt_path))


def log_summary(**context) -> None:
    """Print final summary to Airflow task logs."""
    txt_path = context["ti"].xcom_pull(key="report_path", task_ids="generate_report")
    if txt_path and Path(txt_path).exists():
        with open(txt_path) as f:
            logger.info("\n" + f.read())
    else:
        logger.info("No report file found.")


# ── DAG Definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="smart_city_nightly_report",
    default_args=default_args,
    description="Aggregate daily traffic data and generate police intervention report",
    schedule_interval="55 23 * * *",    # 23:55 every night
    start_date=days_ago(1),
    catchup=False,
    tags=["smart_city", "traffic", "batch"],
) as dag:

    t1_fetch = PythonOperator(
        task_id="fetch_daily_data",
        python_callable=fetch_daily_data,
    )

    t2_peak = PythonOperator(
        task_id="compute_peak_hours",
        python_callable=compute_peak_hours,
    )

    t3_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    t4_log = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
    )

    t1_fetch >> t2_peak >> t3_report >> t4_log