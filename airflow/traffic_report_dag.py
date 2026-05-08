from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

def generate_report():

    path = "../data/processed"

    files = [f"{path}/{f}" for f in os.listdir(path)]

    df_list = []

    for file in files:
        try:
            df = pd.read_csv(file)
            df_list.append(df)
        except:
            pass

    if len(df_list) == 0:
        return

    final_df = pd.concat(df_list)

    report = final_df.groupby("sensor_id")["vehicle_count"].sum()

    report.to_csv("../reports/daily_report.csv")

with DAG(
    dag_id="traffic_report",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="generate_daily_report",
        python_callable=generate_report
    )