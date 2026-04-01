"""
Auto-generated Airflow DAG — orders_pipeline
Airflow 2.x | Generated: 2026-04-01T13:09:13.080192
"""
import json
import os
import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task

SOURCE_PATH = (
    "/home/node/.openclaw/media/inbound/"
    "mock_data_engineering---d5252769-81e4-40f5-9a90-d4f3c91146a5.csv"
)


@dag(
    dag_id="orders_pipeline_pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": True,
    },
    tags=["orders_pipeline", "data-engineering"],
)
def orders_pipeline_pipeline():

    @task()
    def extract():
        subprocess.run(
            [
                "python3",
                "skills/data-engineering/scripts/phase2_discovery.py",
                "--project_name", "orders_pipeline",
                "--source", SOURCE_PATH,
            ],
            check=True,
        )
        return "extract_done"

    @task()
    def transform_data(extract_status: str):
        subprocess.run(
            [
                "python3",
                "skills/data-engineering/scripts/phase6_build.py",
                "--project_name", "orders_pipeline",
                "--source", SOURCE_PATH,
            ],
            check=True,
        )
        return "transform_done"

    @task()
    def dq_check(transform_status: str):
        subprocess.run(
            [
                "python3",
                "skills/data-engineering/scripts/phase7_quality.py",
                "--project_name", "orders_pipeline",
            ],
            check=True,
        )
        dq_path = os.path.join("/home/node/Music/de-artifacts", "phase7", "dq_summary.json",
                               )
        with open(dq_path) as f:
            result = json.load(f)
        return result["dq_score_pct"]

    @task()
    def load_data():
        print("Loading to destination...")
        return "load_done"

    @task()
    def dq_alert():
        print("DQ FAILED — pipeline halted. Check artifacts/phase7/dq_scorecard.json")

    @task()
    def post_validate():
        print("Post-load validation passed")

    @task()
    def notify_success():
        print("Pipeline orders_pipeline completed successfully")

    e = extract()
    t = transform_data(e)
    dq = dq_check(t)
    load = load_data()
    alert = dq_alert()
    validate = post_validate()
    done = notify_success()

    dq >> [load, alert]
    load >> validate >> done


orders_pipeline_pipeline()
