from __future__ import annotations
from pathlib import Path
from typing import List, Dict
import pendulum
import pandas as pd
from airflow.decorators import dag, task

@dag(
    schedule=None, # run on demand
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "titanic", "taskflow"],
    description="ETL with TaskFlow API: Extract Titanic CSV, Transform, Load cleaned CSV",
)

def titanic_etl():
    @task
    def extract() -> str:
        """
        Read titanic.csv (must be available on worker) and write to a temporary
        file path that will be returned (small XCom payload).
        Preferably replace tmp_path with a shared storage path (S3/GCS/NFS).
        """
        base_dir = Path(__file__).resolve().parent
        csv_path = base_dir / "titanic.csv"

        # Prefer to raise early if file not present on worker:
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found at {csv_path} on worker")
        
        df = pd.read_csv(csv_path)
        # Write to a worker-accessible location. If you have network storage,
        # write there instead of /tmp to make files accessible across workers.
        out_path = Path("/tmp") / f"titanic_extract_{pendulum.now('UTC').to_iso8601_string()}.csv"
        df.to_csv(out_path, index=False)
        return str(out_path)
    
    @task
    def transform(extracted_csv_path: str) -> str:
        """
        Read CSV from path, transform, write cleaned CSV, return cleaned path.
        """
        df = pd.read_csv(extracted_csv_path)

        required_cols = ["Name", "Sex", "Age", "Survived"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required column(s): {missing}")

        df = df[required_cols].copy()
        df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
        mean_age = df["Age"].mean()
        df["Age"] = df["Age"].fillna(round(mean_age, 1) if pd.notna(mean_age) else 0)
        df["Survived"] = pd.to_numeric(df["Survived"], errors="coerce").map({0: "No", 1: "Yes"})
        df["Survived"] = df["Survived"].fillna("No")

        cleaned_path = Path("/tmp") / f"titanic_clean_{pendulum.now('UTC').to_iso8601_string()}.csv"
        df.to_csv(cleaned_path, index=False)
        return str(cleaned_path)
    
    @task
    def load(cleaned_csv_path: str) -> str:
        """
        Final load: move cleaned CSV to DAG folder (or upload to DB/S3).
        Return final destination path.
        """
        final_path = Path(__file__).resolve().parent / "titanic_clean.csv"
        # Overwrite existing
        df = pd.read_csv(cleaned_csv_path)
        df.to_csv(final_path, index=False)
        return str(final_path)
    
    cleaned_path = load(transform(extract()))

titanic_etl()