import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

ALLOWED_STATUSES = {
    "placed",
    "shipped",
    "completed",
    "return_pending",
    "returned",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Clean raw orders data (Bronze) and output a Silver CSV filtered by execution date."
        )
    )
    parser.add_argument(
        "--execution-date",
        dest="execution_date",
        required=True,
        help="Execution date in YYYY-MM-DD format (usually passed from Airflow's {{ ds }}).",
    )
    parser.add_argument(
        "--raw-path",
        dest="raw_path",
        default="data/raw/raw_orders.csv",
        help="Path to the Bronze CSV file (default: data/raw/raw_orders.csv).",
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        default="data/silver",
        help="Directory where the cleaned CSV will be stored (default: data/silver).",
    )
    return parser.parse_args()


def clean_orders(raw_path: Path, execution_date: datetime, output_dir: Path) -> Path:
    df = pd.read_csv(raw_path)

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date
    df = df.dropna(subset=["order_date", "status", "id", "user_id"])
    df["status"] = df["status"].str.strip().str.lower()
    df = df[df["status"].isin(ALLOWED_STATUSES)]
    df = df.drop_duplicates(subset=["id"]).sort_values("order_date")

    filtered = df[df["order_date"] == execution_date.date()]

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"orders_clean_{execution_date.strftime('%Y-%m-%d')}.csv"
    filtered.to_csv(output_path, index=False)
    return output_path


def main() -> None:
    args = parse_args()
    exec_dt = datetime.strptime(args.execution_date, "%Y-%m-%d")
    output_path = clean_orders(
        raw_path=Path(args.raw_path),
        execution_date=exec_dt,
        output_dir=Path(args.output_dir),
    )
    print(
        f"Wrote {output_path} with cleaned orders for {exec_dt.date()}"
    )


if __name__ == "__main__":
    main()
