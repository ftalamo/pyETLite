"""Example pipeline: read a CSV, clean it, write to another CSV.

Run with:
    pyetlite run examples/csv_to_csv.py
    pyetlite run examples/csv_to_csv.py --dry-run
    pyetlite validate examples/csv_to_csv.py
    pyetlite list examples/csv_to_csv.py
"""
import polars as pl

from pyetlite import Pipeline, ErrorMode, transform
from pyetlite.sources import CSVSource
from pyetlite.sinks import CSVSink
from pyetlite.transforms import DropNulls, RenameColumns, FilterRows


# Optional: custom transform via decorator
@transform
def normalize_emails(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.col("email").str.to_lowercase())


pipeline = (
    Pipeline("orders_cleanup", error_mode=ErrorMode.SKIP_AND_LOG)
    .extract(CSVSource("examples/data/orders.csv"))
    .transform(DropNulls(subset=["id", "email"]))
    .transform(normalize_emails)
    .transform(RenameColumns({"id": "order_id"}))
    .transform(FilterRows(pl.col("amount") > 0))
    .load(CSVSink("examples/output/orders_clean.csv"))
)
