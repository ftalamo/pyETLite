"""Example: pipeline with a cron schedule.

Run the scheduler with:
    pyetlite schedule examples/scheduled_pipeline.py

Or with verbose logs:
    pyetlite schedule examples/scheduled_pipeline.py --verbose

The pipeline below runs every day at 03:00 UTC.
"""
import polars as pl

from pyetlite import Pipeline, ErrorMode
from pyetlite.core.base import BaseSource, BaseSink
from pyetlite.transforms import DropNulls, FilterRows


# In a real project you'd use CSVSource / PostgresSource etc.
class FakeSource(BaseSource):
    def read(self) -> pl.DataFrame:
        return pl.DataFrame({"id": [1, 2, 3], "amount": [100.0, None, 300.0]})


class FakeSink(BaseSink):
    def write(self, df: pl.DataFrame) -> None:
        print(f"[sink] Writing {len(df)} rows...")


pipeline = (
    Pipeline(
        name="daily_cleanup",
        schedule="0 3 * * *",        # every day at 03:00 UTC
        error_mode=ErrorMode.SKIP_AND_LOG,
    )
    .extract(FakeSource())
    .transform(DropNulls())
    .transform(FilterRows(pl.col("amount") > 0))
    .load(FakeSink())
)
