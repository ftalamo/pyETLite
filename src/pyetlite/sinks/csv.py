"""CSV sink connector."""
from pathlib import Path
from typing import Literal

import polars as pl

from pyetlite.core.base import BaseSink


class CSVSink(BaseSink):
    """Write a DataFrame to a CSV file.

    Args:
        path:      Destination file path.
        separator: Column delimiter. Default: ",".
        mode:      "overwrite" (default) replaces the file.
                   "append" adds rows to an existing file.

    Example::

        CSVSink("output/orders.csv")
        CSVSink("output/log.csv", mode="append")
    """

    def __init__(
        self,
        path: str | Path,
        separator: str = ",",
        mode: Literal["overwrite", "append"] = "overwrite",
    ) -> None:
        self.path = Path(path)
        self.separator = separator
        self.mode = mode

    def write(self, df: pl.DataFrame) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.mode == "append" and self.path.exists():
            existing = pl.read_csv(self.path, separator=self.separator)
            df = pl.concat([existing, df])
        df.write_csv(self.path, separator=self.separator)

    def __repr__(self) -> str:
        return f"CSVSink({self.path.name!r}, mode={self.mode!r})"
