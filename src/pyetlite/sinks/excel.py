"""Excel sink connector."""
from pathlib import Path

import polars as pl

from pyetlite.core.base import BaseSink


class ExcelSink(BaseSink):
    """Write a DataFrame to an Excel (.xlsx) file.

    Args:
        path:       Destination file path.
        sheet_name: Sheet name. Default: "Sheet1".

    Example::

        ExcelSink("output/report.xlsx")
        ExcelSink("output/report.xlsx", sheet_name="Orders")
    """

    def __init__(self, path: str | Path, sheet_name: str = "Sheet1") -> None:
        self.path = Path(path)
        self.sheet_name = sheet_name

    def write(self, df: pl.DataFrame) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.write_excel(self.path, worksheet=self.sheet_name)

    def __repr__(self) -> str:
        return f"ExcelSink({self.path.name!r}, sheet={self.sheet_name!r})"
