"""Excel source connector."""
from pathlib import Path
from typing import Optional, Union

import polars as pl

from pyetlite.core.base import BaseSource


class ExcelSource(BaseSource):
    """Read an Excel file (.xlsx / .xls) into a Polars DataFrame.

    Args:
        path:       Path to the Excel file.
        sheet_name: Sheet name or index (0-based). Default: 0 (first sheet).
        has_header: Whether the first row is a header. Default: True.

    Example::

        ExcelSource("data/report.xlsx")
        ExcelSource("data/report.xlsx", sheet_name="Sales")
        ExcelSource("data/report.xlsx", sheet_name=1)
    """

    def __init__(
        self,
        path: str,
        sheet_name: Union[str, int] = 0,
        has_header: bool = True,
    ) -> None:
        self.path = Path(path)
        self.sheet_name = sheet_name
        self.has_header = has_header

    def read(self) -> pl.DataFrame:
        if not self.path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.path}")
        return pl.read_excel(
            self.path,
            sheet_name=self.sheet_name,
            has_header=self.has_header,
        )

    def __repr__(self) -> str:
        return f"ExcelSource({str(self.path)!r}, sheet={self.sheet_name!r})"
