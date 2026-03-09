"""CSV source connector."""
from pathlib import Path
from typing import Optional

import polars as pl

from pyetlite.core.base import BaseSource


class CSVSource(BaseSource):
    """Read a CSV file into a Polars DataFrame.

    Args:
        path:       Path to the CSV file.
        separator:  Column delimiter. Default: ",".
        encoding:   File encoding. Default: "utf8".
        has_header: Whether the first row is a header. Default: True.
        null_values: String(s) to interpret as null. Default: None.

    Example::

        CSVSource("data/orders.csv")
        CSVSource("data/legacy.csv", separator=";", encoding="latin1")
    """

    def __init__(
        self,
        path: str,
        separator: str = ",",
        encoding: str = "utf8",
        has_header: bool = True,
        null_values: Optional[list] = None,
    ) -> None:
        self.path = Path(path)
        self.separator = separator
        self.encoding = encoding
        self.has_header = has_header
        self.null_values = null_values

    def read(self) -> pl.DataFrame:
        if not self.path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.path}")
        return pl.read_csv(
            self.path,
            separator=self.separator,
            encoding=self.encoding,
            has_header=self.has_header,
            null_values=self.null_values,
        )

    def __repr__(self) -> str:
        return f"CSVSource({str(self.path)!r})"
