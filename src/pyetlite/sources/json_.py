"""JSON source connector."""
from pathlib import Path

import polars as pl

from pyetlite.core.base import BaseSource


class JSONSource(BaseSource):
    """Read a JSON or NDJSON file into a Polars DataFrame.

    Args:
        path:   Path to the JSON file.
        lines:  If True, reads newline-delimited JSON (NDJSON). Default: False.

    Example::

        JSONSource("data/orders.json")
        JSONSource("data/events.ndjson", lines=True)
    """

    def __init__(self, path: str, lines: bool = False) -> None:
        self.path = Path(path)
        self.lines = lines

    def read(self) -> pl.DataFrame:
        if not self.path.exists():
            raise FileNotFoundError(f"JSON file not found: {self.path}")
        if self.lines:
            return pl.read_ndjson(self.path)
        return pl.read_json(self.path)

    def __repr__(self) -> str:
        return f"JSONSource({str(self.path)!r}, lines={self.lines})"
