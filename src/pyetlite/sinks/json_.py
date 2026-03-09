"""JSON sink connector."""
from pathlib import Path

import polars as pl

from pyetlite.core.base import BaseSink


class JSONSink(BaseSink):
    """Write a DataFrame to a JSON file.

    Args:
        path:  Destination file path.
        lines: If True, writes newline-delimited JSON (NDJSON). Default: False.

    Example::

        JSONSink("output/orders.json")
        JSONSink("output/events.ndjson", lines=True)
    """

    def __init__(self, path: str | Path, lines: bool = False) -> None:
        self.path = Path(path)
        self.lines = lines

    def write(self, df: pl.DataFrame) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.lines:
            df.write_ndjson(self.path)
        else:
            df.write_json(self.path)

    def __repr__(self) -> str:
        return f"JSONSink({self.path.name!r}, lines={self.lines})"
