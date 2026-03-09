"""SQL sink connectors (Postgres, MySQL)."""
from typing import Literal

import polars as pl

from pyetlite.core.base import BaseSink


class PostgresSink(BaseSink):
    """Write a DataFrame to a PostgreSQL table.

    Args:
        conn:      SQLAlchemy connection string.
                   Format: ``postgresql://user:password@host:port/dbname``
        table:     Target table name.
        if_exists: What to do if the table already exists.
                   "replace" (default), "append", or "fail".

    Example::

        PostgresSink(
            conn="postgresql://user:pass@localhost:5432/mydb",
            table="clean_orders",
            if_exists="replace",
        )
    """

    def __init__(
        self,
        conn: str,
        table: str,
        if_exists: Literal["replace", "append", "fail"] = "replace",
    ) -> None:
        self.conn = conn
        self.table = table
        self.if_exists = if_exists

    def write(self, df: pl.DataFrame) -> None:
        from sqlalchemy import create_engine

        engine = create_engine(self.conn)
        pandas_df = df.to_pandas()
        pandas_df.to_sql(
            self.table,
            con=engine,
            if_exists=self.if_exists,
            index=False,
        )

    def __repr__(self) -> str:
        return f"PostgresSink(table={self.table!r}, if_exists={self.if_exists!r})"


class MySQLSink(BaseSink):
    """Write a DataFrame to a MySQL table.

    Args:
        conn:      SQLAlchemy connection string.
                   Format: ``mysql+pymysql://user:password@host:port/dbname``
        table:     Target table name.
        if_exists: What to do if the table already exists.
                   "replace" (default), "append", or "fail".

    Example::

        MySQLSink(
            conn="mysql+pymysql://user:pass@localhost:3306/mydb",
            table="customers_clean",
        )
    """

    def __init__(
        self,
        conn: str,
        table: str,
        if_exists: Literal["replace", "append", "fail"] = "replace",
    ) -> None:
        self.conn = conn
        self.table = table
        self.if_exists = if_exists

    def write(self, df: pl.DataFrame) -> None:
        from sqlalchemy import create_engine

        engine = create_engine(self.conn)
        pandas_df = df.to_pandas()
        pandas_df.to_sql(
            self.table,
            con=engine,
            if_exists=self.if_exists,
            index=False,
        )

    def __repr__(self) -> str:
        return f"MySQLSink(table={self.table!r}, if_exists={self.if_exists!r})"
