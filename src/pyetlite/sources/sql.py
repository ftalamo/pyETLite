"""SQL source connectors (Postgres, MySQL)."""
import polars as pl

from pyetlite.core.base import BaseSource


class PostgresSource(BaseSource):
    """Read data from a PostgreSQL database via a SQL query.

    Args:
        conn:  SQLAlchemy connection string.
               Format: ``postgresql://user:password@host:port/dbname``
        query: SQL query to execute.

    Example::

        PostgresSource(
            conn="postgresql://user:pass@localhost:5432/mydb",
            query="SELECT * FROM orders WHERE status = 'pending'",
        )
    """

    def __init__(self, conn: str, query: str) -> None:
        self.conn = conn
        self.query = query

    def read(self) -> pl.DataFrame:
        from sqlalchemy import create_engine, text

        engine = create_engine(self.conn)
        with engine.connect() as connection:
            result = connection.execute(text(self.query))
            rows = result.fetchall()
            columns = list(result.keys())
        return pl.DataFrame(
            [dict(zip(columns, row)) for row in rows]
        )

    def __repr__(self) -> str:
        short_query = self.query[:40].replace("\n", " ")
        return f"PostgresSource(query={short_query!r}...)"


class MySQLSource(BaseSource):
    """Read data from a MySQL database via a SQL query.

    Args:
        conn:  SQLAlchemy connection string.
               Format: ``mysql+pymysql://user:password@host:port/dbname``
        query: SQL query to execute.

    Example::

        MySQLSource(
            conn="mysql+pymysql://user:pass@localhost:3306/mydb",
            query="SELECT * FROM customers LIMIT 1000",
        )
    """

    def __init__(self, conn: str, query: str) -> None:
        self.conn = conn
        self.query = query

    def read(self) -> pl.DataFrame:
        from sqlalchemy import create_engine, text

        engine = create_engine(self.conn)
        with engine.connect() as connection:
            result = connection.execute(text(self.query))
            rows = result.fetchall()
            columns = list(result.keys())
        return pl.DataFrame(
            [dict(zip(columns, row)) for row in rows]
        )

    def __repr__(self) -> str:
        short_query = self.query[:40].replace("\n", " ")
        return f"MySQLSource(query={short_query!r}...)"
