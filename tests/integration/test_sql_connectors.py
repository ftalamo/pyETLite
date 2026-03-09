"""Integration tests for SQL connectors.

These tests are skipped automatically if the env vars are not set.
To run locally:

    $env:TEST_POSTGRES_URL="postgresql://user:pass@localhost:5432/testdb"
    $env:TEST_MYSQL_URL="mysql+pymysql://root:pass@localhost:3306/testdb"
    pytest tests/integration/ -v
"""
import pytest
import polars as pl

from pyetlite.sources import PostgresSource, MySQLSource
from pyetlite.sinks import PostgresSink, MySQLSink


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({
        "id":     [1, 2, 3],
        "name":   ["Alice", "Bob", "Charlie"],
        "amount": [100.0, 200.0, 300.0],
    })


# ── Postgres ──────────────────────────────────────────────────────────

class TestPostgresConnectors:
    def test_write_and_read_roundtrip(self, postgres_url: str, sample_df: pl.DataFrame) -> None:
        sink = PostgresSink(conn=postgres_url, table="test_pyetlite", if_exists="replace")
        sink.write(sample_df)

        source = PostgresSource(conn=postgres_url, query="SELECT * FROM test_pyetlite ORDER BY id")
        result = source.read()

        assert list(result.columns) == list(sample_df.columns)
        assert len(result) == len(sample_df)
        assert result["name"].to_list() == sample_df["name"].to_list()

    def test_append_mode_adds_rows(self, postgres_url: str, sample_df: pl.DataFrame) -> None:
        PostgresSink(conn=postgres_url, table="test_append", if_exists="replace").write(sample_df)
        PostgresSink(conn=postgres_url, table="test_append", if_exists="append").write(sample_df)

        source = PostgresSource(conn=postgres_url, query="SELECT * FROM test_append")
        result = source.read()
        assert len(result) == len(sample_df) * 2

    def test_repr(self, postgres_url: str) -> None:
        src = PostgresSource(conn=postgres_url, query="SELECT 1")
        assert "PostgresSource" in repr(src)
        sink = PostgresSink(conn=postgres_url, table="t")
        assert "PostgresSink" in repr(sink)


# ── MySQL ─────────────────────────────────────────────────────────────

class TestMySQLConnectors:
    def test_write_and_read_roundtrip(self, mysql_url: str, sample_df: pl.DataFrame) -> None:
        sink = MySQLSink(conn=mysql_url, table="test_pyetlite", if_exists="replace")
        sink.write(sample_df)

        source = MySQLSource(conn=mysql_url, query="SELECT * FROM test_pyetlite ORDER BY id")
        result = source.read()

        assert list(result.columns) == list(sample_df.columns)
        assert len(result) == len(sample_df)
        assert result["name"].to_list() == sample_df["name"].to_list()

    def test_append_mode_adds_rows(self, mysql_url: str, sample_df: pl.DataFrame) -> None:
        MySQLSink(conn=mysql_url, table="test_append", if_exists="replace").write(sample_df)
        MySQLSink(conn=mysql_url, table="test_append", if_exists="append").write(sample_df)

        source = MySQLSource(conn=mysql_url, query="SELECT * FROM test_append")
        result = source.read()
        assert len(result) == len(sample_df) * 2

    def test_repr(self, mysql_url: str) -> None:
        src = MySQLSource(conn=mysql_url, query="SELECT 1")
        assert "MySQLSource" in repr(src)
        sink = MySQLSink(conn=mysql_url, table="t")
        assert "MySQLSink" in repr(sink)
