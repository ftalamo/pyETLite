"""Shared pytest fixtures for PyETLite tests."""
import os
import pytest
import polars as pl


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({
        "id":       [1, 2, 3, 4, 5],
        "name":     ["Alice", "Bob", None, "Diana", "Eve"],
        "email":    ["alice@example.com", "BOB@EXAMPLE.COM", None, "diana@example.com", "eve@example.com"],
        "amount":   [100.0, 200.0, None, 400.0, -50.0],
        "category": ["A", "B", "A", None, "B"],
    })


@pytest.fixture
def clean_df() -> pl.DataFrame:
    return pl.DataFrame({
        "id":     [1, 2, 3],
        "name":   ["Alice", "Bob", "Charlie"],
        "amount": [100.0, 200.0, 300.0],
    })


@pytest.fixture
def postgres_url() -> str:
    url = os.getenv("TEST_POSTGRES_URL")
    if not url:
        pytest.skip("TEST_POSTGRES_URL not set")
    return url


@pytest.fixture
def mysql_url() -> str:
    url = os.getenv("TEST_MYSQL_URL")
    if not url:
        pytest.skip("TEST_MYSQL_URL not set")
    return url
