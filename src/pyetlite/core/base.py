from abc import ABC, abstractmethod
from typing import Callable

import polars as pl


class BaseSource(ABC):
    """Abstract base class for all data sources."""

    @abstractmethod
    def read(self) -> pl.DataFrame:
        """Read data and return a Polars DataFrame."""
        ...

    def __repr__(self) -> str:
        return self.__class__.__name__


class BaseSink(ABC):
    """Abstract base class for all data sinks."""

    @abstractmethod
    def write(self, df: pl.DataFrame) -> None:
        """Write a Polars DataFrame to the destination."""
        ...

    def __repr__(self) -> str:
        return self.__class__.__name__


class BaseTransform(ABC):
    """Abstract base class for all transforms."""

    @abstractmethod
    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply transformation and return a new DataFrame."""
        ...

    def __call__(self, df: pl.DataFrame) -> pl.DataFrame:
        return self.apply(df)

    def __repr__(self) -> str:
        return self.__class__.__name__


class FunctionTransform(BaseTransform):
    """Wraps a plain Python function as a BaseTransform.

    Created automatically by the @transform decorator.
    """

    def __init__(self, fn: Callable[[pl.DataFrame], pl.DataFrame]) -> None:
        self._fn = fn
        self._name = fn.__name__

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        return self._fn(df)

    def __repr__(self) -> str:
        return f"transform({self._name})"


def transform(fn: Callable[[pl.DataFrame], pl.DataFrame]) -> FunctionTransform:
    """Decorator to turn a plain function into a BaseTransform.

    Example::

        @transform
        def normalize_emails(df: pl.DataFrame) -> pl.DataFrame:
            return df.with_columns(pl.col("email").str.to_lowercase())

        pipeline.transform(normalize_emails)
    """
    return FunctionTransform(fn)
