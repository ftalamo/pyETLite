"""Built-in transforms for PyETLite."""
from typing import Any, Dict, List, Literal, Optional, Union

import polars as pl

from pyetlite.core.base import BaseTransform


class DropNulls(BaseTransform):
    """Remove rows that contain null values.

    Args:
        subset: Column names to check. If None, checks all columns.

    Example::

        DropNulls()                          # drop if ANY column is null
        DropNulls(subset=["email", "name"])  # drop only if these are null
    """

    def __init__(self, subset: Optional[List[str]] = None) -> None:
        self.subset = subset

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.drop_nulls(subset=self.subset)

    def __repr__(self) -> str:
        return f"DropNulls(subset={self.subset})"


class RenameColumns(BaseTransform):
    """Rename columns using a mapping dict.

    Args:
        mapping: Dict of {old_name: new_name}.

    Example::

        RenameColumns({"id": "order_id", "ts": "created_at"})
    """

    def __init__(self, mapping: Dict[str, str]) -> None:
        self.mapping = mapping

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.rename(self.mapping)

    def __repr__(self) -> str:
        return f"RenameColumns({self.mapping})"


class SelectColumns(BaseTransform):
    """Keep only the specified columns (projection).

    Args:
        columns: List of column names to keep.

    Example::

        SelectColumns(["id", "name", "amount"])
    """

    def __init__(self, columns: List[str]) -> None:
        self.columns = columns

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.select(self.columns)

    def __repr__(self) -> str:
        return f"SelectColumns({self.columns})"


class FilterRows(BaseTransform):
    """Keep only rows that match a Polars expression.

    Args:
        expression: A Polars expression (pl.Expr) to filter by.

    Example::

        FilterRows(pl.col("amount") > 0)
        FilterRows(pl.col("status") == "active")
    """

    def __init__(self, expression: pl.Expr) -> None:
        self.expression = expression

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.filter(self.expression)

    def __repr__(self) -> str:
        return f"FilterRows({self.expression})"


class CastTypes(BaseTransform):
    """Cast columns to new Polars data types.

    Args:
        mapping: Dict of {column_name: polars_dtype}.

    Example::

        CastTypes({"amount": pl.Float64, "id": pl.Int32})
    """

    def __init__(self, mapping: Dict[str, type]) -> None:
        self.mapping = mapping

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        exprs = [pl.col(col).cast(dtype) for col, dtype in self.mapping.items()]
        return df.with_columns(exprs)

    def __repr__(self) -> str:
        names = {k: str(v) for k, v in self.mapping.items()}
        return f"CastTypes({names})"


class AddColumn(BaseTransform):
    """Add a new computed column using a Polars expression.

    Args:
        name:       Name of the new column.
        expression: Polars expression that produces the column values.

    Example::

        AddColumn("total", pl.col("price") * pl.col("qty"))
        AddColumn("upper_name", pl.col("name").str.to_uppercase())
    """

    def __init__(self, name: str, expression: pl.Expr) -> None:
        self.name = name
        self.expression = expression

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(self.expression.alias(self.name))

    def __repr__(self) -> str:
        return f"AddColumn({self.name!r})"


class DropDuplicates(BaseTransform):
    """Remove duplicate rows.

    Args:
        subset: Columns to consider for deduplication. If None, uses all columns.
        keep:   Which duplicate to keep: "first" (default) or "last".

    Example::

        DropDuplicates()
        DropDuplicates(subset=["email"], keep="last")
    """

    def __init__(
        self,
        subset: Optional[List[str]] = None,
        keep: Literal["first", "last"] = "first",
    ) -> None:
        self.subset = subset
        self.keep = keep

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.unique(subset=self.subset, keep=self.keep, maintain_order=True)

    def __repr__(self) -> str:
        return f"DropDuplicates(subset={self.subset}, keep={self.keep!r})"


class FillNulls(BaseTransform):
    """Fill null values with a fixed value or a column strategy.

    Args:
        value:    Scalar value to fill nulls with (e.g. 0, "N/A").
        subset:   Columns to apply the fill to. If None, applies to all columns.
        strategy: Polars fill strategy: "mean", "forward", "backward", "min", "max".
                  If set, overrides ``value``.

    Example::

        FillNulls(value=0)
        FillNulls(value="unknown", subset=["category"])
        FillNulls(strategy="forward")
    """

    VALID_STRATEGIES = {"forward", "backward", "mean", "min", "max"}

    def __init__(
        self,
        value: Optional[Any] = None,
        subset: Optional[List[str]] = None,
        strategy: Optional[str] = None,
    ) -> None:
        if value is None and strategy is None:
            raise ValueError("FillNulls requires either 'value' or 'strategy'.")
        if strategy is not None and strategy not in self.VALID_STRATEGIES:
            raise ValueError(
                f"Invalid strategy {strategy!r}. Valid: {self.VALID_STRATEGIES}"
            )
        self.value = value
        self.subset = subset
        self.strategy = strategy

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        columns = self.subset or df.columns

        if self.strategy:
            exprs = [pl.col(c).fill_null(strategy=self.strategy) for c in columns]  # type: ignore[arg-type]
        else:
            exprs = [pl.col(c).fill_null(self.value) for c in columns]

        return df.with_columns(exprs)

    def __repr__(self) -> str:
        if self.strategy:
            return f"FillNulls(strategy={self.strategy!r}, subset={self.subset})"
        return f"FillNulls(value={self.value!r}, subset={self.subset})"
