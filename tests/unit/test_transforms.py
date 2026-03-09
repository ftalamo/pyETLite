"""Unit tests for all built-in transforms."""
import pytest
import polars as pl

from pyetlite.transforms import (
    DropNulls,
    RenameColumns,
    SelectColumns,
    FilterRows,
    CastTypes,
    AddColumn,
    DropDuplicates,
    FillNulls,
)


# ── Shared fixture ────────────────────────────────────────────────────

@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame({
        "id":       [1, 2, 3, 4, 5],
        "name":     ["Alice", "Bob", None, "Diana", "Alice"],
        "email":    ["a@x.com", "b@x.com", None, "d@x.com", "a@x.com"],
        "amount":   [100.0, 200.0, None, 400.0, 100.0],
        "category": ["A", "B", "A", None, "A"],
    })


# ── DropNulls ─────────────────────────────────────────────────────────

class TestDropNulls:
    def test_drops_all_null_rows(self, df: pl.DataFrame) -> None:
        result = DropNulls().apply(df)
        assert result.null_count().sum_horizontal()[0] == 0

    def test_drops_correct_row_count(self, df: pl.DataFrame) -> None:
        # rows 2 (name null), 2 (email null), 3 (amount null), 3 (category null) — rows 2 and 3 have nulls
        result = DropNulls().apply(df)
        assert len(result) == 3

    def test_subset_only_checks_given_columns(self, df: pl.DataFrame) -> None:
        # Only drop rows where 'name' is null — that's just row index 2
        result = DropNulls(subset=["name"]).apply(df)
        assert len(result) == 4

    def test_repr(self) -> None:
        assert "DropNulls" in repr(DropNulls(subset=["name"]))


# ── RenameColumns ─────────────────────────────────────────────────────

class TestRenameColumns:
    def test_renames_columns(self, df: pl.DataFrame) -> None:
        result = RenameColumns({"id": "order_id", "amount": "total"}).apply(df)
        assert "order_id" in result.columns
        assert "total" in result.columns
        assert "id" not in result.columns
        assert "amount" not in result.columns

    def test_untouched_columns_stay(self, df: pl.DataFrame) -> None:
        result = RenameColumns({"id": "order_id"}).apply(df)
        assert "name" in result.columns

    def test_repr(self) -> None:
        assert "RenameColumns" in repr(RenameColumns({"a": "b"}))


# ── SelectColumns ─────────────────────────────────────────────────────

class TestSelectColumns:
    def test_keeps_only_selected_columns(self, df: pl.DataFrame) -> None:
        result = SelectColumns(["id", "name"]).apply(df)
        assert result.columns == ["id", "name"]

    def test_row_count_unchanged(self, df: pl.DataFrame) -> None:
        result = SelectColumns(["id"]).apply(df)
        assert len(result) == len(df)

    def test_missing_column_raises(self, df: pl.DataFrame) -> None:
        with pytest.raises(Exception):
            SelectColumns(["id", "nonexistent"]).apply(df)

    def test_repr(self) -> None:
        assert "SelectColumns" in repr(SelectColumns(["id"]))


# ── FilterRows ────────────────────────────────────────────────────────

class TestFilterRows:
    def test_filters_by_numeric_condition(self, df: pl.DataFrame) -> None:
        result = FilterRows(pl.col("amount") > 150).apply(df)
        assert all(v > 150 for v in result["amount"].drop_nulls().to_list())

    def test_filters_by_string_condition(self, df: pl.DataFrame) -> None:
        result = FilterRows(pl.col("category") == "A").apply(df)
        assert all(v == "A" for v in result["category"].to_list())

    def test_filter_reduces_rows(self, df: pl.DataFrame) -> None:
        result = FilterRows(pl.col("amount") > 300).apply(df)
        assert len(result) < len(df)

    def test_filter_all_out_returns_empty(self, df: pl.DataFrame) -> None:
        result = FilterRows(pl.col("amount") > 99999).apply(df)
        assert len(result) == 0

    def test_repr(self) -> None:
        assert "FilterRows" in repr(FilterRows(pl.col("id") > 0))


# ── CastTypes ─────────────────────────────────────────────────────────

class TestCastTypes:
    def test_casts_float_to_int(self) -> None:
        df = pl.DataFrame({"amount": [1.9, 2.1, 3.5]})
        result = CastTypes({"amount": pl.Int64}).apply(df)
        assert result["amount"].dtype == pl.Int64

    def test_casts_int_to_string(self) -> None:
        df = pl.DataFrame({"id": [1, 2, 3]})
        result = CastTypes({"id": pl.Utf8}).apply(df)
        assert result["id"].dtype == pl.Utf8

    def test_multiple_columns_cast(self) -> None:
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = CastTypes({"a": pl.Float32, "b": pl.Float64}).apply(df)
        assert result["a"].dtype == pl.Float32
        assert result["b"].dtype == pl.Float64

    def test_repr(self) -> None:
        assert "CastTypes" in repr(CastTypes({"id": pl.Int32}))


# ── AddColumn ─────────────────────────────────────────────────────────

class TestAddColumn:
    def test_adds_new_column(self, df: pl.DataFrame) -> None:
        result = AddColumn("doubled", pl.col("id") * 2).apply(df)
        assert "doubled" in result.columns

    def test_column_values_correct(self) -> None:
        df = pl.DataFrame({"price": [10.0, 20.0], "qty": [2, 3]})
        result = AddColumn("total", pl.col("price") * pl.col("qty")).apply(df)
        assert result["total"].to_list() == [20.0, 60.0]

    def test_overwrites_existing_column(self) -> None:
        df = pl.DataFrame({"x": [1, 2, 3]})
        result = AddColumn("x", pl.col("x") + 10).apply(df)
        assert result["x"].to_list() == [11, 12, 13]

    def test_original_columns_preserved(self, df: pl.DataFrame) -> None:
        result = AddColumn("new", pl.lit(0)).apply(df)
        for col in df.columns:
            assert col in result.columns

    def test_repr(self) -> None:
        assert "AddColumn" in repr(AddColumn("x", pl.lit(1)))


# ── DropDuplicates ────────────────────────────────────────────────────

class TestDropDuplicates:
    def test_removes_duplicate_rows(self, df: pl.DataFrame) -> None:
        # rows 0 and 4 are duplicates on (name, email, amount, category)
        result = DropDuplicates().apply(df)
        assert len(result) < len(df)

    def test_subset_dedup(self, df: pl.DataFrame) -> None:
        # "Alice" appears twice — dedup on name should leave one
        result = DropDuplicates(subset=["name"]).apply(df)
        names = result["name"].drop_nulls().to_list()
        assert names.count("Alice") == 1

    def test_keep_first(self) -> None:
        df = pl.DataFrame({"id": [1, 2, 3], "key": ["a", "a", "b"]})
        result = DropDuplicates(subset=["key"], keep="first").apply(df)
        assert result.filter(pl.col("key") == "a")["id"][0] == 1

    def test_keep_last(self) -> None:
        df = pl.DataFrame({"id": [1, 2, 3], "key": ["a", "a", "b"]})
        result = DropDuplicates(subset=["key"], keep="last").apply(df)
        assert result.filter(pl.col("key") == "a")["id"][0] == 2

    def test_no_duplicates_unchanged(self) -> None:
        df = pl.DataFrame({"id": [1, 2, 3]})
        result = DropDuplicates().apply(df)
        assert len(result) == 3

    def test_repr(self) -> None:
        assert "DropDuplicates" in repr(DropDuplicates(subset=["id"]))


# ── FillNulls ─────────────────────────────────────────────────────────

class TestFillNulls:
    def test_fills_with_scalar_value(self, df: pl.DataFrame) -> None:
        result = FillNulls(value=0).apply(df)
        assert result["amount"].null_count() == 0

    def test_fills_string_column(self, df: pl.DataFrame) -> None:
        result = FillNulls(value="unknown", subset=["name", "category"]).apply(df)
        assert result["name"].null_count() == 0
        assert result["category"].null_count() == 0

    def test_subset_leaves_other_columns_unchanged(self, df: pl.DataFrame) -> None:
        # Only fill 'name', leave 'amount' nulls intact
        result = FillNulls(value="N/A", subset=["name"]).apply(df)
        assert result["name"].null_count() == 0
        assert result["amount"].null_count() > 0

    def test_strategy_forward(self) -> None:
        df = pl.DataFrame({"x": [1.0, None, None, 4.0]})
        result = FillNulls(strategy="forward").apply(df)
        assert result["x"].to_list() == [1.0, 1.0, 1.0, 4.0]

    def test_strategy_mean(self) -> None:
        df = pl.DataFrame({"x": [10.0, None, 20.0]})
        result = FillNulls(strategy="mean").apply(df)
        assert result["x"][1] == 15.0

    def test_no_value_or_strategy_raises(self) -> None:
        with pytest.raises(ValueError, match="requires either"):
            FillNulls()

    def test_invalid_strategy_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid strategy"):
            FillNulls(strategy="median")

    def test_repr_with_value(self) -> None:
        assert "FillNulls" in repr(FillNulls(value=0))

    def test_repr_with_strategy(self) -> None:
        assert "strategy" in repr(FillNulls(strategy="forward"))
