"""Unit tests for file-based sources and sinks (CSV, Excel, JSON)."""
import json
from pathlib import Path

import polars as pl
import pytest

from pyetlite.sources import CSVSource, ExcelSource, JSONSource
from pyetlite.sinks import CSVSink, ExcelSink, JSONSink


# ── Fixture: sample DataFrame ─────────────────────────────────────────

@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame({
        "id":     [1, 2, 3],
        "name":   ["Alice", "Bob", "Charlie"],
        "amount": [100.0, 200.0, 300.0],
    })


# ── CSV ───────────────────────────────────────────────────────────────

class TestCSVSource:
    def test_reads_csv_file(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "test.csv"
        df.write_csv(path)
        result = CSVSource(str(path)).read()
        assert result.equals(df)

    def test_custom_separator(self, tmp_path: Path) -> None:
        path = tmp_path / "pipe.csv"
        path.write_text("id|name\n1|Alice\n2|Bob")
        result = CSVSource(str(path), separator="|").read()
        assert result.columns == ["id", "name"]
        assert len(result) == 2

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        with pytest.raises(Exception):
            CSVSource(str(tmp_path / "missing.csv")).read()

    def test_null_values_parsed(self, tmp_path: Path) -> None:
        path = tmp_path / "nulls.csv"
        path.write_text("id,value\n1,N/A\n2,100")
        result = CSVSource(str(path), null_values="N/A").read()
        assert result["value"][0] is None

    def test_repr(self, tmp_path: Path) -> None:
        src = CSVSource(str(tmp_path / "file.csv"))
        assert "CSVSource" in repr(src)


class TestCSVSink:
    def test_writes_csv_file(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "out.csv"
        CSVSink(str(path)).write(df)
        assert path.exists()

    def test_written_data_matches(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "out.csv"
        CSVSink(str(path)).write(df)
        result = pl.read_csv(path)
        assert result.equals(df)

    def test_creates_parent_dirs(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "nested" / "dir" / "out.csv"
        CSVSink(str(path)).write(df)
        assert path.exists()

    def test_overwrite_mode_replaces_file(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "out.csv"
        CSVSink(str(path)).write(df)
        small_df = df.head(1)
        CSVSink(str(path), mode="overwrite").write(small_df)
        result = pl.read_csv(path)
        assert len(result) == 1

    def test_append_mode_adds_rows(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "out.csv"
        CSVSink(str(path)).write(df)
        CSVSink(str(path), mode="append").write(df)
        result = pl.read_csv(path)
        assert len(result) == len(df) * 2

    def test_repr(self, tmp_path: Path) -> None:
        sink = CSVSink(str(tmp_path / "out.csv"))
        assert "CSVSink" in repr(sink)


# ── JSON ──────────────────────────────────────────────────────────────

class TestJSONSource:
    def test_reads_json_file(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "data.json"
        df.write_json(path)
        result = JSONSource(str(path)).read()
        assert result.equals(df)

    def test_reads_ndjson_file(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "data.ndjson"
        df.write_ndjson(path)
        result = JSONSource(str(path), lines=True).read()
        assert result.equals(df)

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        with pytest.raises(Exception):
            JSONSource(str(tmp_path / "missing.json")).read()

    def test_repr(self, tmp_path: Path) -> None:
        src = JSONSource(str(tmp_path / "data.json"))
        assert "JSONSource" in repr(src)


class TestJSONSink:
    def test_writes_json_file(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "out.json"
        JSONSink(str(path)).write(df)
        assert path.exists()

    def test_written_data_matches(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "out.json"
        JSONSink(str(path)).write(df)
        result = JSONSource(str(path)).read()
        assert result.equals(df)

    def test_writes_ndjson(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "out.ndjson"
        JSONSink(str(path), lines=True).write(df)
        lines = path.read_text().strip().split("\n")
        assert len(lines) == len(df)
        assert json.loads(lines[0])  # valid JSON per line

    def test_creates_parent_dirs(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "nested" / "out.json"
        JSONSink(str(path)).write(df)
        assert path.exists()

    def test_repr(self, tmp_path: Path) -> None:
        sink = JSONSink(str(tmp_path / "out.json"))
        assert "JSONSink" in repr(sink)


# ── Excel ─────────────────────────────────────────────────────────────

class TestExcelSource:
    def test_reads_excel_file(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "data.xlsx"
        df.write_excel(path)
        result = ExcelSource(str(path)).read()
        assert result.equals(df)

    def test_reads_named_sheet(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "data.xlsx"
        df.write_excel(path, worksheet="Sales")
        result = ExcelSource(str(path), sheet_name="Sales").read()
        assert result.equals(df)

    def test_file_not_found_raises(self, tmp_path: Path) -> None:
        with pytest.raises(Exception):
            ExcelSource(str(tmp_path / "missing.xlsx")).read()

    def test_repr(self, tmp_path: Path) -> None:
        src = ExcelSource(str(tmp_path / "data.xlsx"))
        assert "ExcelSource" in repr(src)


class TestExcelSink:
    def test_writes_excel_file(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "out.xlsx"
        ExcelSink(str(path)).write(df)
        assert path.exists()

    def test_written_data_matches(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "out.xlsx"
        ExcelSink(str(path)).write(df)
        result = ExcelSource(str(path)).read()
        assert result.equals(df)

    def test_creates_parent_dirs(self, df: pl.DataFrame, tmp_path: Path) -> None:
        path = tmp_path / "nested" / "out.xlsx"
        ExcelSink(str(path)).write(df)
        assert path.exists()

    def test_repr(self, tmp_path: Path) -> None:
        sink = ExcelSink(str(tmp_path / "out.xlsx"))
        assert "ExcelSink" in repr(sink)
