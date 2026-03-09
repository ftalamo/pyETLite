"""Unit tests for the PyETLite CLI."""
from pathlib import Path

import pytest
from typer.testing import CliRunner

from pyetlite.cli.main import app

runner = CliRunner()


# ── Helpers ───────────────────────────────────────────────────────────

def write_pipeline_file(tmp_path: Path, content: str) -> Path:
    f = tmp_path / "pipeline.py"
    f.write_text(content)
    return f


VALID_PIPELINE = """
import polars as pl
from pyetlite import Pipeline, ErrorMode
from pyetlite.core.base import BaseSource, BaseSink

class FakeSource(BaseSource):
    def read(self):
        return pl.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})

class FakeSink(BaseSink):
    def write(self, df):
        pass

pipeline = (
    Pipeline("test_pipeline")
    .extract(FakeSource())
    .load(FakeSink())
)
"""

FAILING_TRANSFORM_PIPELINE = """
import polars as pl
from pyetlite import Pipeline, ErrorMode
from pyetlite.core.base import BaseSource, BaseSink, BaseTransform

class FakeSource(BaseSource):
    def read(self):
        return pl.DataFrame({"id": [1, 2]})

class FakeSink(BaseSink):
    def write(self, df): pass

class BoomTransform(BaseTransform):
    def apply(self, df):
        raise RuntimeError("boom")

pipeline = (
    Pipeline("failing_pipeline", error_mode=ErrorMode.FAIL_FAST)
    .extract(FakeSource())
    .transform(BoomTransform())
    .load(FakeSink())
)
"""

NO_PIPELINE_FILE = """
x = 42
y = "hello"
"""

INVALID_CONFIG_PIPELINE = """
from pyetlite import Pipeline

pipeline = Pipeline("no_source_pipeline")
"""

TWO_PIPELINES = """
import polars as pl
from pyetlite import Pipeline
from pyetlite.core.base import BaseSource, BaseSink

class FakeSource(BaseSource):
    def read(self):
        return pl.DataFrame({"id": [1]})

class FakeSink(BaseSink):
    def write(self, df): pass

pipeline_a = Pipeline("alpha").extract(FakeSource()).load(FakeSink())
pipeline_b = Pipeline("beta").extract(FakeSource()).load(FakeSink())
"""


# ── pyetlite run ──────────────────────────────────────────────────────

class TestRunCommand:
    def test_run_valid_pipeline_exits_zero(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, VALID_PIPELINE)
        result = runner.invoke(app, ["run", str(f)])
        assert result.exit_code == 0

    def test_run_shows_success_in_output(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, VALID_PIPELINE)
        result = runner.invoke(app, ["run", str(f)])
        assert "SUCCESS" in result.output

    def test_run_shows_pipeline_name(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, VALID_PIPELINE)
        result = runner.invoke(app, ["run", str(f)])
        assert "test_pipeline" in result.output

    def test_run_missing_file_exits_nonzero(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["run", str(tmp_path / "missing.py")])
        assert result.exit_code != 0

    def test_run_no_pipelines_exits_nonzero(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, NO_PIPELINE_FILE)
        result = runner.invoke(app, ["run", str(f)])
        assert result.exit_code != 0

    def test_run_failing_pipeline_exits_nonzero(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, FAILING_TRANSFORM_PIPELINE)
        result = runner.invoke(app, ["run", str(f)])
        assert result.exit_code != 0

    def test_run_dry_run_flag(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, VALID_PIPELINE)
        result = runner.invoke(app, ["run", str(f), "--dry-run"])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output

    def test_run_two_pipelines(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, TWO_PIPELINES)
        result = runner.invoke(app, ["run", str(f)])
        assert result.exit_code == 0
        assert "alpha" in result.output
        assert "beta" in result.output

    def test_run_with_name_filter(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, TWO_PIPELINES)
        result = runner.invoke(app, ["run", str(f), "--name", "alpha"])
        assert result.exit_code == 0
        assert "alpha" in result.output
        assert "beta" not in result.output

    def test_run_with_unknown_name_exits_nonzero(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, TWO_PIPELINES)
        result = runner.invoke(app, ["run", str(f), "--name", "nonexistent"])
        assert result.exit_code != 0


# ── pyetlite validate ─────────────────────────────────────────────────

class TestValidateCommand:
    def test_validate_valid_pipeline_exits_zero(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, VALID_PIPELINE)
        result = runner.invoke(app, ["validate", str(f)])
        assert result.exit_code == 0

    def test_validate_shows_checkmark(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, VALID_PIPELINE)
        result = runner.invoke(app, ["validate", str(f)])
        assert "✓" in result.output

    def test_validate_invalid_config_exits_nonzero(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, INVALID_CONFIG_PIPELINE)
        result = runner.invoke(app, ["validate", str(f)])
        assert result.exit_code != 0

    def test_validate_shows_cross_on_error(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, INVALID_CONFIG_PIPELINE)
        result = runner.invoke(app, ["validate", str(f)])
        assert "✗" in result.output

    def test_validate_no_pipelines_exits_nonzero(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, NO_PIPELINE_FILE)
        result = runner.invoke(app, ["validate", str(f)])
        assert result.exit_code != 0


# ── pyetlite list ─────────────────────────────────────────────────────

class TestListCommand:
    def test_list_shows_pipeline_names(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, TWO_PIPELINES)
        result = runner.invoke(app, ["list", str(f)])
        assert result.exit_code == 0
        assert "alpha" in result.output
        assert "beta" in result.output

    def test_list_no_pipelines_exits_nonzero(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, NO_PIPELINE_FILE)
        result = runner.invoke(app, ["list", str(f)])
        assert result.exit_code != 0

    def test_list_missing_file_exits_nonzero(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["list", str(Path("missing.py"))])
        assert result.exit_code != 0

    def test_list_shows_transform_count(self, tmp_path: Path) -> None:
        f = write_pipeline_file(tmp_path, VALID_PIPELINE)
        result = runner.invoke(app, ["list", str(f)])
        assert "transform" in result.output
