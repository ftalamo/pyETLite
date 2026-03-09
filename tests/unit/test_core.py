"""Unit tests for pyetlite core — Pipeline, ErrorMode, transforms."""
import pytest
import polars as pl

from pyetlite import Pipeline, ErrorMode, transform
from pyetlite.core.base import BaseSource, BaseSink, BaseTransform, FunctionTransform
from pyetlite.core.errors import (
    ExtractError, TransformError, LoadError, PipelineConfigError
)
from pyetlite.core.result import PipelineResult, StepResult


# ── Test doubles ─────────────────────────────────────────────────────

class FakeSource(BaseSource):
    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def read(self) -> pl.DataFrame:
        return self._df


class FailingSource(BaseSource):
    def read(self) -> pl.DataFrame:
        raise ConnectionError("DB unreachable")


class FakeSink(BaseSink):
    def __init__(self) -> None:
        self.written: pl.DataFrame | None = None

    def write(self, df: pl.DataFrame) -> None:
        self.written = df


class FailingSink(BaseSink):
    def write(self, df: pl.DataFrame) -> None:
        raise IOError("Disk full")


class DoubleRows(BaseTransform):
    """Transform that duplicates the DataFrame (for testing row counts)."""
    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        return pl.concat([df, df])


class FailingTransform(BaseTransform):
    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        raise ValueError("intentional failure")


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})


@pytest.fixture
def source(df: pl.DataFrame) -> FakeSource:
    return FakeSource(df)


@pytest.fixture
def sink() -> FakeSink:
    return FakeSink()


# ── Pipeline config validation ────────────────────────────────────────

class TestPipelineValidation:
    def test_missing_source_raises(self, sink: FakeSink) -> None:
        p = Pipeline("no_source").load(sink)
        with pytest.raises(PipelineConfigError, match="no source"):
            p.run()

    def test_missing_sink_raises(self, source: FakeSource) -> None:
        p = Pipeline("no_sink").extract(source)
        with pytest.raises(PipelineConfigError, match="no sink"):
            p.run()

    def test_missing_sink_ok_in_dry_run(self, source: FakeSource) -> None:
        result = Pipeline("dry", dry_run=True).extract(source).run()
        assert result.success

    def test_wrong_source_type_raises(self, sink: FakeSink) -> None:
        with pytest.raises(PipelineConfigError):
            Pipeline("bad").extract("not_a_source")  # type: ignore

    def test_wrong_sink_type_raises(self, source: FakeSource) -> None:
        with pytest.raises(PipelineConfigError):
            Pipeline("bad").extract(source).load("not_a_sink")  # type: ignore

    def test_wrong_transform_type_raises(self, source: FakeSource) -> None:
        with pytest.raises(PipelineConfigError):
            Pipeline("bad").extract(source).transform(42)  # type: ignore


# ── Happy path ────────────────────────────────────────────────────────

class TestPipelineRun:
    def test_basic_run_succeeds(self, source: FakeSource, sink: FakeSink, df: pl.DataFrame) -> None:
        result = Pipeline("basic").extract(source).load(sink).run()
        assert result.success
        assert result.rows_final == len(df)
        assert sink.written is not None
        assert sink.written.equals(df)

    def test_result_has_correct_steps(self, source: FakeSource, sink: FakeSink) -> None:
        result = Pipeline("steps").extract(source).transform(DoubleRows()).load(sink).run()
        # Steps: extract + transform + load
        assert len(result.steps) == 3

    def test_transform_changes_row_count(self, source: FakeSource, sink: FakeSink, df: pl.DataFrame) -> None:
        result = Pipeline("double").extract(source).transform(DoubleRows()).load(sink).run()
        assert result.rows_final == len(df) * 2

    def test_multiple_transforms_applied_in_order(self, sink: FakeSink) -> None:
        log = []
        source = FakeSource(pl.DataFrame({"x": [1]}))

        @transform
        def first(df: pl.DataFrame) -> pl.DataFrame:
            log.append("first")
            return df

        @transform
        def second(df: pl.DataFrame) -> pl.DataFrame:
            log.append("second")
            return df

        Pipeline("order").extract(source).transform(first).transform(second).load(sink).run()
        assert log == ["first", "second"]

    def test_dry_run_does_not_write(self, source: FakeSource, sink: FakeSink) -> None:
        Pipeline("dry", dry_run=True).extract(source).load(sink).run()
        assert sink.written is None

    def test_dry_run_result_is_success(self, source: FakeSource) -> None:
        result = Pipeline("dry", dry_run=True).extract(source).run()
        assert result.success
        assert result.dry_run


# ── Error modes ───────────────────────────────────────────────────────

class TestErrorModes:
    def test_fail_fast_raises_on_transform_error(self, source: FakeSource, sink: FakeSink) -> None:
        p = (
            Pipeline("ff", error_mode=ErrorMode.FAIL_FAST)
            .extract(source)
            .transform(FailingTransform())
            .load(sink)
        )
        with pytest.raises(TransformError):
            p.run()

    def test_fail_fast_does_not_write_sink(self, source: FakeSource, sink: FakeSink) -> None:
        p = (
            Pipeline("ff", error_mode=ErrorMode.FAIL_FAST)
            .extract(source)
            .transform(FailingTransform())
            .load(sink)
        )
        with pytest.raises(TransformError):
            p.run()
        assert sink.written is None

    def test_skip_and_log_continues_after_error(self, source: FakeSource, sink: FakeSink, df: pl.DataFrame) -> None:
        result = (
            Pipeline("sl", error_mode=ErrorMode.SKIP_AND_LOG)
            .extract(source)
            .transform(FailingTransform())
            .load(sink)
            .run()
        )
        # Pipeline completed but marked as failed
        assert result.success is False
        # Sink still received data (the pre-failure DataFrame)
        assert sink.written is not None

    def test_skip_and_log_records_error_in_step(self, source: FakeSource, sink: FakeSink) -> None:
        result = (
            Pipeline("sl", error_mode=ErrorMode.SKIP_AND_LOG)
            .extract(source)
            .transform(FailingTransform())
            .load(sink)
            .run()
        )
        failed_step = next(s for s in result.steps if not s.success)
        assert failed_step.error is not None
        assert "intentional failure" in failed_step.error

    def test_extract_error_always_raises(self) -> None:
        p = Pipeline("err").extract(FailingSource()).load(FakeSink())
        with pytest.raises(ExtractError):
            p.run()

    def test_load_error_raises(self, source: FakeSource) -> None:
        p = Pipeline("err").extract(source).load(FailingSink())
        with pytest.raises(LoadError):
            p.run()


# ── @transform decorator ──────────────────────────────────────────────

class TestTransformDecorator:
    def test_decorator_returns_function_transform(self) -> None:
        @transform
        def my_fn(df: pl.DataFrame) -> pl.DataFrame:
            return df

        assert isinstance(my_fn, FunctionTransform)

    def test_decorated_function_is_callable(self, df: pl.DataFrame) -> None:
        @transform
        def add_col(df: pl.DataFrame) -> pl.DataFrame:
            return df.with_columns(pl.lit(1).alias("new_col"))

        result = add_col(df)
        assert "new_col" in result.columns

    def test_decorated_function_works_in_pipeline(self, source: FakeSource, sink: FakeSink) -> None:
        @transform
        def drop_nothing(df: pl.DataFrame) -> pl.DataFrame:
            return df

        result = Pipeline("dec").extract(source).transform(drop_nothing).load(sink).run()
        assert result.success

    def test_plain_lambda_works_as_transform(self, source: FakeSource, sink: FakeSink) -> None:
        result = (
            Pipeline("lam")
            .extract(source)
            .transform(lambda df: df)
            .load(sink)
            .run()
        )
        assert result.success


# ── PipelineResult ────────────────────────────────────────────────────

class TestPipelineResult:
    def test_summary_contains_pipeline_name(self, source: FakeSource, sink: FakeSink) -> None:
        result = Pipeline("my_pipeline").extract(source).load(sink).run()
        assert "my_pipeline" in result.summary()

    def test_summary_shows_dry_run(self, source: FakeSource) -> None:
        result = Pipeline("p", dry_run=True).extract(source).run()
        assert "DRY RUN" in result.summary()

    def test_summary_shows_row_count(self, source: FakeSource, sink: FakeSink, df: pl.DataFrame) -> None:
        result = Pipeline("p").extract(source).load(sink).run()
        assert str(len(df)) in result.summary()

    def test_result_total_duration_is_positive(self, source: FakeSource, sink: FakeSink) -> None:
        result = Pipeline("p").extract(source).load(sink).run()
        assert result.total_duration_ms > 0
