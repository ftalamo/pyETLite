"""Unit tests for the Scheduler (no APScheduler dependency needed for most tests)."""
import pytest
import polars as pl
from pathlib import Path
from unittest.mock import MagicMock, patch

from pyetlite import Pipeline
from pyetlite.core.base import BaseSource, BaseSink
from pyetlite.core.scheduler import _parse_cron, _run_pipeline_job, Scheduler


# ── Test doubles ──────────────────────────────────────────────────────

class FakeSource(BaseSource):
    def read(self) -> pl.DataFrame:
        return pl.DataFrame({"id": [1, 2], "value": [10.0, 20.0]})


class FakeSink(BaseSink):
    def __init__(self) -> None:
        self.calls = 0

    def write(self, df: pl.DataFrame) -> None:
        self.calls += 1


def make_pipeline(name: str, schedule: str | None = "0 3 * * *") -> Pipeline:
    sink = FakeSink()
    return (
        Pipeline(name=name, schedule=schedule)
        .extract(FakeSource())
        .load(sink)
    )


# ── _parse_cron ───────────────────────────────────────────────────────

class TestParseCron:
    def test_valid_cron_returns_dict(self) -> None:
        result = _parse_cron("30 6 * * 1-5")
        assert result == {
            "minute": "30",
            "hour": "6",
            "day": "*",
            "month": "*",
            "day_of_week": "1-5",
        }

    def test_every_minute(self) -> None:
        result = _parse_cron("* * * * *")
        assert all(v == "*" for v in result.values())

    def test_midnight_daily(self) -> None:
        result = _parse_cron("0 0 * * *")
        assert result["minute"] == "0"
        assert result["hour"] == "0"

    def test_invalid_expression_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid cron expression"):
            _parse_cron("0 3 * *")  # only 4 fields

    def test_too_many_fields_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid cron expression"):
            _parse_cron("0 3 * * * extra")

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_cron("")


# ── _run_pipeline_job ─────────────────────────────────────────────────

class TestRunPipelineJob:
    def test_successful_pipeline_runs(self) -> None:
        sink = FakeSink()
        pipeline = Pipeline("job_test").extract(FakeSource()).load(sink)
        _run_pipeline_job(pipeline)
        assert sink.calls == 1

    def test_failing_pipeline_does_not_raise(self) -> None:
        """Scheduler job must never raise — it logs instead."""
        class BoomSource(BaseSource):
            def read(self) -> pl.DataFrame:
                raise RuntimeError("source failed")

        pipeline = Pipeline("boom").extract(BoomSource()).load(FakeSink())
        # Should not raise
        _run_pipeline_job(pipeline)


# ── Scheduler ─────────────────────────────────────────────────────────

class TestScheduler:
    def test_filters_pipelines_without_schedule(self, tmp_path: Path) -> None:
        p_with = make_pipeline("with_schedule", schedule="0 3 * * *")
        p_without = make_pipeline("no_schedule", schedule=None)

        with patch("pyetlite.core.scheduler.BlockingScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched_cls.return_value = mock_sched
            mock_sched.get_jobs.return_value = []

            scheduler = Scheduler([p_with, p_without], db_path=tmp_path / "test.db")
            scheduler._register_pipelines()

            # Only the pipeline with a schedule should be added
            add_job_calls = mock_sched.add_job.call_args_list
            job_ids = [call.kwargs.get("id") for call in add_job_calls]
            assert "with_schedule" in job_ids
            assert "no_schedule" not in job_ids

    def test_registers_all_scheduled_pipelines(self, tmp_path: Path) -> None:
        pipelines = [
            make_pipeline("alpha", "0 1 * * *"),
            make_pipeline("beta",  "0 2 * * *"),
            make_pipeline("gamma", "0 3 * * *"),
        ]

        with patch("pyetlite.core.scheduler.BlockingScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched_cls.return_value = mock_sched
            mock_sched.get_jobs.return_value = []

            scheduler = Scheduler(pipelines, db_path=tmp_path / "test.db")
            scheduler._register_pipelines()

            assert mock_sched.add_job.call_count == 3

    def test_invalid_cron_raises_on_register(self, tmp_path: Path) -> None:
        p = make_pipeline("bad_cron")
        p.schedule = "bad cron expr here now"  # 5 words but invalid fields is ok,
        p.schedule = "* * * *"                 # 4 fields → should raise

        with patch("pyetlite.core.scheduler.BlockingScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched_cls.return_value = mock_sched

            scheduler = Scheduler([p], db_path=tmp_path / "test.db")
            with pytest.raises(ValueError, match="Invalid cron expression"):
                scheduler._register_pipelines()

    def test_stop_calls_shutdown(self, tmp_path: Path) -> None:
        with patch("pyetlite.core.scheduler.BlockingScheduler") as mock_sched_cls:
            mock_sched = MagicMock()
            mock_sched.running = True
            mock_sched_cls.return_value = mock_sched

            scheduler = Scheduler([], db_path=tmp_path / "test.db")
            scheduler.stop()

            mock_sched.shutdown.assert_called_once_with(wait=False)

    def test_db_directory_created(self, tmp_path: Path) -> None:
        db = tmp_path / "nested" / "dir" / "scheduler.db"

        with patch("pyetlite.core.scheduler.BlockingScheduler"):
            Scheduler([], db_path=db)

        assert db.parent.exists()
