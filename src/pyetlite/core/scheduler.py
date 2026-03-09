"""PyETLite Scheduler — run pipelines on a cron schedule."""
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from pyetlite.core.pipeline import Pipeline

logger = logging.getLogger("pyetlite.scheduler")


def _run_pipeline_job(pipeline: Pipeline) -> None:
    """Job function executed by APScheduler on each tick."""
    logger.info("[scheduler] Triggering pipeline: %s", pipeline.name)
    try:
        result = pipeline.run()
        logger.info("[scheduler] %s", result.summary())
    except Exception as exc:
        logger.error("[scheduler] Pipeline '%s' raised: %s", pipeline.name, exc)


class Scheduler:
    """Wraps APScheduler to run one or more pipelines on cron schedules.

    Pipelines must have a ``schedule`` attribute set (cron expression string).
    Pipelines without a schedule are ignored.

    Args:
        pipelines:  List of Pipeline objects to schedule.
        db_path:    Path to SQLite file used for job persistence.
                    Defaults to ``~/.pyetlite/scheduler.db``.
        timezone:   Timezone string for cron expressions. Default: "UTC".

    Example::

        pipeline = Pipeline("daily", schedule="0 3 * * *") ...

        scheduler = Scheduler([pipeline])
        scheduler.start()   # blocks until Ctrl+C
    """

    def __init__(
        self,
        pipelines: list[Pipeline],
        db_path: Optional[Path] = None,
        timezone: str = "UTC",
    ) -> None:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

        if db_path is None:
            db_path = Path.home() / ".pyetlite" / "scheduler.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        jobstores = {
            "default": SQLAlchemyJobStore(url=f"sqlite:///{db_path}")
        }

        self._scheduler = BlockingScheduler(
            jobstores=jobstores,
            timezone=timezone,
        )
        self._pipelines = pipelines
        self._registered: list[str] = []
        self._db_path = db_path

    def _register_pipelines(self) -> None:
        """Add a cron job for each pipeline that has a schedule."""
        for pipeline in self._pipelines:
            if not pipeline.schedule:
                logger.warning(
                    "[scheduler] Pipeline '%s' has no schedule, skipping.", pipeline.name
                )
                continue

            # Remove existing job so re-runs get a clean state
            try:
                self._scheduler.remove_job(pipeline.name)
            except Exception:
                pass

            self._scheduler.add_job(
                func=_run_pipeline_job,
                trigger="cron",
                args=[pipeline],
                id=pipeline.name,
                name=pipeline.name,
                replace_existing=True,
                **_parse_cron(pipeline.schedule),
            )
            self._registered.append(pipeline.name)
            logger.info(
                "[scheduler] Registered '%s' with schedule '%s'",
                pipeline.name,
                pipeline.schedule,
            )

    def start(self) -> None:
        """Register pipelines and start the blocking scheduler.

        Blocks until SIGINT (Ctrl+C) or SIGTERM is received.
        """
        self._register_pipelines()

        if not self._registered:
            logger.error("[scheduler] No pipelines with a schedule found. Exiting.")
            return

        _print_schedule_table(self._pipelines, self._db_path)

        def _shutdown(signum: int, frame: object) -> None:
            logger.info("[scheduler] Shutdown signal received, stopping...")
            self._scheduler.shutdown(wait=False)
            sys.exit(0)

        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)

        logger.info("[scheduler] Starting. Press Ctrl+C to stop.")
        self._scheduler.start()

    def get_next_run_times(self) -> dict[str, Optional[datetime]]:
        """Return the next scheduled run time for each registered pipeline."""
        result = {}
        for job in self._scheduler.get_jobs():
            result[job.id] = job.next_run_time
        return result

    def stop(self) -> None:
        """Stop the scheduler gracefully (non-blocking contexts)."""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)


def _parse_cron(expression: str) -> dict:
    """Parse a 5-field cron string into APScheduler kwargs.

    Fields: minute hour day month day_of_week
    Example: "30 6 * * 1-5"  →  weekdays at 06:30
    """
    parts = expression.strip().split()
    if len(parts) != 5:
        raise ValueError(
            f"Invalid cron expression: {expression!r}. "
            "Expected 5 fields: minute hour day month day_of_week"
        )
    minute, hour, day, month, day_of_week = parts
    return {
        "minute": minute,
        "hour": hour,
        "day": day,
        "month": month,
        "day_of_week": day_of_week,
    }


def _print_schedule_table(pipelines: list[Pipeline], db_path: Path) -> None:
    """Print a human-readable summary of scheduled pipelines."""
    scheduled = [p for p in pipelines if p.schedule]
    print(f"\n{'─' * 55}")
    print("  PyETLite Scheduler")
    print(f"  DB: {db_path}")
    print(f"{'─' * 55}")
    for p in scheduled:
        print(f"  • {p.name:<30} [{p.schedule}]")
    print(f"{'─' * 55}\n")
