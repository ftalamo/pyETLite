import logging
import time
from typing import Callable, List, Optional, Union

import polars as pl

from .base import BaseSource, BaseSink, BaseTransform, FunctionTransform
from .errors import ErrorMode, ExtractError, TransformError, LoadError, PipelineConfigError
from .result import PipelineResult, StepResult, timer

logger = logging.getLogger("pyetlite")

TransformLike = Union[BaseTransform, Callable[[pl.DataFrame], pl.DataFrame]]


class Pipeline:
    """Declarative ETL pipeline.

    Usage::

        result = (
            Pipeline("my_pipeline", error_mode=ErrorMode.SKIP_AND_LOG)
            .extract(CSVSource("data.csv"))
            .transform(DropNulls())
            .transform(lambda df: df.rename({"id": "order_id"}))
            .load(CSVSink("output/clean.csv"))
            .run()
        )
        print(result.summary())
    """

    def __init__(
        self,
        name: str,
        error_mode: ErrorMode = ErrorMode.FAIL_FAST,
        dry_run: bool = False,
        schedule: Optional[str] = None,
    ) -> None:
        self.name = name
        self.error_mode = error_mode
        self.dry_run = dry_run
        self.schedule = schedule

        self._source: Optional[BaseSource] = None
        self._sink: Optional[BaseSink] = None
        self._transforms: List[BaseTransform] = []

    # ── Builder methods ──────────────────────────────────────────────

    def extract(self, source: BaseSource) -> "Pipeline":
        """Set the data source."""
        if not isinstance(source, BaseSource):
            raise PipelineConfigError(
                f"extract() expects a BaseSource, got {type(source).__name__}"
            )
        self._source = source
        return self

    def transform(self, step: TransformLike) -> "Pipeline":
        """Add a transform step. Accepts a BaseTransform or a plain function."""
        if callable(step) and not isinstance(step, BaseTransform):
            step = FunctionTransform(step)
        if not isinstance(step, BaseTransform):
            raise PipelineConfigError(
                f"transform() expects a BaseTransform or callable, got {type(step).__name__}"
            )
        self._transforms.append(step)
        return self

    def load(self, sink: BaseSink) -> "Pipeline":
        """Set the data sink."""
        if not isinstance(sink, BaseSink):
            raise PipelineConfigError(
                f"load() expects a BaseSink, got {type(sink).__name__}"
            )
        self._sink = sink
        return self

    # ── Execution ────────────────────────────────────────────────────

    def run(self) -> PipelineResult:
        """Execute the pipeline and return a PipelineResult."""
        self._validate_config()

        result = PipelineResult(
            pipeline_name=self.name,
            success=True,
            dry_run=self.dry_run,
        )
        start_total = time.perf_counter()

        # ── Extract ──
        logger.info("[%s] Extracting from %s", self.name, self._source)
        with timer() as t:
            try:
                df = self._source.read()  # type: ignore[union-attr]
            except Exception as exc:
                raise ExtractError(f"Source {self._source} failed: {exc}") from exc

        rows_after_extract = len(df)
        result.steps.append(StepResult(
            name=f"extract({self._source})",
            success=True,
            rows_in=0,
            rows_out=rows_after_extract,
            duration_ms=t["elapsed_ms"],
        ))
        logger.info("[%s] Extracted %d rows in %.1fms", self.name, rows_after_extract, t["elapsed_ms"])

        # ── Transform ──
        for step in self._transforms:
            rows_in = len(df)
            logger.info("[%s] Running transform: %s", self.name, step)
            with timer() as t:
                try:
                    df = step(df)
                    success = True
                    error_msg = None
                except Exception as exc:
                    err = TransformError(str(step), exc)
                    success = False
                    error_msg = str(exc)
                    if self.error_mode == ErrorMode.FAIL_FAST:
                        result.success = False
                        result.steps.append(StepResult(
                            name=f"transform({step})",
                            success=False,
                            rows_in=rows_in,
                            rows_out=rows_in,
                            duration_ms=t["elapsed_ms"],
                            error=error_msg,
                        ))
                        result.total_duration_ms = (time.perf_counter() - start_total) * 1000
                        raise err from exc
                    else:
                        logger.warning("[%s] Skipping failed transform '%s': %s", self.name, step, exc)

            result.steps.append(StepResult(
                name=f"transform({step})",
                success=success,
                rows_in=rows_in,
                rows_out=len(df),
                duration_ms=t["elapsed_ms"],
                error=error_msg,
            ))
            if not success:
                result.success = False

        # ── Load ──
        rows_final = len(df)
        result.rows_final = rows_final

        if self.dry_run:
            logger.info("[%s] DRY RUN — skipping sink %s", self.name, self._sink)
            result.steps.append(StepResult(
                name=f"load({self._sink}) [skipped]",
                success=True,
                rows_in=rows_final,
                rows_out=rows_final,
                duration_ms=0.0,
            ))
        else:
            logger.info("[%s] Loading %d rows into %s", self.name, rows_final, self._sink)
            with timer() as t:
                try:
                    self._sink.write(df)  # type: ignore[union-attr]
                except Exception as exc:
                    raise LoadError(f"Sink {self._sink} failed: {exc}") from exc

            result.steps.append(StepResult(
                name=f"load({self._sink})",
                success=True,
                rows_in=rows_final,
                rows_out=rows_final,
                duration_ms=t["elapsed_ms"],
            ))
            logger.info("[%s] Loaded in %.1fms", self.name, t["elapsed_ms"])

        result.total_duration_ms = (time.perf_counter() - start_total) * 1000
        logger.info("[%s] Done in %.1fms — %s", self.name, result.total_duration_ms,
                    "SUCCESS" if result.success else "FAILED")
        return result

    # ── Validation ───────────────────────────────────────────────────

    def _validate_config(self) -> None:
        if self._source is None:
            raise PipelineConfigError(f"Pipeline '{self.name}' has no source. Call .extract() first.")
        if self._sink is None and not self.dry_run:
            raise PipelineConfigError(
                f"Pipeline '{self.name}' has no sink. Call .load() or use dry_run=True."
            )

    def __repr__(self) -> str:
        return (
            f"Pipeline({self.name!r}, "
            f"transforms={len(self._transforms)}, "
            f"error_mode={self.error_mode.value})"
        )
