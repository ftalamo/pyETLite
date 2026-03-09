import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator, List, Optional


@dataclass
class StepResult:
    """Metrics for a single pipeline step."""

    name: str
    success: bool
    rows_in: int
    rows_out: int
    duration_ms: float
    error: Optional[str] = None

    def __str__(self) -> str:
        icon = "✓" if self.success else "✗"
        err = f"  → {self.error}" if self.error else ""
        return (
            f"  [{icon}] {self.name:<35} "
            f"{self.rows_in:>6} → {self.rows_out:<6} rows  "
            f"({self.duration_ms:.1f}ms){err}"
        )


@dataclass
class PipelineResult:
    """Full result of a pipeline run."""

    pipeline_name: str
    success: bool
    dry_run: bool = False
    steps: List[StepResult] = field(default_factory=list)
    total_duration_ms: float = 0.0
    rows_final: int = 0

    def summary(self) -> str:
        status = "✅ SUCCESS" if self.success else "❌ FAILED"
        dry = "  [DRY RUN — sink skipped]" if self.dry_run else ""
        lines = [
            "",
            f"{status} — {self.pipeline_name}{dry}",
            f"  Total time : {self.total_duration_ms:.1f}ms",
            f"  Final rows : {self.rows_final}",
            f"  Steps      : {len(self.steps)}",
            "",
        ]
        for step in self.steps:
            lines.append(str(step))
        lines.append("")
        return "\n".join(lines)

    def __repr__(self) -> str:
        status = "success" if self.success else "failed"
        return f"PipelineResult({self.pipeline_name!r}, {status}, rows={self.rows_final})"


@contextmanager
def timer() -> Generator[dict, None, None]:
    """Context manager that measures elapsed time in milliseconds."""
    result: dict = {}
    start = time.perf_counter()
    try:
        yield result
    finally:
        result["elapsed_ms"] = (time.perf_counter() - start) * 1000
