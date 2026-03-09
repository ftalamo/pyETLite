from enum import Enum


class ErrorMode(Enum):
    FAIL_FAST = "fail_fast"
    SKIP_AND_LOG = "skip_and_log"


class PyETLiteError(Exception):
    """Base exception for all PyETLite errors."""


class ExtractError(PyETLiteError):
    """Raised when a source fails to read data."""


class TransformError(PyETLiteError):
    """Raised when a transform step fails."""

    def __init__(self, step_name: str, original: Exception) -> None:
        self.step_name = step_name
        self.original = original
        super().__init__(f"Transform '{step_name}' failed: {original}")


class LoadError(PyETLiteError):
    """Raised when a sink fails to write data."""


class PipelineConfigError(PyETLiteError):
    """Raised when the pipeline is misconfigured."""
