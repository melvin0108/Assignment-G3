"""Pure helpers for monitoring CSV schema evolution in Bronze ingestion."""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence, TypeVar


T = TypeVar("T")


@dataclass(frozen=True)
class HeaderComparison:
    """Set-based CSV header differences.

    Auto Loader maps inferred CSV columns by header, so reordering is retained
    for diagnostics but is not treated as schema drift.
    """

    previous_columns: tuple[str, ...]
    current_columns: tuple[str, ...]
    added_columns: tuple[str, ...]
    missing_columns: tuple[str, ...]
    reordered: bool

    @property
    def has_drift(self) -> bool:
        return bool(self.added_columns or self.missing_columns)


def parse_csv_header(csv_text: str) -> list[str]:
    """Parse the first CSV record, including quoted delimiters and BOMs."""
    if not csv_text:
        raise ValueError("CSV file is empty; a header row is required")

    reader = csv.reader(io.StringIO(csv_text.lstrip("\ufeff")))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise ValueError("CSV file is empty; a header row is required") from exc

    if not header or all(not column.strip() for column in header):
        raise ValueError("CSV header is empty")
    return [column.strip() for column in header]


def compare_headers(
    previous_columns: Sequence[str], current_columns: Sequence[str]
) -> HeaderComparison:
    """Compare headers without treating column order as drift."""
    previous = tuple(previous_columns)
    current = tuple(current_columns)
    previous_set = set(previous)
    current_set = set(current)

    return HeaderComparison(
        previous_columns=previous,
        current_columns=current,
        added_columns=tuple(column for column in current if column not in previous_set),
        missing_columns=tuple(column for column in previous if column not in current_set),
        reordered=not (previous == current) and previous_set == current_set,
    )


def is_unknown_field_exception(exc: BaseException) -> bool:
    """Return whether an exception chain represents Auto Loader schema growth."""
    pending: list[BaseException] = [exc]
    visited: set[int] = set()
    markers = ("UNKNOWN_FIELD_EXCEPTION", "UnknownFieldException")

    while pending:
        current = pending.pop()
        if id(current) in visited:
            continue
        visited.add(id(current))

        if any(marker in f"{type(current).__name__}: {current}" for marker in markers):
            return True

        for linked in (current.__cause__, current.__context__):
            if linked is not None:
                pending.append(linked)

        java_exception = getattr(current, "java_exception", None)
        if java_exception is not None:
            try:
                error_class = java_exception.getErrorClass()
            except Exception:
                error_class = None
            if error_class == "UNKNOWN_FIELD_EXCEPTION":
                return True
            if any(marker in str(java_exception) for marker in markers):
                return True

    return False


def run_with_schema_retry(
    operation: Callable[[], T],
    max_attempts: int,
    on_retry: Callable[[int, BaseException], None] | None = None,
) -> T:
    """Retry only Auto Loader's expected one-time schema evolution failure."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except BaseException as exc:
            if not is_unknown_field_exception(exc) or attempt == max_attempts:
                raise
            if on_retry is not None:
                on_retry(attempt, exc)

    raise AssertionError("schema retry loop exited unexpectedly")


def evolve_known_columns(
    known_columns: Iterable[str], observed_columns: Iterable[str]
) -> list[str]:
    """Append newly observed columns while retaining missing historical fields."""
    evolved = list(known_columns)
    known = set(evolved)
    for column in observed_columns:
        if column not in known:
            evolved.append(column)
            known.add(column)
    return evolved
