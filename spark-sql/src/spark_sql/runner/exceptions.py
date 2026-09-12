"""Exceptions raised by the SQL runner / examiner / validator library."""

from __future__ import annotations


class RunnerError(Exception):
    """Base class for all errors raised by the runner package."""


class BackendError(RunnerError):
    """A backend failed to execute or explain a statement."""


class ValidationError(RunnerError, AssertionError):
    """A validation expectation was not met.

    Subclasses :class:`AssertionError` so failures read naturally in pytest and
    ``assert``-style call sites, while remaining catchable as a
    :class:`RunnerError`.
    """
