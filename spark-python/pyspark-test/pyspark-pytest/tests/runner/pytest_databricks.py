"""Pytest runner for Databricks notebook environments.

Configures the working directory and runs pytest from within a
Databricks workspace where bytecode caching should be disabled.

Usage (in a Databricks notebook cell):
    %run ./pytest_databricks
"""

import os
import sys

import pytest


def run_tests(args: list[str] | None = None) -> int:
    """Run pytest with the given arguments from this file's directory.

    Args:
        args: Optional list of pytest arguments. Defaults to sys.argv[1:].

    Returns:
        pytest exit code (0 = success, non-zero = failure).
    """
    dir_root = os.path.dirname(os.path.realpath(__file__))
    os.chdir(dir_root)
    sys.dont_write_bytecode = True

    if args is None:
        args = sys.argv[1:]

    return pytest.main(args)


if __name__ == "__main__":
    sys.exit(run_tests())
