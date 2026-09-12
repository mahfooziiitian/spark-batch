from __future__ import annotations

import pytest

from spark_sql.runner import SqlResult

pytestmark = [pytest.mark.unit]


def _result(**overrides: object) -> SqlResult:
    base: dict[str, object] = {
        "statement": "SELECT id, name FROM t",
        "columns": ["id", "name"],
        "rows": [(1, "a"), (2, "b")],
        "backend": "local-spark",
    }
    base.update(overrides)
    return SqlResult(**base)  # type: ignore[arg-type]


class TestSqlResult:
    def test_shape_helpers(self: TestSqlResult) -> None:
        result = _result()
        assert result.row_count == 2
        assert result.column_count == 2
        assert not result.is_empty
        assert len(result) == 2
        assert bool(result) is True
        assert list(result) == [(1, "a"), (2, "b")]

    def test_empty_result(self: TestSqlResult) -> None:
        result = _result(rows=[])
        assert result.is_empty
        assert result.first() is None
        assert bool(result) is False

    def test_first_and_column(self: TestSqlResult) -> None:
        result = _result()
        assert result.first() == (1, "a")
        assert result.column("name") == ["a", "b"]

    def test_column_missing_raises(self: TestSqlResult) -> None:
        with pytest.raises(KeyError):
            _result().column("nope")

    def test_scalar(self: TestSqlResult) -> None:
        result = _result(statement="SELECT 42", columns=["v"], rows=[(42,)])
        assert result.scalar() == 42

    def test_scalar_requires_single_cell(self: TestSqlResult) -> None:
        with pytest.raises(ValueError, match="1x1"):
            _result().scalar()

    def test_to_dicts(self: TestSqlResult) -> None:
        assert _result().to_dicts() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    def test_plan_default_none(self: TestSqlResult) -> None:
        assert _result().plan is None
