from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from spark_sql.runner import BackendError, DatabricksBackend, SqlRunner

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

pytestmark = [pytest.mark.unit]


def _fake_response(columns: list[str], data: list[list[object]]) -> SimpleNamespace:
    schema = SimpleNamespace(columns=[SimpleNamespace(name=name) for name in columns])
    manifest = SimpleNamespace(schema=schema)
    result = SimpleNamespace(data_array=data)
    return SimpleNamespace(manifest=manifest, result=result)


def _fake_client(response: SimpleNamespace, mocker: MockerFixture) -> SimpleNamespace:
    execute = mocker.Mock(return_value=response)
    return SimpleNamespace(statement_execution=SimpleNamespace(execute_statement=execute))


class TestDatabricksBackend:
    def test_execute_parses_manifest_and_data(self: TestDatabricksBackend, mocker: MockerFixture) -> None:
        response = _fake_response(["id", "name"], [["1", "a"], ["2", "b"]])
        client = _fake_client(response, mocker)
        backend = DatabricksBackend(client, "wh-123")  # type: ignore[arg-type]

        result = backend.run("SELECT id, name FROM t")

        assert backend.name == "databricks"
        assert backend.warehouse_id == "wh-123"
        assert result.columns == ["id", "name"]
        assert result.rows == [("1", "a"), ("2", "b")]
        client.statement_execution.execute_statement.assert_called_once_with(
            warehouse_id="wh-123",
            statement="SELECT id, name FROM t",
        )

    def test_empty_result(self: TestDatabricksBackend, mocker: MockerFixture) -> None:
        response = _fake_response(["id"], [])
        backend = DatabricksBackend(_fake_client(response, mocker), "wh-1")  # type: ignore[arg-type]
        assert backend.run("SELECT id FROM t WHERE 1=0").is_empty

    def test_execution_error_wrapped(self: TestDatabricksBackend, mocker: MockerFixture) -> None:
        execute = mocker.Mock(side_effect=RuntimeError("boom"))
        client = SimpleNamespace(statement_execution=SimpleNamespace(execute_statement=execute))
        backend = DatabricksBackend(client, "wh-1")  # type: ignore[arg-type]
        with pytest.raises(BackendError, match="databricks failed"):
            backend.run("SELECT 1")

    def test_from_env_resolves_warehouse_name(self: TestDatabricksBackend, mocker: MockerFixture) -> None:
        warehouses = [SimpleNamespace(name="other", id="w-0"), SimpleNamespace(name="mine", id="w-9")]
        client = SimpleNamespace(warehouses=SimpleNamespace(list=mocker.Mock(return_value=warehouses)))
        mocker.patch("spark_sql.util.cli_env.get_workspace_client", return_value=client)

        backend = DatabricksBackend.from_env(warehouse_name="mine")
        assert backend.warehouse_id == "w-9"

    def test_from_env_without_warehouse_raises(self: TestDatabricksBackend, mocker: MockerFixture) -> None:
        mocker.patch("spark_sql.util.cli_env.get_workspace_client", return_value=SimpleNamespace())
        mocker.patch.dict("os.environ", {}, clear=True)
        with pytest.raises(BackendError, match="No warehouse configured"):
            DatabricksBackend.from_env()

    def test_runner_databricks_constructor(self: TestDatabricksBackend, mocker: MockerFixture) -> None:
        response = _fake_response(["n"], [["1"]])
        client = _fake_client(response, mocker)
        client.warehouses = SimpleNamespace(list=mocker.Mock(return_value=[SimpleNamespace(name="w", id="wid")]))
        mocker.patch("spark_sql.util.cli_env.get_workspace_client", return_value=client)

        runner = SqlRunner.databricks(warehouse_name="w")
        assert runner.run("SELECT 1 AS n").scalar() == "1"
