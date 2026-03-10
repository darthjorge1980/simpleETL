from __future__ import annotations
from typing import Any
import polars as pl
from sqlalchemy import create_engine, text
from plugins.base import SourcePlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class SQLReader(SourcePlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="sql_reader",
            name="Read SQL",
            category=PluginCategory.SOURCE,
            description="Read data from a SQL database using a query",
            icon="Database",
            color="#22c55e",
            inputs=[],
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="connection_string", label="Connection String", param_type="string", required=True, description="SQLAlchemy connection string (e.g. sqlite:///db.sqlite, postgresql://user:pass@host/db)"),
                ParamDefinition(name="query", label="SQL Query", param_type="code", required=True, description="SELECT query to execute"),
                ParamDefinition(name="schema_overrides", label="Schema Overrides (JSON)", param_type="code", default="{}", required=False, description='Force column types, e.g. {"id": "Int64", "name": "Utf8"}'),
                ParamDefinition(name="execute_options", label="Execute Options (JSON)", param_type="code", default="{}", required=False, description='Engine-specific options, e.g. {"timeout": 30}'),
                ParamDefinition(name="pool_size", label="Connection Pool Size", param_type="number", default=5, required=False, description="Number of connections to keep open"),
                ParamDefinition(name="pool_timeout", label="Pool Timeout (seconds)", param_type="number", default=30, required=False, description="Seconds to wait for a connection from pool"),
                ParamDefinition(name="echo", label="Echo SQL", param_type="boolean", default=False, required=False, description="Log all SQL statements (debug)"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        import json as _json
        p = self.params
        conn_str = p["connection_string"]
        query = p["query"]

        engine_kwargs: dict[str, Any] = dict(echo=p.get("echo", False))
        pool_size = int(p.get("pool_size", 5))
        pool_timeout = int(p.get("pool_timeout", 30))
        if not conn_str.startswith("sqlite"):
            engine_kwargs["pool_size"] = pool_size
            engine_kwargs["pool_timeout"] = pool_timeout

        engine = create_engine(conn_str, **engine_kwargs)
        exec_opts = _json.loads(p.get("execute_options", "{}") or "{}")

        with engine.connect() as connection:
            result = connection.execute(text(query), exec_opts)
            rows = result.fetchall()
            columns = list(result.keys())

        df = pl.DataFrame(
            {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        )

        schema_raw = _json.loads(p.get("schema_overrides", "{}") or "{}")
        if schema_raw:
            type_map = {name: getattr(pl, dtype) for name, dtype in schema_raw.items() if hasattr(pl, dtype)}
            if type_map:
                df = df.cast(type_map)

        return {"output": df.lazy()}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        conn = params["connection_string"]
        query = params["query"]
        var = "__OUT__"
        code = (
            "from sqlalchemy import create_engine, text\n"
            f"_engine = create_engine({conn!r})\n"
            f"with _engine.connect() as _conn:\n"
            f"    _result = _conn.execute(text({query!r}))\n"
            f"    _rows = _result.fetchall()\n"
            f"    _cols = list(_result.keys())\n"
            f"{var} = pl.DataFrame({{c: [r[i] for r in _rows] for i, c in enumerate(_cols)}}).lazy()"
        )
        return code, {"output": var}
