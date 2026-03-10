from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class AddColumn(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="add_column",
            name="Add Column",
            category=PluginCategory.TRANSFORM,
            description="Add one or more computed columns using Polars expressions",
            icon="Plus",
            color="#3b82f6",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="column_name", label="Column Name", param_type="string", required=True, description="Name for the new column"),
                ParamDefinition(name="expression", label="Expression", param_type="code", required=True, description="Polars expression, e.g. pl.col('price') * pl.col('qty')"),
                ParamDefinition(name="extra_columns", label="Extra Columns (JSON)", param_type="code", default="[]", required=False, description='Add more columns: [{"name":"total","expression":"pl.col(\'price\') * pl.col(\'qty\')"}]'),
                ParamDefinition(name="dtype", label="Cast to Dtype", param_type="select", default="none", options=["none", "Utf8", "Int32", "Int64", "Float32", "Float64", "Boolean", "Date", "Datetime"], required=False, description="Cast the result to a specific type"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        import json as _json
        lf = inputs["input"]
        p = self.params

        expr = eval(p["expression"], {"pl": pl})  # noqa: S307
        dtype = p.get("dtype", "none")
        if dtype and dtype != "none":
            expr = expr.cast(getattr(pl, dtype))
        exprs = [expr.alias(p["column_name"])]

        extras = _json.loads(p.get("extra_columns", "[]") or "[]")
        for ec in extras:
            e = eval(ec["expression"], {"pl": pl})  # noqa: S307
            exprs.append(e.alias(ec["name"]))

        return {"output": lf.with_columns(exprs)}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        inv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        dtype = p.get("dtype", "none")
        expr_str = p["expression"]
        if dtype and dtype != "none":
            expr_str = f"({expr_str}).cast(pl.{dtype})"
        code = f"{var} = {inv}.with_columns(({expr_str}).alias({p['column_name']!r}))"
        return code, {"output": var}
