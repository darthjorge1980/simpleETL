from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class FilterRows(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="filter_rows",
            name="Filter Rows",
            category=PluginCategory.TRANSFORM,
            description="Filter rows using one or more Polars expressions",
            icon="Filter",
            color="#3b82f6",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="expression", label="Filter Expression", param_type="code", required=True, description="Polars expression, e.g. pl.col('age') > 18"),
                ParamDefinition(name="additional_filters", label="Additional Filters (JSON list)", param_type="code", default="[]", required=False, description='Extra Polars expressions combined with AND, e.g. ["pl.col(\'status\') == \'active\'"]'),
                ParamDefinition(name="combine", label="Combine Logic", param_type="select", default="and", options=["and", "or"], required=False, description="How to combine primary + additional filters"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        import json as _json
        lf = inputs["input"]
        p = self.params
        primary = eval(p["expression"], {"pl": pl})  # noqa: S307

        extras_raw = _json.loads(p.get("additional_filters", "[]") or "[]")
        if extras_raw:
            exprs = [primary] + [eval(e, {"pl": pl}) for e in extras_raw]  # noqa: S307
            if p.get("combine", "and") == "or":
                combined = exprs[0]
                for e in exprs[1:]:
                    combined = combined | e
            else:
                combined = exprs[0]
                for e in exprs[1:]:
                    combined = combined & e
            return {"output": lf.filter(combined)}
        return {"output": lf.filter(primary)}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        inv = input_vars.get("input", "df_in")
        expr = params["expression"]
        var = "__OUT__"
        code = f"{var} = {inv}.filter({expr})"
        return code, {"output": var}
