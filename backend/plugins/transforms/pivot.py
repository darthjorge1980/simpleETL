from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class Pivot(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="pivot",
            name="Pivot",
            category=PluginCategory.TRANSFORM,
            description="Pivot (rotate) rows into columns",
            icon="RotateCw",
            color="#f59e0b",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="on", label="Pivot Column", param_type="string", required=True, description="Column whose distinct values become new column headers"),
                ParamDefinition(name="index", label="Index Column(s)", param_type="string", required=True, description="Column(s) to keep as row identifiers (comma-separated)"),
                ParamDefinition(name="values", label="Values Column", param_type="string", required=True, description="Column whose values fill the pivoted cells"),
                ParamDefinition(name="aggregate_function", label="Aggregate", param_type="select", default="first", options=["first", "sum", "mean", "min", "max", "count", "last", "len"], required=False, description="Aggregation when multiple values exist per cell"),
                ParamDefinition(name="sort_columns", label="Sort result columns", param_type="boolean", default=True, required=False, description="Sort the resulting pivoted columns alphabetically"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        df = lf.collect()

        index = [c.strip() for c in p["index"].split(",") if c.strip()]
        agg_fn = p.get("aggregate_function", "first")

        result = df.pivot(
            on=p["on"],
            index=index,
            values=p["values"],
            aggregate_function=agg_fn,
            sort_columns=p.get("sort_columns", True),
        )
        return {"output": result.lazy()}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        index = [c.strip() for c in p["index"].split(",") if c.strip()]
        code = f"{var} = {iv}.collect().pivot(on={p['on']!r}, index={index!r}, values={p['values']!r}, aggregate_function={p.get('aggregate_function', 'first')!r}, sort_columns={p.get('sort_columns', True)!r}).lazy()"
        return code, {"output": var}
