from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class ColumnMerge(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="column_merge",
            name="Column Merge",
            category=PluginCategory.TRANSFORM,
            description="Concatenate multiple columns into a single string column",
            icon="Combine",
            color="#0d9488",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="columns", label="Columns", param_type="string", required=True, description="Columns to merge (comma-separated)"),
                ParamDefinition(name="separator", label="Separator", param_type="string", default="", required=False, description="Separator between values"),
                ParamDefinition(name="output_column", label="Output column", param_type="string", default="merged", required=False, description="Name for the merged column"),
                ParamDefinition(name="ignore_nulls", label="Ignore nulls", param_type="boolean", default=True, required=False, description="Skip null values instead of producing null for the whole row"),
                ParamDefinition(name="drop_source", label="Drop source columns", param_type="boolean", default=False, required=False, description="Remove the original columns after merging"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        cols = [c.strip() for c in p["columns"].split(",") if c.strip()]
        sep = p.get("separator", "")
        out = p.get("output_column", "merged") or "merged"
        ignore_nulls = p.get("ignore_nulls", True)

        expr = pl.concat_str(
            [pl.col(c).cast(pl.Utf8) for c in cols],
            separator=sep,
            ignore_nulls=ignore_nulls,
        ).alias(out)

        result = lf.with_columns(expr)
        if p.get("drop_source", False):
            result = result.drop([c for c in cols if c != out])
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        cols = [c.strip() for c in p["columns"].split(",") if c.strip()]
        sep = p.get("separator", "")
        out = p.get("output_column", "merged") or "merged"
        col_exprs = ", ".join(f"pl.col({c!r}).cast(pl.Utf8)" for c in cols)
        code = f'{var} = {iv}.with_columns(pl.concat_str([{col_exprs}], separator={sep!r}).alias({out!r}))'
        return code, {"output": var}
