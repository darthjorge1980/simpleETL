from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class ColumnSplit(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="column_split",
            name="Column Split",
            category=PluginCategory.TRANSFORM,
            description="Split a single column into multiple columns by delimiter",
            icon="Scissors",
            color="#f43f5e",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="column", label="Column", param_type="string", required=True, description="Column to split"),
                ParamDefinition(name="delimiter", label="Delimiter", param_type="string", default=",", required=True, description="Delimiter to split on"),
                ParamDefinition(name="max_splits", label="Max splits", param_type="number", default=0, required=False, description="Max number of splits (0 = unlimited)"),
                ParamDefinition(name="prefix", label="Column prefix", param_type="string", default="", required=False, description="Prefix for generated columns (empty = original column name)"),
                ParamDefinition(name="drop_original", label="Drop original", param_type="boolean", default=True, required=False, description="Remove the original column after splitting"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        col = p["column"]
        delim = p.get("delimiter", ",")
        prefix = p.get("prefix", "") or col

        df = lf.collect()
        split_series = df[col].str.split(delim)
        max_parts = split_series.list.len().max()

        if int(p.get("max_splits", 0)) > 0:
            max_parts = min(max_parts, int(p["max_splits"]) + 1)

        exprs = []
        for i in range(max_parts):
            exprs.append(
                pl.col(col).str.split(delim).list.get(i, null_on_oob=True).alias(f"{prefix}_{i}")
            )

        result = df.lazy().with_columns(exprs)
        if p.get("drop_original", True):
            result = result.drop(col)

        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        col = p["column"]
        delim = p.get("delimiter", ",")
        prefix = p.get("prefix", "") or col
        code = f"# Split column {col!r} by {delim!r}\n"
        code += f"_split = {iv}.collect()[{col!r}].str.split({delim!r})\n"
        code += f"_max = _split.list.len().max()\n"
        code += f"_exprs = [pl.col({col!r}).str.split({delim!r}).list.get(i, null_on_oob=True).alias(f'{prefix}_{{i}}') for i in range(_max)]\n"
        code += f"{var} = {iv}.with_columns(_exprs)"
        if p.get("drop_original", True):
            code += f".drop({col!r})"
        return code, {"output": var}
