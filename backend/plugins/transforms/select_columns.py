from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class SelectColumns(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="select_columns",
            name="Select Columns",
            category=PluginCategory.TRANSFORM,
            description="Keep, drop, or reorder columns",
            icon="Columns3",
            color="#3b82f6",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="columns", label="Columns (comma-separated)", param_type="string", default="", required=False, description="Columns to keep (leave blank to use Exclude mode)"),
                ParamDefinition(name="exclude", label="Exclude Columns (comma-separated)", param_type="string", default="", required=False, description="Columns to drop (used when Columns is blank)"),
                ParamDefinition(name="regex", label="Regex Pattern", param_type="string", default="", required=False, description="Select columns matching regex (e.g. ^price_.*)"),
                ParamDefinition(name="dtype_select", label="Select by Dtype", param_type="select", default="none", options=["none", "numeric", "Utf8", "Boolean", "Date", "Datetime", "Int64", "Float64"], required=False, description="Select columns by data type"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params

        cols_str = p.get("columns", "")
        exclude_str = p.get("exclude", "")
        regex = p.get("regex", "")
        dtype_sel = p.get("dtype_select", "none")

        if cols_str:
            cols = [c.strip() for c in cols_str.split(",") if c.strip()]
            lf = lf.select(cols)
        elif exclude_str:
            excl = [c.strip() for c in exclude_str.split(",") if c.strip()]
            lf = lf.select([pl.exclude(excl)])

        if regex:
            lf = lf.select(pl.col(f"^{regex}$"))

        if dtype_sel and dtype_sel != "none":
            if dtype_sel == "numeric":
                lf = lf.select(pl.col(pl.NUMERIC_DTYPES))
            else:
                lf = lf.select(pl.col(getattr(pl, dtype_sel)))

        return {"output": lf}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        inv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"

        cols_str = p.get("columns", "")
        exclude_str = p.get("exclude", "")

        if cols_str:
            cols = [c.strip() for c in cols_str.split(",") if c.strip()]
            code = f"{var} = {inv}.select({cols!r})"
        elif exclude_str:
            excl = [c.strip() for c in exclude_str.split(",") if c.strip()]
            code = f"{var} = {inv}.select([pl.exclude({excl!r})])"
        else:
            code = f"{var} = {inv}"
        return code, {"output": var}
