from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class Explode(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="explode",
            name="Explode (Normalize)",
            category=PluginCategory.TRANSFORM,
            description="Expand list/array column values into separate rows",
            icon="Expand",
            color="#7c3aed",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="column", label="Column to explode", param_type="string", required=True, description="List-type column to expand into rows"),
                ParamDefinition(name="split_first", label="Split string first", param_type="boolean", default=False, required=False, description="If column is a string, split it by delimiter before exploding"),
                ParamDefinition(name="delimiter", label="Delimiter", param_type="string", default=",", required=False, description="Delimiter for splitting strings (if enabled)"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        col = p["column"]

        if p.get("split_first", False):
            delim = p.get("delimiter", ",")
            lf = lf.with_columns(pl.col(col).str.split(delim))

        result = lf.explode(col)
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        col = p["column"]
        lines = []
        if p.get("split_first", False):
            lines.append(f"{iv} = {iv}.with_columns(pl.col({col!r}).str.split({p.get('delimiter', ',')!r}))")
        lines.append(f"{var} = {iv}.explode({col!r})")
        return "\n".join(lines), {"output": var}
