from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class SurrogateKey(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="surrogate_key",
            name="Surrogate Key",
            category=PluginCategory.TRANSFORM,
            description="Generate an auto-increment surrogate key column",
            icon="Key",
            color="#10b981",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="column_name", label="Key Column Name", param_type="string", default="sk_id", required=False, description="Name of the surrogate key column"),
                ParamDefinition(name="offset", label="Start value", param_type="number", default=1, required=False, description="Starting value for the key sequence"),
                ParamDefinition(name="position", label="Position", param_type="select", default="first", options=["first", "last"], required=False, description="Place the key column first or last"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        p = self.params
        name = p.get("column_name", "sk_id") or "sk_id"
        offset = int(p.get("offset", 1))

        result = lf.with_row_index(name=name, offset=offset)
        if p.get("position", "first") == "first":
            cols = result.collect_schema().names()
            reordered = [name] + [c for c in cols if c != name]
            result = result.select(reordered)
        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        iv = input_vars.get("input", "df_in")
        p = params
        var = "__OUT__"
        name = p.get("column_name", "sk_id") or "sk_id"
        offset = int(p.get("offset", 1))
        code = f"{var} = {iv}.with_row_index(name={name!r}, offset={offset})"
        return code, {"output": var}
