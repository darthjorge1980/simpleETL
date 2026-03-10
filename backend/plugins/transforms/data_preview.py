from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo


@register
class DataPreview(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="data_preview",
            name="Preview Data",
            category=PluginCategory.TRANSFORM,
            description="Passthrough node that displays a sample of the data. Place it anywhere to inspect records.",
            icon="Eye",
            color="#a855f7",
            inputs=cls._base_inputs(),
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="max_rows", label="Max Rows to Show", param_type="number", default=5, required=False, description="Number of rows to display (1-100)"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        lf = inputs["input"]
        return {"output": lf}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        inv = input_vars.get("input", "df_in")
        var = "__OUT__"
        code = f"{var} = {inv}  # preview passthrough"
        return code, {"output": var}
