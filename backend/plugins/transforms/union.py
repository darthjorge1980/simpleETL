from __future__ import annotations
from typing import Any
import polars as pl
from plugins.base import TransformPlugin, register
from core.models import ParamDefinition, PluginCategory, PluginInfo, PortDefinition, PortType


@register
class UnionDataframes(TransformPlugin):
    @classmethod
    def info(cls) -> PluginInfo:
        return PluginInfo(
            id="union",
            name="Union (Stack)",
            category=PluginCategory.TRANSFORM,
            description="Stack two dataframes vertically (concat rows)",
            icon="Layers",
            color="#8b5cf6",
            inputs=[
                PortDefinition(name="top", port_type=PortType.DATAFRAME),
                PortDefinition(name="bottom", port_type=PortType.DATAFRAME),
            ],
            outputs=cls._base_outputs(),
            params=[
                ParamDefinition(name="how", label="Alignment", param_type="select", default="vertical", options=["vertical", "diagonal", "diagonal_relaxed", "align"], required=False, description="vertical: must have same columns. diagonal: fill missing with null. diagonal_relaxed: also cast types. align: align on column names."),
                ParamDefinition(name="rechunk", label="Rechunk", param_type="boolean", default=True, required=False, description="Rechunk the result for contiguous memory allocation"),
            ],
        )

    def execute(self, inputs: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
        top = inputs["top"]
        bottom = inputs["bottom"]
        p = self.params

        how = p.get("how", "vertical")

        if how == "vertical":
            result = pl.concat([top, bottom], how="vertical")
        elif how == "diagonal":
            result = pl.concat([top, bottom], how="diagonal")
        elif how == "diagonal_relaxed":
            result = pl.concat([top, bottom], how="diagonal_relaxed")
        else:  # align
            result = pl.concat([top, bottom], how="align")

        if p.get("rechunk", True):
            result = result.rechunk()

        return {"output": result}

    @classmethod
    def generate_code(cls, params: dict[str, Any], input_vars: dict[str, str]) -> tuple[str, dict[str, str]]:
        top_var = input_vars.get("top", "df_top")
        bottom_var = input_vars.get("bottom", "df_bottom")
        p = params
        var = "__OUT__"

        how = p.get("how", "vertical")
        rechunk = p.get("rechunk", True)

        lines = [f'{var} = pl.concat([{top_var}, {bottom_var}], how={how!r})']
        if rechunk:
            lines.append(f"{var} = {var}.rechunk()")

        return "\n".join(lines), {"output": var}
